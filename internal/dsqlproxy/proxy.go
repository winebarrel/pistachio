// Package dsqlproxy is a PostgreSQL wire protocol proxy that makes a
// PostgreSQL server answer the way Aurora DSQL does, for the part of DSQL that
// pistachio's DSQL scenarios reach. It rewrites the text of each statement a
// client sends and leaves every other message alone:
//
//   - CREATE INDEX ASYNC and ALTER TABLE ASYNC ... VALIDATE CONSTRAINT run at
//     once and return a job ID, which sys.wait_for_job then reports on. The
//     index stays invalid, and the constraint NOT VALID, until the wait.
//   - A statement DSQL refuses fails with DSQL's SQLSTATE and message, and so
//     does a second DDL statement in a transaction.
//   - The catalog reads return the values DSQL reports: btree_index as the
//     access method, every other column of a primary key's table as
//     INCLUDE, and lz4 compression.
//
// The rules follow DSQL.md, which records what a live cluster answered. The
// proxy is a test tool. It does not replace the live cluster, and it knows
// nothing DSQL.md does not say.
package dsqlproxy

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

//go:embed setup.sql
var setupSQL string

// Proxy accepts connections and relays each to a connection of its own to the
// backend.
type Proxy struct {
	ln      net.Listener
	backend string
	wg      sync.WaitGroup
}

// Start sets the backend up, listens on addr and serves in the background.
// backend is the connection string of the PostgreSQL server behind the proxy.
func Start(ctx context.Context, addr, backend string) (*Proxy, error) {
	if err := Setup(ctx, backend); err != nil {
		return nil, err
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	p := &Proxy{ln: ln, backend: backend}
	p.wg.Add(1)
	go p.accept()
	return p, nil
}

// Setup creates in the backend the objects the rewritten statements use.
func Setup(ctx context.Context, backend string) error {
	conn, err := pgconn.Connect(ctx, backend)
	if err != nil {
		return err
	}
	defer conn.Close(ctx) //nolint:errcheck
	if _, err := conn.Exec(ctx, setupSQL).ReadAll(); err != nil {
		return fmt.Errorf("failed to set the backend up: %w", err)
	}
	return nil
}

// Addr returns the address the proxy listens on.
func (p *Proxy) Addr() net.Addr {
	return p.ln.Addr()
}

// Close stops listening. Connections already open run until their client
// closes them.
func (p *Proxy) Close() error {
	err := p.ln.Close()
	p.wg.Wait()
	return err
}

func (p *Proxy) accept() {
	defer p.wg.Done()
	for {
		conn, err := p.ln.Accept()
		if err != nil {
			return
		}
		go p.serve(conn)
	}
}

func (p *Proxy) serve(client net.Conn) {
	defer client.Close() //nolint:errcheck

	fe := pgproto3.NewBackend(client, client)
	startup, err := receiveStartup(client, fe)
	if err != nil || startup == nil {
		return
	}

	// DSQL takes the read-only flag only as a statement after connecting.
	if _, ok := startup.Parameters["default_transaction_read_only"]; ok {
		sendError(fe, &pgconn.PgError{Severity: "FATAL", Code: "0A000", Message: msgDefaultReadOnly})
		return
	}

	ctx := context.Background()
	cfg, err := pgconn.ParseConfig(p.backend)
	if err != nil {
		sendError(fe, &pgconn.PgError{Severity: "FATAL", Code: "08000", Message: err.Error()})
		return
	}
	for k, v := range startup.Parameters {
		if k != "user" && k != "database" {
			cfg.RuntimeParams[k] = v
		}
	}
	conn, err := pgconn.ConnectConfig(ctx, cfg)
	if err != nil {
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) {
			pgErr = &pgconn.PgError{Severity: "FATAL", Code: "08000", Message: err.Error()}
		}
		sendError(fe, pgErr)
		return
	}
	hc, err := conn.Hijack()
	if err != nil {
		return
	}
	defer hc.Conn.Close() //nolint:errcheck
	be := hc.Frontend

	fe.Send(&pgproto3.AuthenticationOk{})
	for k, v := range hc.ParameterStatuses {
		// DSQL reports the bare version number.
		if k == "server_version" {
			v, _, _ = strings.Cut(v, " ")
		}
		fe.Send(&pgproto3.ParameterStatus{Name: k, Value: v})
	}
	fe.Send(&pgproto3.BackendKeyData{ProcessID: hc.PID, SecretKey: hc.SecretKey})
	fe.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	if err := fe.Flush(); err != nil {
		return
	}

	var txStatus atomic.Int32
	txStatus.Store('I')
	var mu sync.Mutex
	sess := &session{inTx: func() bool { return txStatus.Load() != 'I' }}
	tg := newTags()

	done := make(chan struct{})
	go func() {
		defer close(done)
		defer client.Close() //nolint:errcheck
		for {
			msg, err := be.Receive()
			if err != nil {
				return
			}
			mu.Lock()
			switch m := msg.(type) {
			case *pgproto3.CommandComplete:
				if tag := tg.complete(); tag != "" {
					m.CommandTag = []byte(tag)
				}
			case *pgproto3.ReadyForQuery:
				tg.ready()
				txStatus.Store(int32(m.TxStatus))
				if m.TxStatus == 'I' {
					sess.txEnded()
				}
			}
			mu.Unlock()
			fe.Send(msg)
			if err := fe.Flush(); err != nil {
				return
			}
		}
	}()

	for {
		msg, err := fe.Receive()
		if err != nil {
			break
		}
		mu.Lock()
		switch m := msg.(type) {
		case *pgproto3.Query:
			var stmtTags []string
			m.String, stmtTags = sess.rewrite(m.String)
			tg.query(stmtTags)
		case *pgproto3.Parse:
			var stmtTags []string
			m.Query, stmtTags = sess.rewrite(m.Query)
			tg.parse(m.Name, stmtTags)
		case *pgproto3.Bind:
			tg.bind(m.DestinationPortal, m.PreparedStatement)
		case *pgproto3.Execute:
			tg.execute(m.Portal)
		case *pgproto3.Sync:
			tg.sync()
		}
		mu.Unlock()
		be.Send(msg)
		if err := be.Flush(); err != nil {
			break
		}
		if _, ok := msg.(*pgproto3.Terminate); ok {
			break
		}
	}
	hc.Conn.Close() //nolint:errcheck
	<-done
}

// receiveStartup answers the requests a client may send before its startup
// message: the proxy speaks neither TLS nor GSS encryption. It returns nil
// for a cancel request, which the proxy does not pass on.
func receiveStartup(client io.Writer, fe *pgproto3.Backend) (*pgproto3.StartupMessage, error) {
	for {
		msg, err := fe.ReceiveStartupMessage()
		if err != nil {
			return nil, err
		}
		switch m := msg.(type) {
		case *pgproto3.SSLRequest, *pgproto3.GSSEncRequest:
			if _, err := client.Write([]byte("N")); err != nil {
				return nil, err
			}
		case *pgproto3.StartupMessage:
			return m, nil
		default:
			return nil, nil
		}
	}
}

func sendError(fe *pgproto3.Backend, e *pgconn.PgError) {
	fe.Send(&pgproto3.ErrorResponse{Severity: e.Severity, SeverityUnlocalized: e.Severity, Code: e.Code, Message: e.Message})
	fe.Flush() //nolint:errcheck
}
