CREATE TABLE public."Album" (
    "AlbumId" integer NOT NULL,
    "Title" character varying(160) NOT NULL,
    "ArtistId" integer NOT NULL,
    CONSTRAINT "PK_Album" PRIMARY KEY ("AlbumId")
);
CREATE INDEX "IFK_AlbumArtistId" ON public."Album" USING btree ("ArtistId");

CREATE TABLE public."Artist" (
    "ArtistId" integer NOT NULL,
    "Name" character varying(120),
    "Country" character varying(40),
    CONSTRAINT "PK_Artist" PRIMARY KEY ("ArtistId")
);

CREATE TABLE public."Customer" (
    "CustomerId" integer NOT NULL,
    "FirstName" character varying(40) NOT NULL,
    "LastName" character varying(20) NOT NULL,
    "Company" character varying(80),
    "Address" character varying(70),
    "City" character varying(40),
    "State" character varying(40),
    "Country" character varying(40),
    "PostalCode" character varying(10),
    "Phone" character varying(24),
    "Fax" character varying(24),
    "Email" character varying(60) NOT NULL,
    "SupportRepId" integer,
    CONSTRAINT "PK_Customer" PRIMARY KEY ("CustomerId")
);
CREATE INDEX "IFK_CustomerSupportRepId" ON public."Customer" USING btree ("SupportRepId");

CREATE TABLE public."Employee" (
    "EmployeeId" integer NOT NULL,
    "LastName" character varying(20) NOT NULL,
    "FirstName" character varying(20) NOT NULL,
    "Title" character varying(30),
    "ReportsTo" integer,
    "BirthDate" timestamp without time zone,
    "HireDate" timestamp without time zone,
    "Address" character varying(70),
    "City" character varying(40),
    "State" character varying(40),
    "Country" character varying(40),
    "PostalCode" character varying(10),
    "Phone" character varying(24),
    "Fax" character varying(24),
    "Email" character varying(60),
    CONSTRAINT "PK_Employee" PRIMARY KEY ("EmployeeId")
);
CREATE INDEX "IFK_EmployeeReportsTo" ON public."Employee" USING btree ("ReportsTo");

CREATE TABLE public."Genre" (
    "GenreId" integer NOT NULL,
    "Name" character varying(120),
    CONSTRAINT "PK_Genre" PRIMARY KEY ("GenreId")
);

CREATE TABLE public."Invoice" (
    "InvoiceId" integer NOT NULL,
    "CustomerId" integer NOT NULL,
    "InvoiceDate" timestamp without time zone NOT NULL,
    "BillingAddress" character varying(70),
    "BillingCity" character varying(40),
    "BillingState" character varying(40),
    "BillingCountry" character varying(40),
    "BillingPostalCode" character varying(10),
    "Total" numeric(10,2) NOT NULL,
    CONSTRAINT "PK_Invoice" PRIMARY KEY ("InvoiceId")
);
CREATE INDEX "IFK_InvoiceCustomerId" ON public."Invoice" USING btree ("CustomerId");
CREATE INDEX "IX_InvoiceDate" ON public."Invoice" USING btree ("InvoiceDate");

CREATE TABLE public."InvoiceLine" (
    "InvoiceLineId" integer NOT NULL,
    "InvoiceId" integer NOT NULL,
    "TrackId" integer NOT NULL,
    "UnitPrice" numeric(10,2) NOT NULL,
    "Quantity" integer NOT NULL,
    CONSTRAINT "PK_InvoiceLine" PRIMARY KEY ("InvoiceLineId")
);
CREATE INDEX "IFK_InvoiceLineInvoiceId" ON public."InvoiceLine" USING btree ("InvoiceId");
CREATE INDEX "IFK_InvoiceLineTrackId" ON public."InvoiceLine" USING btree ("TrackId");

CREATE TABLE public."MediaType" (
    "MediaTypeId" integer NOT NULL,
    "Name" character varying(120),
    CONSTRAINT "PK_MediaType" PRIMARY KEY ("MediaTypeId")
);

CREATE TABLE public."Playlist" (
    "PlaylistId" integer NOT NULL,
    "Name" character varying(120),
    CONSTRAINT "PK_Playlist" PRIMARY KEY ("PlaylistId")
);

CREATE TABLE public."PlaylistTrack" (
    "PlaylistId" integer NOT NULL,
    "TrackId" integer NOT NULL,
    CONSTRAINT "PK_PlaylistTrack" PRIMARY KEY ("PlaylistId", "TrackId")
);
CREATE INDEX "IFK_PlaylistTrackTrackId" ON public."PlaylistTrack" USING btree ("TrackId");

CREATE TABLE public."Track" (
    "TrackId" integer NOT NULL,
    "Name" character varying(200) NOT NULL,
    "AlbumId" integer,
    "MediaTypeId" integer NOT NULL,
    "GenreId" integer,
    "Composer" character varying(220),
    "Milliseconds" integer NOT NULL,
    "Bytes" integer,
    "UnitPrice" numeric(10,2) NOT NULL,
    CONSTRAINT "PK_Track" PRIMARY KEY ("TrackId")
);
CREATE INDEX "IFK_TrackAlbumId" ON public."Track" USING btree ("AlbumId");
CREATE INDEX "IFK_TrackGenreId" ON public."Track" USING btree ("GenreId");
CREATE INDEX "IFK_TrackMediaTypeId" ON public."Track" USING btree ("MediaTypeId");

-- Foreign keys (after all tables are created)
ALTER TABLE ONLY public."Album" ADD CONSTRAINT "FK_AlbumArtistId" FOREIGN KEY ("ArtistId") REFERENCES "Artist"("ArtistId");
ALTER TABLE ONLY public."Customer" ADD CONSTRAINT "FK_CustomerSupportRepId" FOREIGN KEY ("SupportRepId") REFERENCES "Employee"("EmployeeId");
ALTER TABLE ONLY public."Employee" ADD CONSTRAINT "FK_EmployeeReportsTo" FOREIGN KEY ("ReportsTo") REFERENCES "Employee"("EmployeeId");
ALTER TABLE ONLY public."Invoice" ADD CONSTRAINT "FK_InvoiceCustomerId" FOREIGN KEY ("CustomerId") REFERENCES "Customer"("CustomerId");
ALTER TABLE ONLY public."InvoiceLine" ADD CONSTRAINT "FK_InvoiceLineInvoiceId" FOREIGN KEY ("InvoiceId") REFERENCES "Invoice"("InvoiceId");
ALTER TABLE ONLY public."InvoiceLine" ADD CONSTRAINT "FK_InvoiceLineTrackId" FOREIGN KEY ("TrackId") REFERENCES "Track"("TrackId");
ALTER TABLE ONLY public."PlaylistTrack" ADD CONSTRAINT "FK_PlaylistTrackPlaylistId" FOREIGN KEY ("PlaylistId") REFERENCES "Playlist"("PlaylistId");
ALTER TABLE ONLY public."PlaylistTrack" ADD CONSTRAINT "FK_PlaylistTrackTrackId" FOREIGN KEY ("TrackId") REFERENCES "Track"("TrackId");
ALTER TABLE ONLY public."Track" ADD CONSTRAINT "FK_TrackAlbumId" FOREIGN KEY ("AlbumId") REFERENCES "Album"("AlbumId");
ALTER TABLE ONLY public."Track" ADD CONSTRAINT "FK_TrackGenreId" FOREIGN KEY ("GenreId") REFERENCES "Genre"("GenreId");
ALTER TABLE ONLY public."Track" ADD CONSTRAINT "FK_TrackMediaTypeId" FOREIGN KEY ("MediaTypeId") REFERENCES "MediaType"("MediaTypeId");
