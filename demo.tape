Output demo.gif

Set Shell "bash"
Set FontSize 12
Set Width 800
Set Height 600
Set Padding 20

Type@3ms "export PISTA_PAGER='bat -l sql -pp --color=always'" Enter
Sleep 2s

# Create schema file
Enter
Type "# Create a desired schema definition file" Enter
Sleep 2s
Type "cat <<'EOF' > schema.sql" Enter
Type@3ms "CREATE TABLE public.posts (" Enter
Type@3ms "    id integer NOT NULL," Enter
Type@3ms "    user_id integer NOT NULL," Enter
Type@3ms "    title text NOT NULL," Enter
Type@3ms "    CONSTRAINT posts_pkey PRIMARY KEY (id)" Enter
Type@3ms ");" Enter
Type@3ms "" Enter
Type@3ms "CREATE INDEX idx_posts_user_id ON public.posts USING btree (user_id);" Enter
Type@3ms "EOF" Enter
Sleep 4s

# Plan
Enter
Type "# Preview the changes to be applied (like terraform plan)" Enter
Sleep 2s
Type "pista plan schema.sql" Enter
Sleep 4s

# Apply
Enter
Type "# Apply the schema to the database" Enter
Sleep 2s
Type "pista apply schema.sql" Enter
Sleep 4s

# Verify no remaining diff
Enter
Type "# Verify that the schema is now in sync" Enter
Sleep 2s
Type "pista plan schema.sql" Enter
Sleep 4s

# Dump
Enter
Type "# Dump the current database schema" Enter
Sleep 2s
Type "pista dump" Enter
Sleep 4s

# Backup and modify schema: add a column
Enter
Type "# Modify the schema file: add an body column to posts" Enter
Sleep 2s
Type "sed -i.bak 's/title text NOT NULL,/title text NOT NULL,\n    body text NOT NULL,/' schema.sql" Enter
Sleep 0.5s

# Show diff
Enter
Type "# Show the schema file diff" Enter
Sleep 2s
Type "diff -u --color schema.sql.bak schema.sql" Enter
Sleep 4s

# Plan the diff
Enter
Type "# Plan again to see the incremental changes" Enter
Sleep 2s
Type "pista plan schema.sql" Enter
Sleep 4s

# Apply the diff
Enter
Type "# Apply the incremental changes" Enter
Sleep 2s
Type "pista apply schema.sql" Enter
Sleep 4s

# Verify no remaining diff
Enter
Type "# Verify that the schema is now in sync" Enter
Sleep 2s
Type "pista plan schema.sql" Enter
Sleep 4s

# Dump
Enter
Type "# Dump the current database schema" Enter
Sleep 2s
Type "pista dump" Enter
Sleep 4s
