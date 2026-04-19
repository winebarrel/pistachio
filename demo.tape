Output demo.gif

Set Shell "bash"
Set FontSize 12
Set Width 800
Set Height 600
Set Padding 20

Hide
Type "alias colorize='bat -l sql -p --paging=never'"
Enter
Enter
Sleep 3s
Show

# Create schema file
Type "# Create a desired schema definition file" Enter
Sleep 1s
Type "cat <<'EOF' > schema.sql" Enter
Type@3ms "CREATE TABLE public.users (" Enter
Type@3ms "    id integer NOT NULL," Enter
Type@3ms "    name text NOT NULL," Enter
Type@3ms "    CONSTRAINT users_pkey PRIMARY KEY (id)" Enter
Type@3ms ");" Enter
Type@3ms "" Enter
Type@3ms "CREATE TABLE public.posts (" Enter
Type@3ms "    id integer NOT NULL," Enter
Type@3ms "    user_id integer NOT NULL," Enter
Type@3ms "    title text NOT NULL," Enter
Type@3ms "    CONSTRAINT posts_pkey PRIMARY KEY (id)" Enter
Type@3ms ");" Enter
Type@3ms "" Enter
Type@3ms "CREATE INDEX idx_posts_user_id ON public.posts USING btree (user_id);" Enter
Type@3ms "" Enter
Type@3ms "ALTER TABLE ONLY public.posts" Enter
Type@3ms "    ADD CONSTRAINT posts_user_id_fkey" Enter
Type@3ms "    FOREIGN KEY (user_id) REFERENCES public.users(id);" Enter
Type@3ms "EOF" Enter
Sleep 0.5s
Type "colorize schema.sql" Enter
Sleep 0.1s
Enter
Sleep 5s

# Plan
Type "# Preview the changes to be applied (like terraform plan)" Enter
Sleep 1s
Type "pist plan schema.sql | colorize" Enter
Sleep 0.1s
Enter
Sleep 5s

# Apply
Type "# Apply the schema to the database" Enter
Sleep 1s
Type "pist apply schema.sql | colorize" Enter
Sleep 0.1s
Enter
Sleep 5s

# Verify no remaining diff
Type "# Verify that the schema is now in sync" Enter
Sleep 1s
Type "pist plan schema.sql" Enter
Sleep 0.1s
Enter
Sleep 5s

# Dump
Type "# Dump the current database schema" Enter
Sleep 1s
Type "pist dump | colorize" Enter
Sleep 0.1s
Enter
Sleep 5s

# Backup and modify schema: add a column
Type "# Modify the schema file: add an email column to users" Enter
Sleep 1s
Type "cp schema.sql schema.sql.bak" Enter
Sleep 0.5s
Type@10ms "sed -i '' 's/name text NOT NULL,/name text NOT NULL,\n    email text NOT NULL,/' schema.sql" Enter
Sleep 0.5s

# Show diff
Type "# Show the schema file diff" Enter
Sleep 1s
Type "diff -u --color schema.sql.bak schema.sql" Enter
Sleep 0.1s
Enter
Sleep 5s

# Plan the diff
Type "# Plan again to see the incremental changes" Enter
Sleep 1s
Type "pist plan schema.sql | colorize" Enter
Sleep 0.1s
Enter
Sleep 5s

# Apply the diff
Type "# Apply the incremental changes" Enter
Sleep 1s
Type "pist apply schema.sql | colorize" Enter
Sleep 0.1s
Enter
Sleep 5s

# Verify no remaining diff
Type "# Verify that the schema is now in sync" Enter
Sleep 1s
Type "pist plan schema.sql" Enter
Sleep 5s
