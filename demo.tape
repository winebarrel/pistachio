Output demo.gif

Set Shell "bash"
Set FontSize 12
Set Width 800
Set Height 600
Set Padding 20

Hide
Type "alias colorize='bat -l sql -p --paging=never' && clear"
Show

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
Sleep 0.5s
Enter
Type "colorize schema.sql" Enter
Sleep 4s

# Plan
Enter
Type "# Preview the changes to be applied (like terraform plan)" Enter
Sleep 2s
Type "pist plan schema.sql | colorize" Enter
Sleep 4s

# Apply
Enter
Type "# Apply the schema to the database" Enter
Sleep 2s
Type "pist apply schema.sql | colorize" Enter
Sleep 4s

# Verify no remaining diff
Enter
Type "# Verify that the schema is now in sync" Enter
Sleep 2s
Type "pist plan schema.sql | colorize" Enter
Sleep 4s

# Dump
Enter
Type "# Dump the current database schema" Enter
Sleep 2s
Type "pist dump | colorize" Enter
Sleep 4s

# Backup and modify schema: add a column
Enter
Type "# Modify the schema file: add an body column to posts" Enter
Sleep 2s
Type "cp schema.sql schema.sql.bak" Enter
Sleep 0.5s
Type "sed -i '' 's/title text NOT NULL,/title text NOT NULL,\n    body text NOT NULL,/' schema.sql" Enter
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
Type "pist plan schema.sql | colorize" Enter
Sleep 4s

# Apply the diff
Enter
Type "# Apply the incremental changes" Enter
Sleep 2s
Type "pist apply schema.sql | colorize" Enter
Sleep 4s

# Verify no remaining diff
Enter
Type "# Verify that the schema is now in sync" Enter
Sleep 2s
Type "pist plan schema.sql | colorize" Enter
Sleep 4s

# Dump
Enter
Type "# Dump the current database schema" Enter
Sleep 2s
Type "pist dump | colorize" Enter
Sleep 4s
