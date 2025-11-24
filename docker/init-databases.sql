-- Create DB for app with RLS
CREATE DATABASE rlp OWNER root;

-- Create DB for SpiceDB
CREATE DATABASE rls_spicedb OWNER root;

-- Optional: explicitly grant (not really needed since root is superuser)
GRANT ALL PRIVILEGES ON DATABASE rlp TO root;
GRANT ALL PRIVILEGES ON DATABASE rls_spicedb TO root;