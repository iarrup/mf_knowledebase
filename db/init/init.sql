-- Create pgvector extension
-- This must be run by a superuser (which the 'admin' user is during init)
CREATE EXTENSION IF NOT EXISTS vector;

-- Create Apache AGE extension
CREATE EXTENSION IF NOT EXISTS age;
LOAD 'age';
SET search_path = ag_catalog, "$user", public;