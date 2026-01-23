-- Enterprise Agentic Data Platform
-- DDL: PostgreSQL Extensions
-- Version: 1.0.0

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- JSONB support (built-in, but ensure latest features)
-- No extension needed for JSONB

-- Comments
COMMENT ON EXTENSION "uuid-ossp" IS 'Functions for generating UUIDs';
COMMENT ON EXTENSION "pgcrypto" IS 'Cryptographic functions for secure hashing';
