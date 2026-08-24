-- Initialize logs database
-- This script runs when PostgreSQL container starts for the first time

-- Create logs table
CREATE TABLE IF NOT EXISTS logs (
    id VARCHAR(36) PRIMARY KEY,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    service VARCHAR(100) NOT NULL,
    level VARCHAR(20) NOT NULL,
    message TEXT NOT NULL,
    user_id VARCHAR(100),
    metadata JSONB,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    archived_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for efficient querying
CREATE INDEX IF NOT EXISTS idx_logs_timestamp ON logs(timestamp);
CREATE INDEX IF NOT EXISTS idx_logs_service ON logs(service);
CREATE INDEX IF NOT EXISTS idx_logs_level ON logs(level);
CREATE INDEX IF NOT EXISTS idx_logs_user_id ON logs(user_id) WHERE user_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_logs_created_at ON logs(created_at);

-- Create composite indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_logs_service_timestamp ON logs(service, timestamp);
CREATE INDEX IF NOT EXISTS idx_logs_level_timestamp ON logs(level, timestamp);

-- Create index on metadata for JSON queries
CREATE INDEX IF NOT EXISTS idx_logs_metadata_gin ON logs USING GIN(metadata);

-- Insert initial test data (optional)
INSERT INTO logs (id, timestamp, service, level, message, user_id, metadata, created_at)
VALUES 
    ('init-log-1', NOW(), 'database', 'INFO', 'Database initialized successfully', 'system', '{"initialization": true}', NOW())
ON CONFLICT (id) DO NOTHING;

-- Create a function to cleanup old logs (for future use)
CREATE OR REPLACE FUNCTION cleanup_old_logs(days_to_keep INTEGER DEFAULT 30)
RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    DELETE FROM logs 
    WHERE created_at < NOW() - INTERVAL '1 day' * days_to_keep;
    
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

-- Grant permissions
GRANT ALL PRIVILEGES ON TABLE logs TO postgres;
GRANT EXECUTE ON FUNCTION cleanup_old_logs(INTEGER) TO postgres;