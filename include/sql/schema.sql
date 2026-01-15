-- ============================================================================
-- Crypto ETL Pipeline - Database Schema
-- ============================================================================
-- Purpose: Store cryptocurrency price and market data from CoinGecko API
-- Updated: Every 15 minutes
-- ============================================================================

-- Drop table if exists (for development/testing)
-- WARNING: Uncomment only if you want to reset the database
-- DROP TABLE IF EXISTS crypto_prices CASCADE;

-- Create crypto_prices table
CREATE TABLE IF NOT EXISTS crypto_prices (
    -- Primary Key
    id SERIAL PRIMARY KEY,
    
    -- Coin Identification
    coin_id VARCHAR(50) NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    name VARCHAR(100) NOT NULL,
    
    -- Price Data (DECIMAL for precision)
    price_usd DECIMAL(20,8) NOT NULL CHECK (price_usd >= 0),
    
    -- Market Data (BIGINT for large numbers)
    market_cap BIGINT,
    volume_24h BIGINT,
    
    -- Price Changes
    price_change_24h DECIMAL(20,8),
    price_change_pct_24h DECIMAL(10,4),
    
    -- 24h High/Low
    high_24h DECIMAL(20,8),
    low_24h DECIMAL(20,8),
    
    -- Timestamps
    last_updated TIMESTAMP,  -- When CoinGecko last updated
    extracted_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP  -- When we fetched it
);

-- ============================================================================
-- Indexes for Performance
-- ============================================================================

-- Index on coin_id for fast lookups by specific coin
CREATE INDEX IF NOT EXISTS idx_coin_id 
    ON crypto_prices(coin_id);

-- Index on extracted_at for time-based queries
CREATE INDEX IF NOT EXISTS idx_extracted_at 
    ON crypto_prices(extracted_at DESC);

-- Composite index for querying specific coin over time
CREATE INDEX IF NOT EXISTS idx_coin_time 
    ON crypto_prices(coin_id, extracted_at DESC);

-- ============================================================================
-- Table Comments (Documentation)
-- ============================================================================

COMMENT ON TABLE crypto_prices IS 
    'Stores cryptocurrency price and market data fetched from CoinGecko API every 15 minutes';

COMMENT ON COLUMN crypto_prices.coin_id IS 
    'CoinGecko coin identifier (e.g., bitcoin, ethereum)';

COMMENT ON COLUMN crypto_prices.symbol IS 
    'Coin symbol in uppercase (e.g., BTC, ETH)';

COMMENT ON COLUMN crypto_prices.price_usd IS 
    'Current price in USD with 8 decimal precision';

COMMENT ON COLUMN crypto_prices.extracted_at IS 
    'Timestamp when data was extracted by our ETL pipeline';

-- ============================================================================
-- Verify Table Creation
-- ============================================================================

-- Show table structure
\d crypto_prices

-- Show all indexes
\di crypto_prices*

-- Success message
SELECT 'Database schema created successfully!' AS status;
