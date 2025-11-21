-- I created this schema to ensure data integrity in the warehouse.
-- Instead of letting Pandas guess types, I strictly define them here.

CREATE TABLE IF NOT EXISTS rpt_daily_domain_trends (
    date DATE NOT NULL,
    domain VARCHAR(255) NOT NULL,
    daily_avg_value NUMERIC,
    PRIMARY KEY (date, domain)
);

CREATE TABLE IF NOT EXISTS rpt_location_performance (
    location VARCHAR(255) PRIMARY KEY,
    avg_txn_value NUMERIC,
    avg_txn_count NUMERIC
);

CREATE TABLE IF NOT EXISTS rpt_domain_leaderboard (
    domain VARCHAR(255) PRIMARY KEY,
    total_value NUMERIC,
    rank INTEGER
);