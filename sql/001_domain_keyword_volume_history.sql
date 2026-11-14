-- One row per Keyword Planner lookup, appended on every run (domain_keyword_volume keeps only the latest).
-- The service never issues DDL: apply this once, by hand, on the crawl database.
CREATE TABLE IF NOT EXISTS domain_keyword_volume_history (
  id INT AUTO_INCREMENT PRIMARY KEY,
  domain VARCHAR(191) NOT NULL,
  keyword VARCHAR(255) NOT NULL,
  fetched_at DATETIME(3) NOT NULL,
  avg_monthly_searches INT NULL,
  three_month_change VARCHAR(20) NULL,
  yoy_change VARCHAR(20) NULL,
  competition VARCHAR(20) NULL,
  competition_index INT NULL,
  low_top_of_page_bid INT NULL,
  high_top_of_page_bid INT NULL,
  monthly_trend JSON NULL,
  UNIQUE KEY uq_dkvh (domain, keyword, fetched_at),
  INDEX idx_dkvh_domain_time (domain, fetched_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
