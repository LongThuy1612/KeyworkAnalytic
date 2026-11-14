-- Google Ads Transparency Center signal: does anyone OTHER than the website's own advertiser run Google ads that point
-- to this domain? (third-party advertisers = affiliates/partners already paying for traffic to it). One row per domain.
CREATE TABLE IF NOT EXISTS domain_ads_signal (
  domain VARCHAR(191) NOT NULL PRIMARY KEY,
  has_ads TINYINT(1) NOT NULL DEFAULT 0,
  has_other_advertisers TINYINT(1) NOT NULL DEFAULT 0,
  advertiser_count INT NOT NULL DEFAULT 0,
  other_advertiser_count INT NOT NULL DEFAULT 0,
  owner_match TINYINT NOT NULL DEFAULT 0,      -- 1 owner found by brand name, 2 by dominance, 0 unknown
  sampled_ads INT NOT NULL DEFAULT 0,
  ads_range_low INT NULL,
  ads_range_high INT NULL,
  other_names JSON NULL,                        -- at most 3 names, for a human glance only
  checked_at DATETIME(3) NOT NULL,
  INDEX idx_dasig_checked (checked_at),
  INDEX idx_dasig_other (has_other_advertisers)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
