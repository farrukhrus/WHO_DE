CREATE OR REPLACE TABLE who_stg.D_DIMENSION (
	`Code` String,
	`Title` String
)
ENGINE = ReplacingMergeTree
ORDER BY Code
SETTINGS index_granularity = 8192;