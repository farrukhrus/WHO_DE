CREATE OR REPLACE TABLE who_stg.D_INDICATOR (
	`IndicatorCode` String,
	`IndicatorName` String
)
ENGINE = ReplacingMergeTree
ORDER BY IndicatorCode
SETTINGS index_granularity = 8192;