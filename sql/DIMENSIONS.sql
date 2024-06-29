CREATE OR REPLACE TABLE who_stg.DIMENSIONS (
	`DimCode` String,
	`Code` String,
	`Title` String,
	`ParentDimension` String,
	`Dimension` String,
	`ParentCode` String,
	`ParentTitle` String
)
ENGINE = ReplacingMergeTree
ORDER BY Code
SETTINGS index_granularity = 8192;