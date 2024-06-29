CREATE OR REPLACE TABLE who_stg.INDICATORS (
	`Id` String,
	`IndicatorCode` String,
	`SpatialDimType` String,
	`SpatialDim` String,
	`ParentLocationCode` String,
	`TimeDimType` String,
	`ParentLocation` String,
	`Dim1Type` String,
	`Dim1` String,
	`TimeDim` String,
	`Dim2Type` String,
	`Dim2` String,
	`Dim3Type` String,
	`Dim3` String,
	`DataSourceDimType` String,
	`DataSourceDim` String,
	`Value` String,
	`NumericValue` String,
	`Low` String,
	`High` String,
	`Comments` String,
	`Date` String,
	`TimeDimensionValue` String,
	`TimeDimensionBegin` String,
	`TimeDimensionEnd` String
)
ENGINE = ReplacingMergeTree
ORDER BY Id
SETTINGS index_granularity = 8192;