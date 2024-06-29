# map
select distinct SpatialDim, Value 
from who_stg.INDICATORS where IndicatorCode = 'RS_208' group by SpatialDim, Value;