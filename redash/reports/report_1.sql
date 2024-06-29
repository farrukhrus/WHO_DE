select distinct d.Title, i.Value, i.SpatialDim as "Country Code", i.TimeDim as "Year",d2.Title as "Country Name" 
from who_stg.INDICATORS i 
inner join who_stg.DIMENSIONS d on i.Dim1Type = d.DimCode
inner join who_stg.DIMENSIONS d2 on i.SpatialDimType = d2.DimCode

where IndicatorCode = 'SA_0000001398'
and i.SpatialDim in ('SRB', 'RUS', 'USA', 'DEU')
and i.Dim1 = d.Code
and i.SpatialDim = d2.Code
group by d.Title, i.Value, i.SpatialDim as "Country Code", i.TimeDim as "Year",d2.Title as "Country Name" 