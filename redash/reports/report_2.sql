select DISTINCT d.ParentTitle as "Region", i.SpatialDim as "Country Code", d.Title as "Country Name", o.IndicatorName as "Indicator", i.Dim1 as "Year",  i.Value from who_stg.INDICATORS i 
join (select distinct IndicatorCode, IndicatorName FROM who_stg.D_INDICATOR) AS o
on i.IndicatorCode = o.IndicatorCode
JOIN who_stg.DIMENSIONS d on i.SpatialDim = d.Code
where o.IndicatorName like 'Alcohol-related%'
and i.SpatialDimType = 'COUNTRY'
and d.Dimension = i.SpatialDimType
and i.Dim1Type = ''
and i.IndicatorCode not like '%_ARCHIVED'
and i.SpatialDim = 'SRB'
group by d.ParentTitle as "Region", i.SpatialDim as "Country Code", d.Title as "Country Name", o.IndicatorName as "Indicator", i.Dim1 as "Year",  i.Value