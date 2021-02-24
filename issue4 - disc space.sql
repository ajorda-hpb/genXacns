


select 'hro'[Set]
    ,cast(LastUpdate as date)[UpdtDt]
    ,count(*)[NumRows]
from ReportsView..hroXacns
group by cast(LastUpdate as date)
union
select 'ng'[Set]
    ,cast(LastUpdate as date)[UpdtDt]
    ,count(*)[NumRows]
from ReportsView..ngXacns
group by cast(LastUpdate as date)
order by 2,1


select 'hro'[Set]
    ,cast([date] as date)[UpdtDt]
    ,count(*)[NumRows]
from ReportsView..hroXacns
group by cast([date] as date)
union
select 'ng'[Set]
    ,cast([date] as date)[UpdtDt]
    ,count(*)[NumRows]
from ReportsView..ngXacns
group by cast([date] as date)
order by 2,1


drop table if exists #fancy
select 'hro'[Set]
    ,Item
    ,min(Date)[minDt]
    ,max(Date)[maxDt]
    ,count(*)[NumRows]
into #fancy
from ReportsView..hroXacns
group by Item
union
select 'ng'[Set]
    ,Item
    ,min(Date)[minDt]
    ,max(Date)[maxDt]
    ,count(*)[NumRows]
from ReportsView..ngXacns
group by Item


select *
    ,datediff(YY,minDt,getdate())[YrsOld]
    ,datediff(YY,maxDt,getdate())[YrsInac]
    ,row_number() over(partition by [Set] order by NumRows desc)[ItemIdx]
from #fancy
order by 2,1