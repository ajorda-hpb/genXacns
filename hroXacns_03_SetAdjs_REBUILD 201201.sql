
-- 12/2/2020: Added Item field to PK in order to cover case when a UPC's pointer item code changes.

SET STATISTICS IO ON

/*
drop table if exists ReportsView.dbo.hroXacns_SetAdjs
create table ReportsView.dbo.hroXacns_SetAdjs
	(Loc char(5) not null
	,ItemCode varchar(20) not null
	,Item varchar(20) not null
	,crSet int not null
	,nxIn int not null
	,AdjQ int
	,minInvQ int
	,minAInvQ int
	,nAdjQ int
	,fAdjQ int
	,fCC int
	,Span int
	,PctAdj numeric(24,12)
	,fPctAdj numeric(24,12)
	,constraint PK_hroXacnsSetAdjs primary key(Loc,ItemCode,Item,crSet,nxIn))
*/

truncate table ReportsView..hroXacns_SetAdjs

--When hroXacns DOES have crSet & nxIn...
--    drop table if exists ReportsView.dbo.hroXacns_SetAdjs
;with fAdjQ_means as(
	select Loc,ItemCode,Item,Date,u,InvQ,crSet,nxIn
		,sum(isnull(Inv-InvQ,0)) over(partition by Loc,Item,crSet)[AdjQ]
	from ReportsView..hroXacns
	)
,fAdjQ_prep as(
	select fa.Loc
		,fa.ItemCode
		,fa.Item
		--,fa.Date
		,fa.crSet
		,fa.nxIn
		,fa.AdjQ
		,min(fa.InvQ) over(partition by Loc,Item,crSet)[minInvQ]
		,min(fa.InvQ + fa.AdjQ) over(partition by Loc,Item,crSet)[minAInvQ]
		--when InvQ never falls below zero over the crSet
		,case when min(fa.InvQ + fa.AdjQ) over(partition by Loc,Item,crSet) >= 0 then fa.AdjQ
			--when InvQ falls below zero, but not YET.
			else case when min(fa.InvQ + fa.AdjQ) over(partition by Loc,Item,crSet order by nxIn rows between unbounded preceding and current row) >=0 then fa.AdjQ
					  --when InvQ has fallen below zero
					  else -min(fa.InvQ) over(partition by Loc,Item,crSet order by fa.u rows between unbounded preceding and current row)end end[nAdjQ]
		--when InvQ never falls below zero over the crSet
		,case when min(fa.InvQ + fa.AdjQ) over(partition by Loc,Item,crSet) >= 0 then fa.AdjQ
			--when InvQ falls below zero, but not YET.
			else case when min(fa.InvQ + fa.AdjQ) over(partition by Loc,Item,crSet order by nxIn rows between unbounded preceding and current row) >=0 then fa.AdjQ
					  --when InvQ has fallen below zero
					  else -min(fa.InvQ) over(partition by Loc,Item,crSet)end end[fAdjQ]
		,case when min(fa.InvQ + fa.AdjQ) over(partition by Loc,Item,crSet) >= 0 then 0 --'use AdjQ'
			else case when min(fa.InvQ + fa.AdjQ) over(partition by Loc,Item,crSet order by nxIn rows between unbounded preceding and current row) >=0 then 1 --'pre-neg'
					  else 2 --'neg-zone'
					  end end[fCC]
	from fAdjQ_means fa
	)
insert into ReportsView.dbo.hroXacns_SetAdjs
select fa.Loc,fa.ItemCode,fa.Item
	,fa.crSet,fa.nxIn,fa.AdjQ
	,fa.minInvQ,fa.minAInvQ
	--,max(fa.Date)[maxDt]
	--Double rows in the nxIn grp that transitions to neg-zone.
	--Therefore want the larger adjustment.
	,max(fa.nAdjQ)[nAdjQ]
	,max(fa.fAdjQ)[fAdjQ]
	,max(fa.fCC)[fCC]
	--,count(distinct fa.fAdjQ)[NumDets]
	--fAdjQ validations....
	,fa.nxIn - fa.crSet[Span]
	,abs(max(fa.AdjQ) * 1.0 / nullif(fa.minInvQ,0))[PctAdj]
	,abs(max(fa.fAdjQ) * 1.0 / nullif(fa.minInvQ,0))[fPctAdj]
from fAdjQ_prep fa
group by fa.Loc,fa.ItemCode,fa.Item,fa.crSet,fa.nxIn,fa.AdjQ,fa.minInvQ,fa.minAInvQ



--create nonclustered index idx_hroXacnsSetAdjs_Loc on ReportsView.dbo.hroXacns_SetAdjs(Loc) include(ItemCode,Item,crSet,nxIn,nAdjQ,fAdjQ)

