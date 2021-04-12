----Pithy Age of Inventory------------------------------------[
-------------------------------------------------------------[
/*
drop table if exists ReportsView.dbo.genXacns_Ages
create table ReportsView.dbo.genXacns_Ages
	(Loc char(5) not null
	,Item varchar(20) not null
	,ItemCode varchar(20) not null
	,SkuExt varchar(10) not null
	,u int not null
	,Xacn varchar(10) not null
	,Date datetime not null
	,flow varchar(10) not null
	,Qty int

	,InvDays numeric(24,12)
	,AjInvDays numeric(24,12)
	,NAjInvDays numeric(24,12)
	,FAjInvDays numeric(24,12)

	,QtyDays int
	,AjQtyDays int
	,NAjQtyDays int
	,FAjQtyDays int

	,aInvAge numeric(24,12)
	,mInvAge numeric(24,12)
	,aAjInvAge numeric(24,12)
	,mAjInvAge numeric(24,12)
	,aNAjInvAge numeric(24,12)
	,mNAjInvAge numeric(24,12)
	,aFAjInvAge numeric(24,12)
	,mFAjInvAge numeric(24,12)

	,QtyAge numeric(24,12)
	,AjQtyAge numeric(24,12)
	,NAjQtyAge numeric(24,12)
	,FAjQtyAge numeric(24,12)

	,constraint PK_genXacnsAges primary key(Loc,Item,ItemCode,u));
*/
USE [ReportsView]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER procedure [dbo].[CDC_genXacns_04_Ages_REBUILD]
as 
/*Change Notes for genXacns_Age logic....

11/7/19: Fixed aSepta formula to replicate Septa when the activity is an in-flow.
11/12/19: Added Septas for AdjInvQ, nAdjInvQ, & fAdjInvQ
12/2/2020: Added Item to table & Primary Key to cover case when a UPC's pointer item changes.
*/


----Transaction 1--------------------------------
--Clear out old SetAdjs data---------------------[
-----------------------------------------------[
BEGIN TRY
	begin transaction

	truncate table ReportsView..genXacns_Ages;
	
	commit transaction;
END TRY
BEGIN CATCH
    if @@trancount > 0 rollback transaction
    declare @msg1 nvarchar(2048) = error_message()  
    raiserror (@msg1, 16, 1)
END CATCH
-----------------------------------------------]
------------------------------------------------]


----Transaction 2--------------------------------
-- Simple table of inbound activity--------------[
-----------------------------------------------[
BEGIN TRY
	begin transaction

	;with inQtys as(
		select Loc,Item,u,Date,Xacn,Qty
			,row_number() over(partition by Loc,Item order by u desc)[dInU]
		from ReportsView..genXacns 
		where flow = 'in'
	) 
	,maxInU as(
		select n.Loc,n.Item,n.ItemCode,n.SkuExt,n.Xacn,n.Date,n.u,n.flow,n.Qty
			,datediff(HH,iq.Date,n.Date) / 24.0[InAge]
			--(Post Activity) Inventory Qty's
			,n.InvQ
			,n.InvQ + sa.AdjQ [aInvQ]
			,n.InvQ + sa.nAdjQ [nInvQ]
			,n.InvQ + sa.fAdjQ [fInvQ]
			--Before Activity (ba) Inventory Qty's
			,n.InvQ - isnull(n.Qty,0)[baInvQ]
			,n.InvQ - isnull(n.Qty,0) + sa.AdjQ[baAjInvQ]
			,n.InvQ - isnull(n.Qty,0) + sa.nAdjQ[baNAjInvQ]
			,n.InvQ - isnull(n.Qty,0) + sa.fAdjQ[baFAjInvQ]
			,iq.dInU
			,iq.Qty[InQty]
			,sum(iq.Qty) over(partition by n.Loc,n.Item,n.u order by iq.Date rows between current row and unbounded following)
				--Back out current activity if that activity is inbound, since it otherwise affects the running total of inbound qty
				- case when n.flow = 'in' then n.Qty else 0 end[btdInQty]
		from ReportsView..genXacns n inner join ReportsView..genXacns_SetAdjs sa 
			on n.Loc = sa.Loc and n.ItemCode = sa.ItemCode and n.Item = sa.Item 
				and sa.crSet = n.crSet and sa.nxIn = n.nxIn
			inner join inQtys iq on n.Loc = iq.Loc and n.Item = iq.Item 
		where iq.Date <= n.Date and iq.u <= n.u
	)
	,sept as(
		select mi.*
			--Septa Calcs on InvQ as-is--------------------------------
			,case when btdInQty = 0 then 0
				when mi.baInvQ >= btdInQty then InQty
				else (case when baInvQ - btdInQty + InQty <=0 then 0
						else baInvQ - btdInQty + InQty end) end[Septa] 

			,sum(case when btdInQty = 0 then 0
					when mi.baInvQ >= btdInQty then InQty
					else (case when baInvQ - btdInQty + InQty <=0 then 0
							else baInvQ - btdInQty + InQty end) end) 
				over(partition by mi.Loc,mi.ItemCode,mi.u order by dInU 
						rows between current row and unbounded following)[ruSepta] 

			,case when mi.flow <> 'in' 
				then (case when btdInQty = 0 then 0
						when mi.InvQ >= btdInQty then InQty
						else (case when InvQ - btdInQty + InQty <=0 then 0
								else InvQ - btdInQty + InQty end) end)
				else (case when btdInQty = 0 then 0
						when mi.baInvQ >= btdInQty then InQty
						else (case when baInvQ - btdInQty + InQty <=0 then 0
								else baInvQ - btdInQty + InQty end) end) end[aSepta]

			--Septa Calcs on aInvQ--------------------------------------
			,case when btdInQty = 0 then 0
				when mi.baAjInvQ >= btdInQty then InQty
				else (case when baAjInvQ - btdInQty + InQty <=0 then 0
						else baAjInvQ - btdInQty + InQty end) end[AjSepta]

			,sum(case when btdInQty = 0 then 0
					when baAjInvQ >= btdInQty then InQty
					else (case when baAjInvQ - btdInQty + InQty <=0 then 0
							else baAjInvQ - btdInQty + InQty end) end) 
				over(partition by mi.Loc,mi.ItemCode,mi.u order by dInU 
						rows between current row and unbounded following)[ruAjSepta] 

			,case when mi.flow <> 'in' 
				then (case when btdInQty = 0 then 0
						when mi.aInvQ >= btdInQty then InQty
						else (case when aInvQ - btdInQty + InQty <=0 then 0
								else aInvQ - btdInQty + InQty end) end)
				else (case when btdInQty = 0 then 0
						when mi.baAjInvQ >= btdInQty then InQty
						else (case when baAjInvQ - btdInQty + InQty <=0 then 0
								else baAjInvQ - btdInQty + InQty end) end) end[aAjSepta]
						
			--Septa Calcs on naInvQ-------------------------------------
			,case when btdInQty = 0 then 0
				when mi.baNAjInvQ >= btdInQty then InQty
				else (case when baNAjInvQ - btdInQty + InQty <=0 then 0
						else baNAjInvQ - btdInQty + InQty end) end[NAjSepta]

			,sum(case when btdInQty = 0 then 0
					when baNAjInvQ >= btdInQty then InQty
					else (case when baNAjInvQ - btdInQty + InQty <=0 then 0
							else baNAjInvQ - btdInQty + InQty end) end) 
				over(partition by mi.Loc,mi.ItemCode,mi.u order by dInU 
						rows between current row and unbounded following)[ruNAjSepta] 

			,case when mi.flow <> 'in' 
				then (case when btdInQty = 0 then 0
						when mi.nInvQ >= btdInQty then InQty
						else (case when nInvQ - btdInQty + InQty <=0 then 0
								else nInvQ - btdInQty + InQty end) end)
				else (case when btdInQty = 0 then 0
						when mi.baNAjInvQ >= btdInQty then InQty
						else (case when baNAjInvQ - btdInQty + InQty <=0 then 0
								else baNAjInvQ - btdInQty + InQty end) end) end[aNAjSepta]
						
			--Septa Calcs on faInvQ-------------------------------------
			,case when btdInQty = 0 then 0
				when mi.baFAjInvQ >= btdInQty then InQty
				else (case when baFAjInvQ - btdInQty + InQty <=0 then 0
						else baFAjInvQ - btdInQty + InQty end) end[FAjSepta]

			,sum(case when btdInQty = 0 then 0
					when baFAjInvQ >= btdInQty then InQty
					else (case when baFAjInvQ - btdInQty + InQty <=0 then 0
							else baFAjInvQ - btdInQty + InQty end) end) 
				over(partition by mi.Loc,mi.ItemCode,mi.u order by dInU 
						rows between current row and unbounded following)[ruFAjSepta] 

			,case when mi.flow <> 'in' 
				then (case when btdInQty = 0 then 0
						when mi.fInvQ >= btdInQty then InQty
						else (case when fInvQ - btdInQty + InQty <=0 then 0
								else fInvQ - btdInQty + InQty end) end)
				else (case when btdInQty = 0 then 0
						when mi.baFAjInvQ >= btdInQty then InQty
						else (case when baFAjInvQ - btdInQty + InQty <=0 then 0
								else baFAjInvQ - btdInQty + InQty end) end) end[aFAjSepta]
		from maxInU mi
	)
	insert into ReportsView.dbo.genXacns_Ages
	select se.Loc,se.Item,se.ItemCode,se.SkuExt,se.u,se.Xacn,se.Date,se.flow,se.Qty
		--Inventory's QtyDays
		,sum(InAge * Septa)[InvDays]
		,sum(InAge * AjSepta)[AjInvDays]
		,sum(InAge * NAjSepta)[NAjInvDays]
		,sum(InAge * FAjSepta)[FAjInvDays]
		--Activity Qty's QtyDays
		,sum(InAge * case when flow = 'out' then Septa - aSepta else 0 end)[QtyDays]
		,sum(InAge * case when flow = 'out' then AjSepta - aAjSepta else 0 end)[AjQtyDays]
		,sum(InAge * case when flow = 'out' then NAjSepta - aNAjSepta else 0 end)[NAjQtyDays]
		,sum(InAge * case when flow = 'out' then FAjSepta - aFAjSepta else 0 end)[FAjQtyDays]
		--Avg & Median Inventory Age
		,sum(InAge * Septa) / nullif(sum(Septa),0)[aInvAge]
		,case when max(baInvQ) % 2 = 0 
			then max(case when Septa <> 0 and baInvQ/2+1 <= ruSepta then InAge end) * 0.5
					+ max(case when Septa <> 0 and baInvQ/2 <= ruSepta then InAge end) * 0.5
			else max(case when Septa <> 0 and baInvQ/2.0 <= ruSepta then InAge end) end[mInvAge]		  
		,sum(InAge * AjSepta) / nullif(sum(AjSepta),0)[aAjInvAge]
		,case when max(baAjInvQ) % 2 = 0 
			then max(case when AjSepta <> 0 and baAjInvQ/2+1 <= ruAjSepta then InAge end) * 0.5
					+ max(case when AjSepta <> 0 and baAjInvQ/2 <= ruAjSepta then InAge end) * 0.5
			else max(case when AjSepta <> 0 and baAjInvQ/2.0 <= ruAjSepta then InAge end) end[mAjInvAge]
		,sum(InAge * NAjSepta) / nullif(sum(NAjSepta),0)[aNAjInvAge]
		,case when max(baNAjInvQ) % 2 = 0 
			then max(case when NAjSepta <> 0 and baNAjInvQ/2+1 <= ruNAjSepta then InAge end) * 0.5
					+ max(case when NAjSepta <> 0 and baNAjInvQ/2 <= ruNAjSepta then InAge end) * 0.5
			else max(case when NAjSepta <> 0 and baNAjInvQ/2.0 <= ruNAjSepta then InAge end) end[mNAjInvAge]
		,sum(InAge * FAjSepta) / nullif(sum(FAjSepta),0)[aFAjInvAge]
		,case when max(baFAjInvQ) % 2 = 0 
			then max(case when FAjSepta <> 0 and baFAjInvQ/2+1 <= ruFAjSepta then InAge end) * 0.5
					+ max(case when FAjSepta <> 0 and baFAjInvQ/2 <= ruFAjSepta then InAge end) * 0.5
			else max(case when FAjSepta <> 0 and baFAjInvQ/2.0 <= ruFAjSepta then InAge end) end[mFAjInvAge]
		--Activity Qty's avg Age
		,sum(InAge * case when flow = 'out' then Septa - aSepta else 0 end) / nullif(abs(se.Qty),0)[QtyAge]
		,sum(InAge * case when flow = 'out' then AjSepta - aAjSepta else 0 end) / nullif(abs(se.Qty),0)[AjQtyAge]
		,sum(InAge * case when flow = 'out' then NAjSepta - aNAjSepta else 0 end) / nullif(abs(se.Qty),0)[NAjQtyAge]
		,sum(InAge * case when flow = 'out' then FAjSepta - aFAjSepta else 0 end) / nullif(abs(se.Qty),0)[FAjQtyAge]
	from sept se
	group by se.Loc,se.Item,se.ItemCode,se.SkuExt,se.u,se.Xacn,se.Date,se.flow,se.Qty;
	
	commit transaction;
END TRY
BEGIN CATCH
    if @@trancount > 0 rollback transaction
    declare @msg2 nvarchar(2048) = error_message()  
    raiserror (@msg2, 16, 1)
END CATCH
-----------------------------------------------]
------------------------------------------------]




