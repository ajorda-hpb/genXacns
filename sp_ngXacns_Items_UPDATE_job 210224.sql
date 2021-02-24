USE [ReportsView]
GO

/****** Object:  StoredProcedure [dbo].[CDC_ngXacns_Items_UPDATE_job]    Script Date: 2/24/2021 2:36:09 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



CREATE procedure [dbo].[CDC_ngXacns_Items_UPDATE_job]
as

--CHANGE LOG------------------------------------------------------------------------------------
--12/04/19: sp version of file: ngXacns_Items UPDATE job 191122.sql
--02/21/20: Expanded list of sections to include Drinkware & Plush, which are now pull sections


/*
drop table if exists #excludes
drop table if exists #set0_base
drop table if exists #set0
drop table if exists #set0_Items
drop table if exists #items
*/

SET XACT_ABORT, NOCOUNT ON;

declare @minDate datetime
set @minDate = dateadd(DD,-367,getdate())  --

----List of DIPS ItemCode excludes--------------------
------------------------------------------------------
--    drop table if exists #excludes
select pm.ItemCode
into #excludes
--    select *
from ReportsData..ProductMaster pm with(nolock)
	left join ReportsData..BaseInventory bi with(nolock) on pm.ItemCode = bi.ItemCode
where (pm.ItemCode in 
			('00000000000010199940','00000000000010199941','00000000000010199942','00000000000010199943' -- CX, LP, MG, MSCU
			,'00000000000010199944','00000000000010199945','00000000000010199946','00000000000010199947' -- PB, UN, BKM, ELTU
			,'00000000000010199938','00000000000010199939','00000000000010211475','00000000000010211476' -- CDU, CSU, BCD, BTU
			,'00000000000010200071','00000000000010200072','00000000000010200073' -- DVD, VGU, SWU
			,'00000000000010200074'  -- Misc/Generic/Unscannable DISTRO item
			,'00000000000010051490','00000000000010196953','00000000000010200047')  -- Sticker King & Bag Charges
		or bi.ItemCode is not null
		or pm.ProductType in ('HPBP', 'PRMO','CHA ','PGC ','EGC ')
		or pm.UserChar15 = 'SUP'
		or pm.PurchaseFromVendorID = 'WHPBSUPPLY')
	--Tote bags are the bane of my existence. Need to actually track since they are reorderable, though mostly sell for free.
	and ltrim(rtrim(pm.PurchaseFromVendorID)) not in ('IDTXBKSTAP','TEXASBKNON')
	and ltrim(rtrim(pm.VendorID)) not in ('IDTXBKSTAP','IDPOLLOCKP');


----List of items, old and new, being included in ngXacns--------------------
--This includes all currently reorderable TTB items for SuggestedOrders data
--& the currently-designated pull sections, as shown in PullSectionsRpt
-----------------------------------------------------------------------------
--     drop table if exists #set0_base
--   declare @minDate datetime set @minDate = dateadd(DD,-367,getdate())
select pm.ItemCode     
	,pm.PurchaseFromVendorID [pfVndr]
into #set0_base
from ReportsData..ProductMaster pm with(nolock)
where pm.ItemCode not in (select ItemCode from #excludes)
	and (ltrim(rtrim(pm.SectionCode)) in ('BLANK','BOOKLIGHT','BOOKMARK','HEADPHONES','NOTECARDS','PUZZLES','EYEGLASSES','METALSIGNS','DRINKWARE','PLUSH')
		--Taking out the Reorderable = 'Y' allows stuff that WAS that may be again or may still be in inv.
		or (/*pm.Reorderable = 'Y' and*/ pm.PurchaseFromVendorID in ('TEXASBKMNA','TEXASBKMNB','TEXASBKNON','TEXASSTATI'))
		or (pm.UserChar15 = 'TTB' and pm.CreateDate > @minDate));
create index idx_Set0Base_ItemCode on #set0_base(ItemCode);


----Cross-Referencing for UPC's-----------------------------
--      drop table if exists #set0
select pm.ItemCode
	,case when (rtrim(ltrim(pmd.ReportItemCode))= '' or pmd.ReportItemCode is null) then pm.ItemCode else pmd.ReportItemCode end[RptIt]
into #set0
from ReportsData..ProductMaster pm with(nolock)
	inner join ReportsData..ProductMasterDist pmd on pm.ItemCode = pmd.ItemCode
	inner join #set0_base it on pm.ItemCode = it.ItemCode or pmd.ReportItemCode = it.ItemCode
	left join #excludes ex on pm.ItemCode = ex.ItemCode
where ex.ItemCode is null
group by pm.ItemCode
	,case when (rtrim(ltrim(pmd.ReportItemCode))= '' or pmd.ReportItemCode is null) then pm.ItemCode else pmd.ReportItemCode end;
create index idx_Set0_ItemCode on #set0(ItemCode);
create index idx_Set0_RptIt on #set0(RptIt);
	
--Adds in ItemCode records for any linked-to ReportItemCodes 
----or any ItemCodes that show up as the ReportItemCode for ItemCodes not yet in the list
--Piecemeal insertion is faster than a laundry list of or's in the #set0 join.
insert into #set0
select pm.ItemCode
	,case when (rtrim(ltrim(pmd.ReportItemCode))= '' or pmd.ReportItemCode is null) then pm.ItemCode else pmd.ReportItemCode end[RptIt]
from ReportsData..ProductMaster pm with(nolock)
	inner join ReportsData..ProductMasterDist pmd on pm.ItemCode = pmd.ItemCode
	inner join #set0 ep on pm.ItemCode = ep.RptIt --RptItem isn't yet on list of #set0
where pm.ItemCode not in (select distinct itemCode from #set0)
--Group By required since ep.RptIt is not guaranteed to be unique
group by pm.ItemCode
	,case when (rtrim(ltrim(pmd.ReportItemCode))= '' or pmd.ReportItemCode is null) then pm.ItemCode else pmd.ReportItemCode end;
	
insert into #set0
select pm.ItemCode
	,case when (rtrim(ltrim(pmd.ReportItemCode))= '' or pmd.ReportItemCode is null) then pm.ItemCode else pmd.ReportItemCode end[RptIt]
from ReportsData..ProductMaster pm with(nolock)
	inner join ReportsData..ProductMasterDist pmd on pm.ItemCode = pmd.ItemCode
	inner join #set0 ep on pmd.ReportItemCode = ep.RptIt --Other ItemCodes that also point to RptItem that aren't yet on list of #set0
where pm.ItemCode not in (select distinct itemCode from #set0)
--Group By required since ep.RptIt is not guaranteed to be unique
group by pm.ItemCode
	,case when (rtrim(ltrim(pmd.ReportItemCode))= '' or pmd.ReportItemCode is null) then pm.ItemCode else pmd.ReportItemCode end;
	
insert into #set0
select pm.ItemCode
	,case when (rtrim(ltrim(pmd.ReportItemCode))= '' or pmd.ReportItemCode is null) then pm.ItemCode else pmd.ReportItemCode end[RptIt]
from ReportsData..ProductMaster pm with(nolock)
	inner join ReportsData..ProductMasterDist pmd on pm.ItemCode = pmd.ItemCode
	inner join #set0 ep on pmd.ReportItemCode = ep.ItemCode	--Other ItemCodes for deprecated UPC's pointing to an old RptItem instead of the current RptItem
where pm.ItemCode not in (select distinct itemCode from #set0)
--Group By required since pmd.ReportItemCode is not guaranteed to be unique
group by pm.ItemCode
	,case when (rtrim(ltrim(pmd.ReportItemCode))= '' or pmd.ReportItemCode is null) then pm.ItemCode else pmd.ReportItemCode end;

--Some item codes fail to have their RptIt updated by whatever auto-update processes exist in the DIPS database. 
--This fixes it by replacing current RptIt with whatever RptIt's RptIt appears as.
update tar
set tar.RptIt = src.RptIt
from #set0 tar inner join #set0 src
	on tar.RptIt = src.ItemCode;

--Housekeeping...
drop table if exists #set0_base


--Create the Item field (replace UPC with the ItemCode containing that UPC in ItemAlias)
--Needed because UPCs don't get SICCs on their own (aside from that one batch in 2014),
--only as the ItemCode that points to them via ItemAlias.
--    drop table if exists #set0_Items
select s0.ItemCode
	,coalesce(pm.ItemCode,s0.ItemCode)[Item]
	,s0.RptIt
into #set0_Items
from #set0 s0 left join ReportsData..ProductMaster pm with(nolock) 
	on s0.itemCode = right('0000000000'+right(pm.ItemAlias,12),20)

--Manual Item & RptIt OVerwrites----------------------------------------
--Fix outdated ReportItemCode for an old UPC code that's still active
update #set0_Items
set RptIt = '00000000000010199979'
where ItemCode = '00000000814229022213'
--And an untethered UPC code that doesn't properly point to its parent ItemCode
update #set0_Items
set Item = '00000000000010086153'
where ItemCode = '00000000713755003560'
--And a pair of OLD Assort-derived items that don't use ReportItemCode at all.
update #set0_Items
set Item = '00000000000000178790'
where ItemCode in ('00000000000000202614','00000000000000202626')
--And some... uniquely managed TTB bookmarks from an old assortment.
update #set0_Items
set Item = '00000000000001404029'
	,RptIt = '00000000000001404029'
where ItemCode in ('00000000000001703280','00000000000001703279','00000000000001703274','00000000000001703273'
				  ,'00000000830395033525','00000000830395033518','00000000830395033495','00000000830395033501')


----Final Staging List of [@Vendor] Items------------------------
--Using the UPC/ISBN of the RptIt since ItemCode-is-a-UPC items
--won't have anything in the UPC field, unlike its RptIt.
-----------------------------------------------------------------
--    drop table if exists #items
select s0.ItemCode,s0.Item,s0.RptIt
	,isnull(ri.PurchaseFromVendorID,ri.VendorID)[riVendorID]
	,ltrim(rtrim(ri.SectionCode))[riSection]
	,ic.Cost[icCost]
	,ri.ISBN[riISBN]
	,rid.UPC[riUPC]
into #items
from #set0_Items s0
	inner join ReportsData..ProductMaster ic with(nolock) on s0.ItemCode = ic.ItemCode
	inner join ReportsData..ProductMaster ri with(nolock) on s0.RptIt = ri.ItemCode
	inner join ReportsData..ProductMasterDist rid with(nolock) on s0.RptIt = rid.ItemCode
group by s0.ItemCode,s0.Item,s0.RptIt
	,isnull(ri.PurchaseFromVendorID,ri.VendorID)
	,ltrim(rtrim(ri.SectionCode))
	,ic.Cost,ri.ISBN,rid.UPC
drop table if exists #set0


----Transaction 1--------------------------------
--Updates to existing ngXacns_Items-------------[
-----------------------------------------------[
BEGIN TRY
	begin transaction

		update ReportsView.dbo.ngXacns_Items
		set Item = src.Item
			,RptIt = src.RptIt
			,riVendorID = src.riVendorID
			,riSection = src.riSection
			,icCost = src.icCost
			,riISBN = src.riISBN
			,riUPC = src.riUPC
			,LastUpdate = getdate()
		from ReportsView.dbo.ngXacns_Items tp 
			join #items src on tp.ItemCode=src.ItemCode
		where tp.Item <> src.Item 
			or tp.RptIt <> src.RptIt
			or tp.riVendorID <> src.riVendorID
			or tp.riSection <> src.riSection
			or tp.icCost <> src.icCost
			or tp.riISBN <> src.riISBN
			or tp.riUPC <> src.riUPC;
			
	commit transaction
END TRY
BEGIN CATCH
	if @@trancount > 0 rollback transaction
	declare @msg1 nvarchar(2048) = error_message()  
	raiserror (@msg1, 16, 1)
END CATCH
-----------------------------------------------]
------------------------------------------------]
		


----Transaction 2--------------------------------
--Inserting NEW items ngXacns_Items-------------[
-----------------------------------------------[
BEGIN TRY
	begin transaction
	
	insert into ReportsView.dbo.ngXacns_Items
	select it.ItemCode
		,it.Item
		,it.RptIt
		,it.riVendorID
		,it.riSection
		,it.icCost
		,it.riISBN
		,it.riUPC
		,getdate()
	from #items it 
		left join ReportsView.dbo.ngXacns_Items tp 
			on it.ItemCode = tp.ItemCode
	where tp.ItemCode is null;

	commit transaction
	drop table if exists #excludes
	drop table if exists #set0_Items
	drop table if exists #items
END TRY
BEGIN CATCH
	if @@trancount > 0 rollback transaction
	declare @msg2 nvarchar(2048) = error_message()  
	raiserror (@msg2, 16, 1)
END CATCH
-----------------------------------------------]
------------------------------------------------]


--------------------------------------------------------------------
--------------------------------------------------------------------
/*
--Listing of all generated temp tables
drop table if exists #excludes
drop table if exists #set0_base
drop table if exists #set0
drop table if exists #set0_Items
drop table if exists #items


--Drop & Recreate table on Sage-----------------[
-----------------------------------------------[

drop table if exists ReportsView.dbo.ngXacns_Items 

create table ReportsView.dbo.ngXacns_Items
	(ItemCode varchar(20) not null
	,Item varchar(20) not null
	,RptIt varchar(20) not null
	,riVendorID varchar(10) not null
	,riSection varchar(10) not null
	,icCost money not null
	,riISBN varchar(13) null
	,riUPC varchar(20) null
	,LastUpdate datetime null
	,constraint PK_ngXacnsItems primary key(ItemCode)
	);
-----------------------------------------------]
------------------------------------------------]


*/

















GO


