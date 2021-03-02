

----sp version of file: hroXacns UPDATE daily job 191203.sql ***12/04/19

----hroXacns - Transaction-level data for New Goods--------------------------------------------
----ajorda------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------
--Towards determining metrics dependent on having unique identifiers for individual copies. 
----Metrics such as age at final disposition, days without inventory (Zero-Inventory-Days), 
----inventory levels, and value of inventory; all become possible and thereby comparable 
----to similar metics that are naturally calculated for SIPS/used goods.
----------------------------------------------------------------------------------------------
--CHANGE LOG----------------------------------------------------------------------------------
--12/04/19: sp version of file: hroXacns UPDATE daily job 191203.sql
--12/20/19: Changed date range criteria on store receiving data to use srd.ProcessDate instead 
----of srh.ProcessStartDate. Since ProcessStartDate is shipment-wide, using it as the criteria
----meant only the two first days of a shipment's store receiving records were ever captured, 
----since the current update structure for the hroXacns data is a rolling two-day window.
----Using ProcessDate (unique at the detail level) ensures later receiving is also captured.
----ProcessStartTime is still used as the timestamp for the actual ngxacn records. A manual 
----correction of the old data was also run.
--08/04/20: Store receiving corrections. Apparently the existing logic wass insufficient and 
----would miss updating still-unreceived details after some details were received. 
----This is now corrected as an additional update, 1a. 
----The important part was adding specific tests for when a tar field IS NULL, 
----because SET ANSI_NULLS ON means SQL won't affirm that the absence of a value is, at least,
----NOT a specific value. Technically, its response is "I can't answer that, Dave." 
--09/30/20: Added sh.ItemStatus = 1 to the Outbound Transfers query.
----Refactored the #wms stuff to use the wms_ils & wms_ar_ils databases. 
--1/5/21: Completed edit of the #wms stuff to use the wms_ils database instead of rILS_Data.	
-----------------------------------------------------------------------------------------------


-- SET XACT_ABORT, NOCOUNT ON;

declare @sDate datetime, @eDate datetime
/*--Run Weekly Sunday(incl) to Sunday(excl)
set @eDate = cast(dateadd(DD,1-datepart(DW,getdate()),getdate()) as date)
set @sDate = dateadd(DD,-7,@eDate)       */

/*--Run Daily (includes last two days because of 2-day update lag on ReportsData)
set @eDate = cast(getdate() as date)
set @sDate = dateadd(DD,-2,@eDate)*/

--Run since the last time it was run based on LastUpdate
set @eDate = cast(getdate() as date)
set @sDate = (select cast(dateadd(DD,-2,max(lastUpdate)) as date) from ReportsView..hroXacns)

/* -- See where the data is before using two days before max of LastUpdate
select cast(lastUpdate as date)[LastUpdate]
    ,max(case when Xacn not in ('CDC','Drps') then Date end)[LastDate]
	,cast(dateadd(DD,-2,max(lastUpdate)) as date)[New sDate]
    ,count(*)[NumRcrds]
from ReportsView..hroXacns 
group by cast(lastUpdate as date)
*/

--The #dates table is only used for running something piecemeal
drop table if exists #dates
select @sDate[sDate],@eDate[eDate]
into #dates


----List of valid locations (stores)------------------
------------------------------------------------------
drop table if exists #Stores
select distinct LocationNo[LocNo],LocationID[LocID] 
into #Stores
from ReportsData..Locations l with(nolock)
--Changed to not rely on edits every time a new store opens
where l.LocationNo < '00150' --non-outlets, non-accrual locs
	and l.[Status] = 'A' --active locations only
create index idx_Stores_LocID on #Stores(LocID)
create index idx_Stores_LocNo on #Stores(LocNo)


----List of NEW items (no records in hroXacns)---------
----for which minimum date is ignored-----------------
------------------------------------------------------
drop table if exists #NewItems
;with CurItems as(select ItemCode 
				  from ReportsView..hroXacns 
				  group by ItemCode)
select it.ItemCode
into #NewItems
from ReportsView..hroXacns_Items it 
	left join CurItems ci on it.ItemCode = ci.ItemCode
where  ci.ItemCode is null
create index idx_NewItems_ItemCode on #NewItems(ItemCode)


------------------------------------------------------------
--Stage inbound data (Shipments & Store Receiving) data-----
------------------------------------------------------------

----Shipment Data---------------------------------------
--------------------------------------------------------

--WMS Shipment data to distinguish push/pull------------
--  declare @eDate datetime =(select max(eDate) from #dates), @sDate datetime =(select max(sDate) from #dates);
drop table if exists #wms
create table #wms(TransferID varchar(10)
				 ,ItemCode varchar(20)
				 ,Loc varchar(5)
				 ,PshQty int
				 ,TotQty int)

;with unqOrdTypes(OrdTy) as(
	select Order_Type 
	from wms_ils..Shipment_Detail sd with(nolock)
	union select Order_Type
	from wms_ils..AR_Shipment_Detail sd with(nolock)
	union select Order_Type
	from wms_ar_ils..AR_Shipment_Detail sd with(nolock))
,OrdTypes as(
	select OrdTy
	,case when left(OrdTy,8) in ('HPB Asso','HPB Titl','TTB Init','TITLES') then 'Push'
		when left(OrdTy,8) in ('HPB REOR','TTB REOR') then 'Pull'
		when left(OrdTy,8) in ('TTB WHOL','WEBORDER') then 'Whsl'
		else 'wtf' end[OrdCat]
	from unqOrdTypes
	group by OrdTy)

insert into #wms(TransferID,ItemCode,Loc,PshQty,TotQty)
select right('0000000000'+ convert(varchar(10), wms.INTERNAL_SHIPMENT_NUM), 10) collate database_default [TransferID]
	,eq.ItemCode
	,right('00000'+ convert(varchar(5), wms.SHIP_TO), 5) collate database_default [Loc]
	,sum(isnull(case when ot.OrdCat = 'Push' then wms.Total_Qty end,0))[PshQty]
	,sum(wms.Total_Qty)[ShpQty]
from wms_ils..Shipment_Detail wms with(nolock)
	inner join wms_ils..Shipment_Header wmsh with(nolock)
		on wms.INTERNAL_SHIPMENT_NUM = wmsh.INTERNAL_SHIPMENT_NUM
	left join OrdTypes ot on wms.Order_Type = ot.OrdTy
	inner join ReportsView..hroXacns_Items eq 
		on right('00000000000000000000' + convert(varchar(20), wms.ITEM),20) collate database_default = eq.ItemCode
	left join #NewItems ni on eq.ItemCode = ni.ItemCode
where wmsh.Actual_Ship_Date_Time < @eDate 
	and (wmsh.Actual_Ship_Date_Time >= @sDate or ni.ItemCode is not null)
group by right('0000000000'+ convert(varchar(10), wms.INTERNAL_SHIPMENT_NUM), 10) collate database_default 
	,eq.ItemCode
	,right('00000'+ convert(varchar(5), wms.SHIP_TO), 5) collate database_default 

union 
select right('0000000000'+ convert(varchar(10), wms.INTERNAL_SHIPMENT_NUM), 10) collate database_default [TransferID]
	,eq.ItemCode
	,right('00000'+ convert(varchar(5), wms.SHIP_TO), 5) collate database_default [Loc]
	,sum(isnull(case when ot.OrdCat = 'Push' then wms.Total_Qty end,0))[PshQty]
	,sum(wms.Total_Qty)[ShpQty]
from wms_ils..AR_Shipment_Detail wms with(nolock)
	inner join wms_ils..AR_Shipment_Header wmsh with(nolock)
		on wms.INTERNAL_SHIPMENT_NUM = wmsh.INTERNAL_SHIPMENT_NUM
	left join OrdTypes ot on wms.Order_Type = ot.OrdTy
	inner join ReportsView..hroXacns_Items eq 
		on right('00000000000000000000' + convert(varchar(20), wms.ITEM),20) collate database_default = eq.itemCode
	left join #NewItems ni on eq.ItemCode = ni.ItemCode
where wmsh.Actual_Ship_Date_Time < @eDate 
	and (wmsh.Actual_Ship_Date_Time >= @sDate or ni.ItemCode is not null)
group by right('0000000000'+ convert(varchar(10), wms.INTERNAL_SHIPMENT_NUM), 10) collate database_default 
	,eq.ItemCode
	,right('00000'+ convert(varchar(5), wms.SHIP_TO), 5) collate database_default 

union 
select right('0000000000'+ convert(varchar(10), wms.INTERNAL_SHIPMENT_NUM), 10) collate database_default [TransferID]
	,eq.ItemCode
	,right('00000'+ convert(varchar(5), wms.SHIP_TO), 5) collate database_default [Loc]
	,sum(isnull(case when ot.OrdCat = 'Push' then wms.Total_Qty end,0))[PshQty]
	,sum(wms.Total_Qty)[ShpQty]
from wms_ar_ils..AR_Shipment_Detail wms with(nolock)
	inner join wms_ar_ils..AR_Shipment_Header wmsh with(nolock)
		on wms.INTERNAL_SHIPMENT_NUM = wmsh.INTERNAL_SHIPMENT_NUM
	left join OrdTypes ot on wms.Order_Type = ot.OrdTy
	inner join ReportsView..hroXacns_Items eq 
		on right('00000000000000000000' + convert(varchar(20), wms.ITEM),20) collate database_default = eq.itemCode
	left join #NewItems ni on eq.ItemCode = ni.ItemCode
where wmsh.Actual_Ship_Date_Time < @eDate 
	and (wmsh.Actual_Ship_Date_Time >= @sDate or ni.ItemCode is not null)
group by right('0000000000'+ convert(varchar(10), wms.INTERNAL_SHIPMENT_NUM), 10) collate database_default 
	,eq.ItemCode
	,right('00000'+ convert(varchar(5), wms.SHIP_TO), 5) collate database_default 


--Shipment Details----------------------------------
--  declare @eDate datetime =(select max(eDate) from #dates), @sDate datetime =(select max(sDate) from #dates);
drop table if exists #ships_det
SELECT sd.ItemCode
	,sh.ToLocationNo[Loc]
	,min(sh.DateReceived)[ShipDate]
	,case when sh.DropShipment = 1 and sh.DateReceived >= isnull(ds.minPon,sh.DateReceived) then right('0000000000'+cast(sd.PONumber as varchar(10)),10) else sd.TransferID end[ShipNo]
	,case when sh.DropShipment = 1 and sh.DateReceived >= isnull(ds.minPon,sh.DateReceived) then 'pon' else 'tid' end[isHist]
	,case when sh.FromLocationNo = '00944' and sh.DropShipment = 0 then cast('CDC' as varchar(10)) 
		  when sh.FromLocationNo = '00944' and sh.DropShipment = 1 then 'Drps' else cast('StSi' as varchar(10)) end[Xacn]
	,cast(case sh.Dropshipment when 1 then eq.riVendorID else sh.FromLocationNo end as varchar(10))[Src]
	--Occasionally there are two ShipDetail lines for an item on a shipment, so to avoid duplicating (or triplicating...&c.), wms.PshQty is shown as-is.
	--See 41 shipments for TTB items mostly of the form 16110% from 12/29/16, incl item 161104 to store 104 on ShipNo 0003115704.
	,wms.PshQty[PshQ]
	,sum(isnull(sd.Qty,0))[ShipQty]
into #ships_det   
from ReportsData..ShipmentDetail sd with(nolock)
    inner join ReportsData..ShipmentHeader sh with(nolock) on sh.TransferID = sd.TransferID 
	inner join #stores s on sh.ToLocationNo=s.LocNo
	left  join #wms wms with(nolock) on sd.TransferID = wms.TransferID and sd.ItemCode = wms.ItemCode and sh.ToLocationNo = wms.Loc
	left  join ReportsView..DrpShpSrCutoffDates_171219 ds on sh.ToLocationNo = ds.LocationNo
	inner join ReportsView..hroXacns_Items eq on sd.ItemCode = eq.itemCode
	left join #NewItems ni on eq.ItemCode = ni.ItemCode
where --UPC-related item code updates are "Shipped" systematically to the stores from the CDC as dropshipments
	--Actual UPCs are sometimes transfered between stores, but these are never dropshipments
	--Only count records for UPC codes if the don't come from the CDC:
	(sd.itemCode < '00000000000050000000' or sh.FromLocationNo <> '00944')
	--Only count records of nonzero qty's shipped: (this blocks the upc-alias item codes that may otherwise look like normal shipments)
	and sd.Qty > 0
	and sh.DateReceived < @eDate 
	and (sh.DateReceived >= @sDate or ni.ItemCode is not null)
group by sd.ItemCode,sh.ToLocationNo,wms.PshQty
	,case when sh.DropShipment = 1 and sh.DateReceived >= isnull(ds.minPon,sh.DateReceived) then right('0000000000'+cast(sd.PONumber as varchar(10)),10) else sd.TransferID end
	,case when sh.DropShipment = 1 and sh.DateReceived >= isnull(ds.minPon,sh.DateReceived) then 'pon' else 'tid' end
	,case when sh.FromLocationNo = '00944' and sh.DropShipment = 0 then cast('CDC' as varchar(10)) 
		  when sh.FromLocationNo = '00944' and sh.DropShipment = 1 then 'Drps' else cast('StSi' as varchar(10)) end
	,cast(case sh.Dropshipment when 1 then eq.riVendorID else sh.FromLocationNo end as varchar(10))

--Shipment Rollups------------------
drop table if exists #ships_ru
select sd.Loc
	,sd.isHist
	,sd.ShipNo
	,min(ShipDate)[ruShpDt]
	,sum(ShipQty)[sumShpQ]
into #ships_ru
from #ships_det sd
group by sd.Loc
	,sd.isHist
	,sd.ShipNo

----Store Receiving details-------------------
--  declare @eDate datetime =(select max(eDate) from #dates), @sDate datetime =(select max(sDate) from #dates);
drop table if exists #rcvds_det_OMFG
select srh.LocationNo[Loc]
	,srd.ItemCode
	,min(srh.ProcessStartTime)[RcvDate]
	,srh.ShipmentNo[ShipNo]
	--Don't need to check dates here since the PON convention for R's ONLY appears in the NON-historical tables
	,case when srh.ShipmentType = 'R' then 'pon' else 'tid' end[isHist]
	--10/1/19 change: Distinguish between DistCtr-sourced & Dropshipped for accurate estimate of transit time
	,cast(case srh.ShipmentType when 'W' then (case when srh.ProcessStartTime > '7/1/10' then 'CDC' else 'RDC' end) 
				when 'R' then 'Drps' else 'StSi' end as varchar(10))[Xacn]
	--10/4/19: Don't have Origin Location in SR, so no guesses as to whence stuff came.
	,cast(case srh.ShipmentType when 'R' then eq.riVendorID
			when 'W' then case when srh.ProcessStartTime >= '7/1/10' then 'CDC' else 'RDC' end
			else srh.ShipmentType end as varchar(10))[Src]
	,isnull(sum(srd.Qty),0)[RcvQty]
into #rcvds_det_OMFG    
from ReportsData..SR_Header srh with(nolock)
	inner join ReportsData..SR_Detail srd with(nolock) on srh.BatchID = srd.BatchID
	inner join #stores s on srh.LocationNo=s.LocNo
	inner join ReportsView..hroXacns_Items eq on srd.ItemCode = eq.itemCode
	left join #NewItems ni on eq.ItemCode = ni.ItemCode
where srd.ItemCode < '00000000000050000000'  --exclude UPC codes shipped for system use
	and srd.ProcessDate < @eDate 
	and (srd.ProcessDate >= @sDate or ni.ItemCode is not null)
group by srd.ItemCode,srh.LocationNo,srh.ShipmentNo
	,case when srh.ShipmentType = 'R' then 'pon' else 'tid' end
	,cast(case srh.ShipmentType when 'W' then (case when srh.ProcessStartTime > '7/1/10' then 'CDC' else 'RDC' end) 
				when 'R' then 'Drps' else 'StSi' end as varchar(10))
	,cast(case srh.ShipmentType when 'R' then eq.riVendorID
			when 'W' then case when srh.ProcessStartTime >= '7/1/10' then 'CDC' else 'RDC' end
			else srh.ShipmentType end as varchar(10))

--Older Store Receiving details-------------------------------
--  declare @eDate datetime =(select max(eDate) from #dates), @sDate datetime =(select max(sDate) from #dates);
insert into #rcvds_det_OMFG
select srh.LocationNo[Loc]
	,srd.ItemCode
	,min(srh.ProcessStartTime)[RcvDate]
	,srh.ShipmentNo[ShipNo]
	--Don't need to check dates here since the TID convention for R's ONLY appears in the historical tables
	,'tid'[isHist]
	--10/1/19: Distinguish between DistCtr-sourced & Dropshipped for accurate estimate of transit time
	,cast(case srh.ShipmentType when 'W' then (case when srh.ProcessStartTime > '7/1/10' then 'CDC' else 'RDC' end) 
				when 'R' then 'Drps' else 'StSi' end as varchar(10))[Xacn]
	--10/4/19: Don't have Origin Location in SR, so no guesses as to whence stuff came.
	,cast(case srh.ShipmentType when 'R' then eq.riVendorID
			when 'W' then case when srh.ProcessStartTime >= '7/1/10' then 'CDC' else 'RDC' end
			else srh.ShipmentType end as varchar(10))[Src]
	,isnull(sum(srd.Qty),0)[RcvQty]
from ReportsData..SR_Header_Historical srh with(nolock)
	inner join ReportsData..SR_Detail_Historical srd with(nolock) on srh.BatchID = srd.BatchID 
	inner join #stores s on srh.LocationNo=s.LocNo
	inner join ReportsView..hroXacns_Items eq on srd.ItemCode = eq.itemCode
	left join #NewItems ni on eq.ItemCode = ni.ItemCode
where srd.itemCode < '00000000000050000000'  --exclude UPC codes shipped for system use
	and srd.ProcessDate < @eDate 
	and (srd.ProcessDate >= @sDate or ni.ItemCode is not null)
group by srd.ItemCode,srh.LocationNo,srh.ShipmentNo
	,cast(case srh.ShipmentType when 'W' then (case when srh.ProcessStartTime > '7/1/10' then 'CDC' else 'RDC' end) 
				when 'R' then 'Drps' else 'StSi' end as varchar(10))
	,cast(case srh.ShipmentType when 'R' then eq.riVendorID
			when 'W' then case when srh.ProcessStartTime >= '7/1/10' then 'CDC' else 'RDC' end
			else srh.ShipmentType end as varchar(10))

--Combine Store Receiving details--------------
drop table if exists #rcvds_det
select rd.Loc,rd.ItemCode
	,min(rd.RcvDate)[RcvDate]
	,rd.ShipNo,rd.isHist
	,rd.Xacn,rd.Src
	,sum(rd.RcvQty)[RcvQty]
into #rcvds_det
from #rcvds_det_OMFG rd
group by rd.ItemCode,rd.Loc,rd.ShipNo,rd.Xacn,rd.isHist,rd.Src

--Housekeeping-------
drop table if exists #rcvds_det_OMFG

--Store Receiving Rollups-------------
drop table if exists #rcvds_ru
select rd.Loc
	,rd.isHist,rd.ShipNo
	--<<--Need RcvDate to fill in Date in case a specific line isn't received.
	,min(rd.RcvDate)[ruRcvDt]
	,sum(rd.RcvQty)[sumRcvQ]
into #rcvds_ru  
from #rcvds_det rd 
group by rd.Loc
	,rd.ShipNo
	,rd.isHist

----Merging Shipment & Receiving data-------
--------------------------------------------
drop table if exists #ShpRcv_prep1
select coalesce(sd.Loc,rd.Loc)[Loc]
	,coalesce(sd.ItemCode,rd.ItemCode)[ItemCode]
	,coalesce(sd.isHist,rd.isHist)[isHist]
	,coalesce(sd.ShipNo,rd.ShipNo)[ShipNo]
	,coalesce(sd.Xacn,rd.Xacn)[Xacn]
	,coalesce(sd.Src,rd.Src)[Src]
	,sd.ShipDate
	,rd.RcvDate
	,sd.ShipQty
	,coalesce(sd.PshQ,0)[PshQty]
	,rd.RcvQty
into #ShpRcv_prep1
from #ships_det sd full outer join #rcvds_det rd
	on sd.ItemCode = rd.ItemCode and sd.Loc = rd.Loc 
	and sd.isHist = rd.isHist and sd.ShipNo = rd.ShipNo

--Housekeeping-------
drop table if exists #ships_det
drop table if exists #rcvds_det
drop table if exists #wms

--First staging table-----------------------------------------------------
--------------------------------------------------------------------------
--Add in rollup data to fill in Date if a specific detail isn't received.
drop table if exists #ShpRcv
select pr.Loc,pr.ItemCode
	,pr.isHist+pr.ShipNo[Shpmt]
	,pr.Xacn,pr.ShipDate
	,pr.ShipQty,pr.PshQty
	,sru.ruShpDt,sru.sumShpQ
	,pr.RcvDate,pr.RcvQty
	,rru.ruRcvDt,rru.sumRcvQ
	,pr.Src
	,dateadd(hh,tt.mdnTransitHrs,isnull(pr.ShipDate,sru.ruShpDt))[ShpTnsDt]
	--Searching for non-unique Loc-Item-Shpmt tuples...
	,count(*) over(partition by pr.Loc,pr.ItemCode,pr.isHist,pr.ShipNo)[Dups]
into #ShpRcv
from #ShpRcv_prep1 pr
	left join #ships_ru sru 
		on pr.Loc = sru.Loc and pr.ShipNo = sru.ShipNo and pr.isHist = sru.isHist
	left join  #rcvds_ru rru
		on pr.Loc = rru.Loc and pr.ShipNo = rru.ShipNo and pr.isHist = rru.isHist
	left join ReportsView..Xacns_TransitTimes tt  with(nolock) on tt.Loc = pr.Loc 
		and tt.Xacn = pr.Xacn and (pr.Src = tt.Vndr or pr.Xacn = 'CDC')
		and coalesce(pr.ShipDate,sru.ruShpDt,pr.RcvDate,rru.ruRcvDt) > '6/1/2010'

--Housekeeping-------
drop table if exists #ShpRcv_prep1
drop table if exists #ships_ru
drop table if exists #rcvds_ru


------------------------------------------------------------
--Pulling outbound & set (Sales/Xfers/Inv Counts) data------
------------------------------------------------------------
drop table if exists #xacns
create table #xacns
	(Loc char(5) not null
	,ItemCode varchar(20) not null
	,SkuExt varchar(10) not null
	,Shpmt varchar(13)
	,Date datetime not null
	,Xacn varchar(10) not null
	,flow varchar(3) not null
	,Qty int
	,Inv int
	,pshQty int
	,mdQty int
	,SldVal money
	,mdSldVal money
	,SldFee money
	);

	
--Brick & Mortar Sales--------------------------------------
------------------------------------------------------------
--  declare @eDate datetime =(select max(eDate) from #dates), @sDate datetime =(select max(sDate) from #dates);
insert into #xacns 
--Item Sales...
select loc.LocNo
	,sh.ItemCode
	,isnull(nullif(sh.SkuExtension,' '),0)[SkuExt]
	,NULL[Shpmt]
	,hh.EndDate
	,(case when sh.IsReturn = 'Y' then 'Rtrn' else 'Sale' end)
	,(case when sh.IsReturn = 'Y' then 'in' else 'out' end)
	,(case when sh.IsReturn = 'Y' then sh.Quantity else -1*sh.Quantity end)
	,NULL[Inv]
	,NULL[PshQty]
	,(case when sh.IsReturn = 'Y' then 1 else -1 end)*(case when sh.RegisterPrice < sh.UnitPrice then sh.quantity else 0 end)
	,sh.ExtendedAmt
	,(case when sh.RegisterPrice < sh.UnitPrice then sh.ExtendedAmt else 0 end)
	,NULL[SldFee]
from rHPB_Historical.dbo.SalesItemHistory sh with(nolock)
	inner join rHPB_Historical.dbo.SalesHeaderHistory hh with(nolock)
		on sh.LocationID = hh.LocationID and sh.XactionType = hh.XactionType
			and sh.SalesXactionId = hh.SalesXactionID and sh.BusinessDate = hh.BusinessDate 
	inner join #Stores loc on sh.LocationID = loc.LocID
	inner join ReportsView..hroXacns_Items it on sh.ItemCode = it.ItemCode
	left join #NewItems ni on it.ItemCode = ni.ItemCode
where sh.Status = 'A'
	and sh.BusinessDate < @eDate 
	and (sh.BusinessDate >= @sDate or ni.ItemCode is not null)
		

--HPB.com/iStore Online Sales-----------------------------------
----------------------------------------------------------------
--  declare @eDate datetime =(select max(eDate) from #dates), @sDate datetime =(select max(sDate) from #dates);
drop table if exists #online
--iStore Sales------------------------
select isnull(od.LocationNo,fa.HPBLocationNo)[Loc]
	,it.ItemCode
	,isnull(nullif(od.SKUExtension,' '),0)[SkuExt]
	,om.OrderDate[Date]
	,'iSale'[xacn]
	,'out'[flow]
	,-1 * om.ShippedQuantity[Qty]
	,-1*case when om.Price < pm.Price then om.ShippedQuantity else 0 end[mdQty]
	,om.Price[SldVal]
	,case when om.Price < pm.Price then om.Price else 0 end[mdSldVal]
	,om.ShippingFee[SldFee]
into #online
--    select top 1000*
from isis..Order_Monsoon om with(nolock)
	inner join isis..App_Facilities fa with(nolock) on om.FacilityID = fa.FacilityID
	--pre-2014ish, SAS & XFRs would show up in Monsoon, so specifying 'MON' excludes those
	left join ofs..Order_Header oh with(nolock) on om.ISIS_OrderID = oh.ISISOrderID and oh.OrderSystem = 'MON' 
	--Grabs fulfilment location where available, otherwise uses originating location
	left join ofs..Order_Detail od with(nolock) on oh.OrderID = od.OrderID and od.Status in (1,4)
		--Problem orders have ProblemStatusID not null
		and (od.ProblemStatusID is null or od.ProblemStatusID = 0)	
	inner join #Stores loc on isnull(od.LocationNo,fa.HPBLocationNo) = loc.LocNo
	inner join ReportsView..hroXacns_Items it on right(om.SKU,20) = it.ItemCode
	inner join ReportsData..ProductMaster pm with(nolock) on it.ItemCode = pm.ItemCode
	left join #NewItems ni on it.ItemCode = ni.ItemCode
where om.ShippedQuantity > 0
	and om.OrderStatus in ('New','Pending','Shipped')
	and om.OrderDate < @eDate 
	and (om.OrderDate >= @sDate or ni.ItemCode is not null)
	and left(om.SKU,1) = 'D'
	
--iStore Refunds---------------------
insert into #online
select isnull(od.LocationNo,fa.HPBLocationNo)[Loc]
	,it.ItemCode
	,isnull(nullif(od.SKUExtension,' '),0)[SkuExt]
	,om.RefundDate[Date]
	,'iRtrn'[xacn]
	,'in'[flow]
	--Think refunded product doesn't go back to the store per se...
	,0[Qty] --,om.ShippedQuantity[Qty]
	,0[mdQty] --,case when om.Price < pm.Price then om.ShippedQuantity else 0 end[mdQty]
	--Backs out SldVal FIRST
	,-1.0 * (case when om.RefundAmount >= om.Price then om.Price else om.RefundAmount end)[SldVal]
	,-1.0 * case when om.Price < pm.Price then
				(case when om.RefundAmount >= om.Price then om.Price else om.RefundAmount end) else 0 end[mdSldVal]
	,-1.0 * case when om.RefundAmount > om.Price then om.RefundAmount - om.Price else 0 end[SldFee]
--    select top 1000*
from isis..Order_Monsoon om with(nolock)
	inner join isis..App_Facilities fa with(nolock) on om.FacilityID = fa.FacilityID
	--pre-2014ish, SAS & XFRs would show up in Monsoon, so this excludes those
	left join ofs..Order_Header oh with(nolock) on om.ISIS_OrderID = oh.ISISOrderID and oh.OrderSystem = 'MON' 
	--Grabs fulfilment location where available, otherwise uses originating location
	left join ofs..Order_Detail od with(nolock) on oh.OrderID = od.OrderID and od.Status in (1,4)
		--Problem orders have ProblemStatusID not null
		and (od.ProblemStatusID is null or od.ProblemStatusID = 0)	
	inner join #Stores loc on isnull(od.LocationNo,fa.HPBLocationNo) = loc.LocNo
	inner join ReportsView..hroXacns_Items it on right(om.SKU,20) = it.ItemCode
	inner join ReportsData..ProductMaster pm with(nolock) on it.ItemCode = pm.ItemCode
	left join #NewItems ni on it.ItemCode = ni.ItemCode
where om.OrderStatus in ('New','Pending','Shipped')
	and om.RefundAmount > 0
	and om.RefundDate < @eDate 
	and (om.RefundDate >= @sDate or ni.ItemCode is not null)
	and left(om.SKU,1) = 'D'

--HPB.com Sales----------------------
insert into #online
select fa.HPBLocationNo[Loc]
	,it.ItemCode
	--Don't know if/where SkuExt is logged for OMNI stuff
	,0[SkuExt]
	,od.OrderDate[Date]
	,'hSale'[xacn]
	,'out'[flow]
	,-1 * od.Quantity[Qty]
	,-1 * case when od.ItemPrice < pm.Price then od.Quantity else 0 end[mdQty]
	,od.ExtendedAmount[SoldVal]
	,case when od.ItemPrice < pm.Price then od.ExtendedAmount else 0 end[mdSldVal]
	,od.ShippingAmount[SldFee]
--    select top 1000*
from isis..Order_Omni od with(nolock)
	inner join isis..App_Facilities fa with(nolock) on od.FacilityID = fa.FacilityID
	inner join #Stores loc on fa.HPBLocationNo = loc.LocNo
	inner join ReportsView..hroXacns_Items it on right(od.SKU,20) = it.ItemCode
	inner join ReportsData..ProductMaster pm with(nolock) on it.ItemCode = pm.ItemCode
	left join #NewItems ni on it.ItemCode = ni.ItemCode
where od.OrderStatus not in ('canceled')
	and od.ItemStatus not in ('canceled')
	and od.OrderDate < @eDate 
	and (od.OrderDate >= @sDate or ni.ItemCode is not null)
	and left(od.SKU,1) = 'D'
	and od.Quantity > 0

--HPB.com Refunds--------------------
insert into #online
select fa.HPBLocationNo[Loc]
	,it.ItemCode
	,0[SkuExt]
	,od.SiteLastModifiedDate[Date]
	,'hRtrn'[xacn]
	,'in'[flow]
	--Same idea... don't think we actually get the thing back to the store.
	,0[Qty] --,-1 * od.Quantity[Qty]
	,0[mdQty] --,case when od.ItemPrice < pm.Price then od.Quantity else 0 end[mdQty]
	--SldVal is backed out FIRST...
	,-1.0 * (case when od.ItemRefundAmount >= od.ItemPrice then od.ItemPrice else od.ItemRefundAmount end)[SldVal]
	,-1.0 * (case when od.ItemPrice < pm.Price then 
				(case when od.ItemRefundAmount >= od.ItemPrice then od.ItemPrice else od.ItemRefundAmount end) 
				else 0 end)[mdSldVal]
	,-1.0 * (case when od.ItemRefundAmount >= od.ItemPrice then od.ItemRefundAmount - od.ItemPrice else 0 end)[SldFee]
--    select top 1000*
from isis..Order_Omni od with(nolock)
	inner join isis..App_Facilities fa with(nolock) on od.FacilityID = fa.FacilityID
	inner join #Stores loc on fa.HPBLocationNo = loc.LocNo
	inner join ReportsView..hroXacns_Items it on right(od.SKU,20) = it.ItemCode
	inner join ReportsData..ProductMaster pm with(nolock) on it.ItemCode = pm.ItemCode
	left join #NewItems ni on it.ItemCode = ni.ItemCode
where od.ItemStatus not in ('canceled')
	and od.OrderStatus not in ('canceled')
	and od.SiteLastModifiedDate < @eDate 
	and (od.SiteLastModifiedDate >= @sDate or ni.ItemCode is not null)								  
	and left(od.SKU,1) = 'D'
	and od.Quantity > 0
	and od.ItemRefundAmount > 0

--Combine all #online data into #online-------------------
--Right now SldFee doesn't get rolled into SldVal-------
insert into #xacns
select op.Loc
	,op.ItemCode
	,op.SkuExt
	,NULL[Shpmt]
	,op.Date
	,op.xacn
	,op.flow
	,op.Qty
	,NULL[Inv]
	,NULL[PshQty]
	,op.mdQty
	,op.SldVal
	,op.mdSldVal
	,op.SldFee
from #online op


--Transfers OUTBOUND----------------------------------------
------------------------------------------------------------
--  declare @eDate datetime =(select max(eDate) from #dates), @sDate datetime =(select max(sDate) from #dates)
insert into #xacns
--Item FROM a location...
select sh.FromLocationNo
	,sh.DipsItemCode
	,0[SkuExt]
	,'na'[Shpmt]
	,sh.LastUpdateTime
	,(case sh.TransferType 
		when 1 then 'Tsh' 
		when 3 then 'StSo' 
		when 6 then 'vRtn'
		when 2 then 'Dnt'
		when 4 then 'Bsm'
		when 7 then 'Dmg' 
		else 'Mkt' end)
	,'out'
	,-1*sh.Quantity
	,NULL[Inv]
	,NULL[PshQty]
	,NULL[mdQty]
	,NULL[SldVal]
	,NULL[mdSldVal]
	,NULL[SldFee]
from ReportsView..vw_StoreTransferDetail sh with(nolock)
	inner join #Stores loc on sh.FromLocationNo = loc.LocNo
	inner join ReportsView..hroXacns_Items it on sh.DIPSItemCode=it.ItemCode
	left join #NewItems ni on it.ItemCode = ni.ItemCode
where sh.LastUpdateTime < @eDate 
	and (sh.LastUpdateTime >= @sDate or ni.ItemCode is not null)
	and sh.StatusCode = 3
	and sh.TransferType in (1,3,5,6,2,4,7)
	and sh.ItemStatus = 1

	
--SICC-sourced Inventory Adjustments------------------------
------------------------------------------------------------
--  declare @eDate datetime =(select max(eDate) from #dates), @sDate datetime =(select max(sDate) from #dates)
insert into #xacns
--Via rf guns in the stores...
select sia.LocationNo
	,sia.ItemCode
	,0[SkuExt]
	,'na'[Shpmt]
	,sia.TransDate
	,'SICC'
	,'set'
	,NULL[Qty]
	,sia.NewQty
	,NULL[PshQty]
	,NULL[mdQty]
	,NULL[SldVal]
	,NULL[mdSldVal]
	,NULL[SldFee]
FROM ReportsData..SICC_Inventory_Adjustments sia with(nolock)
	inner join #Stores loc on sia.LocationNo = loc.LocNo
	inner join ReportsView..hroXacns_Items it on sia.ItemCode=it.ItemCode
	left join #NewItems ni on it.ItemCode = ni.ItemCode
where sia.TransDate < @eDate 
	and (sia.TransDate >= @sDate or ni.ItemCode is not null)
	and sia.NewQty is not null
	
insert into #xacns
--Via the SICC program by CDC folks...
select cia.LocationNo
	,cia.ItemCode
	,0[SkuExt]
	,'na'[Shpmt]
	,cia.TransDate
	,'CICC'
	,'set'
	,NULL[Qty]
	,cia.NewQty
	,NULL[PshQty]
	,NULL[mdQty]
	,NULL[SldVal]
	,NULL[mdSldVal]
	,NULL[SldFee]
FROM ReportsData..CDC_Inventory_Adjustments cia with(nolock)
	inner join #Stores loc on cia.LocationNo = loc.LocNo
	inner join ReportsView..hroXacns_Items it on cia.ItemCode=it.ItemCode
	left join #NewItems ni on it.ItemCode = ni.ItemCode
where cia.TransDate < @eDate 
	and (cia.TransDate >= @sDate or ni.ItemCode is not null)
	and cia.NewQty is not null

	
--------------------------------------------------------------------
----Creating the second staging table of transaction records--------
--------------------------------------------------------------------
drop table if exists #xacns_final
select xa.Loc
	,xa.ItemCode,xa.SkuExt
	,isnull(xa.Shpmt,'na')[Shpmt]
	,xa.Date,xa.Xacn
	,xa.flow
	,sum(xa.Qty)[Qty]
	,sum(xa.Inv)[Inv]
	,sum(xa.pshQty)[pshQty]
	,sum(xa.mdQty)[mdQty]
	,sum(xa.SldVal)[SldVal]
	,sum(xa.mdSldVal)[mdSldVal]
	,sum(xa.SldFee)[SldFee]
	,count(*)[n]
	,it.Item
into #xacns_final 
from #xacns xa inner join ReportsView..hroXacns_Items it 
	on xa.ItemCode = it.ItemCode
group by xa.Loc,xa.ItemCode,xa.Shpmt
	,xa.Date,xa.flow,xa.Xacn,xa.SkuExt,it.Item

--Housekeeping------
drop table if exists #online
drop table if exists #xacns


--------------------------------------------------------------------
----Updating & Inserting on hroXacns_ShRvDetail & hroXacns------------
--------------------------------------------------------------------

----Transaction 1--------------------------------
--Update Receiving data hroXacns_ShRvDetail------[
-----------------------------------------------[
BEGIN TRY
	declare @uDate datetime = getdate()
	begin transaction	

	--Have Rcv data where we didn't before
	update ReportsView..hroXacns_ShRvDetail
	set RcvDate = src.RcvDate
		,RcvQty = src.RcvQty
		,ruRcvDt = src.ruRcvDt
		,sumRcvQ = src.sumRcvQ
		,LastUpdate = @uDate
	from #ShpRcv src 
		inner join ReportsView..hroXacns_ShRvDetail tar
		on tar.ItemCode = src.ItemCode 
			and tar.Loc = src.Loc 
			and tar.Shpmt = src.Shpmt
	where tar.RcvDate is null
		and (src.RcvDate is not null or src.ruRcvDt is not null);

	commit transaction;
END TRY
BEGIN CATCH
    if @@trancount > 0 rollback transaction
    declare @msg1 nvarchar(2048) = error_message()  
    raiserror (@msg1, 16, 1)
END CATCH
-----------------------------------------------]
------------------------------------------------]


----Transaction 1a-------------------------------------------
--Update Shipment-level Receiving data ngXacns_ShRvDetail---[
-----------------------------------------------------------[
BEGIN TRY
	begin transaction	

	--Update the rollup rcv data over entire shipment
	;with ShpmtTots as(
		select sr.Shpmt, sr.Loc
			,sum(sr.RcvQty)[sumRcvQ]
			,max(sr.ruRcvDt)[ruRcvDt]
		from ReportsView..hroXacns_ShRvDetail sr with(nolock)
		group by Shpmt, sr.Loc)
	update ReportsView..hroXacns_ShRvDetail
	set ruRcvDt = src.ruRcvDt
		,sumRcvQ = src.sumRcvQ
		,LastUpdate = @uDate
	from ShpmtTots src 
		inner join ReportsView..hroXacns_ShRvDetail tar with(nolock)
		on tar.Shpmt = src.Shpmt 
			and tar.Loc = src.Loc
	where tar.ruRcvDt <> src.ruRcvDt or tar.sumRcvQ <> src.sumRcvQ
		or tar.ruRcvDt is null or tar.sumRcvQ is null;

	commit transaction;
END TRY
BEGIN CATCH
    if @@trancount > 0 rollback transaction
    declare @msg1a nvarchar(2048) = error_message()  
    raiserror (@msg1a, 16, 1)
END CATCH
-----------------------------------------------]
------------------------------------------------]




----Transaction 2--------------------------------
--Update Date/Qty/PshQty hroXacns_ShRvDetail-----[
-----------------------------------------------[
BEGIN TRY
	begin transaction
	
	--Refresh Calc fields on records with new Rcv data...
	update ReportsView..hroXacns_ShRvDetail
	set Date = coalesce(RcvDate,ruRcvDt,ShpTnsDt,ShipDate)
		,Qty = case when isnull(sumRcvQ,0) > 0 and RcvQty is null then 0
					when sumRcvQ is null and RcvQty is null then ShipQty
					else coalesce(RcvQty,ShipQty,0) end
		,PshQty = case when isnull(sumRcvQ,0) > 0 and RcvQty is null then 0
					   when sumRcvQ is null and RcvQty is null then PshQty
					   else case when RcvQty < PshQty then RcvQty else PshQty end end
		,LastUpdate = getDate()
	where LastUpdate = @uDate;
	--More robust method? Doesn't rely on ephemeral timestamps...
	--where Date <> coalesce(RcvDate,ruRcvDt,ShpTnsDt,ShipDate)
	--	or Qty <> case when isnull(sumRcvQ,0) > 0 and RcvQty is null then 0
	--				when sumRcvQ is null and RcvQty is null then ShipQty
	--				else coalesce(RcvQty,ShipQty,0) end
	--	or PshQty = case when isnull(sumRcvQ,0) > 0 and RcvQty is null then 0
	--				   when sumRcvQ is null and RcvQty is null then PshQty
	--				   else case when RcvQty < PshQty then RcvQty else PshQty end end;
	
	commit transaction;
END TRY
BEGIN CATCH
    if @@trancount > 0 rollback transaction
    declare @msg2 nvarchar(2048) = error_message()  
    raiserror (@msg2, 16, 1)
END CATCH
-----------------------------------------------]
------------------------------------------------]



----Transaction 3--------------------------------
--Insert NEW records hroXacns_ShRvDetail---------[
-----------------------------------------------[
BEGIN TRY
	begin transaction
	set @uDate = getdate()

	--Add new records (at least ship data, if not also rcv data)
	insert into ReportsView..hroXacns_ShRvDetail
	select src.Loc,src.ItemCode
		,src.Shpmt,src.Xacn
	----CALCULATED/BUSINESS LOGIC FIELDS------------------------------------------------------
		,coalesce(src.RcvDate,src.ruRcvDt,src.ShpTnsDt,src.ShipDate)[Date]
			--Unfulfilled: If sumRcvQ > 0 and RcvQty is null then ShRvQty = 0 
		,case when isnull(src.sumRcvQ,0) > 0 and src.RcvQty is null then 0
			--Fulfilled-Not-Received: If sumRcvQ is null and RcvQ is null then ShRvQty = ShipQty
			when src.sumRcvQ is null and src.RcvQty is null then isnull(src.ShipQty,0)
			else coalesce(src.RcvQty,src.ShipQty,0) end[Qty]
		--Same logic applies to PshQty
		,case when isnull(src.sumRcvQ,0) > 0 and src.RcvQty is null then 0
			when src.sumRcvQ is null and src.RcvQty is null then isnull(src.PshQty,0)
			--If less qty was received than was shipped, allocate at least the RcvQty to PshQty
			else isnull(case when src.RcvQty < src.PshQty then src.RcvQty else src.PshQty end,0) end[PshQty]
	-----------------------------------------------------------------------------------------------
		,src.ShipDate,src.ShipQty
		,src.ruShpDt,src.sumShpQ
		,src.RcvDate,src.RcvQty
		,src.ruRcvDt,src.sumRcvQ
		,src.Src,src.ShpTnsDt
		,@uDate
	from #ShpRcv src 
		left join ReportsView..hroXacns_ShRvDetail tar 
		on tar.ItemCode = src.ItemCode 
			and tar.Loc = src.Loc 
			and tar.Shpmt = src.Shpmt
	where tar.ItemCode is null;
	
	commit transaction;
END TRY
BEGIN CATCH
    if @@trancount > 0 rollback transaction
    declare @msg3 nvarchar(2048) = error_message()  
    raiserror (@msg3, 16, 1)
END CATCH
-----------------------------------------------]
------------------------------------------------]



----Transaction 4--------------------------------
--Update calc fields in hroXacns-----------------[
-----------------------------------------------[
--   declare @uDate datetime
BEGIN TRY
	begin transaction
	set @uDate = getdate()
	
	--Update calculated fields
	update ReportsView..hroXacns
	set Date = src.Date
		,Qty = src.Qty
		,PshQty = src.PshQty
		,LastUpdate = @uDate
	from ReportsView..hroXacns tar
		inner join ReportsView..hroXacns_ShRvDetail src
		on tar.ItemCode = src.ItemCode 
		and tar.Loc = src.Loc 
		and tar.Shpmt = src.Shpmt
	where tar.Date <> src.Date
		or tar.Qty <> src.Qty
		or tar.PshQty <> src.PshQty;

	commit transaction;
END TRY
BEGIN CATCH
    if @@trancount > 0 rollback transaction
    declare @msg4 nvarchar(2048) = error_message()  
    raiserror (@msg4, 16, 1)
END CATCH
-----------------------------------------------]
------------------------------------------------]



----Transaction 5--------------------------------
--Add new CDC/Drps/StSi records to hroXacns------[
-----------------------------------------------[
--   declare @uDate datetime
BEGIN TRY
	begin transaction
	set @uDate = getdate()
	
	insert into ReportsView..hroXacns
	select src.Loc,src.ItemCode
		,0[SkuExt]
		,src.Shpmt,src.Date
		,src.Xacn,'in'
		,src.Qty
		,null[Inv]
		,src.PshQty
		,null[mdQty]
		,null[SldVal]
		,null[mdSldVal]
		,null[SldFee]
		,1[n]
		,@uDate
		,it.Item
		,null[u]
		,null[InvQ]
		,null[crSet]
		,null[nxIn]
	from ReportsView..hroXacns_ShRvDetail src
		inner join ReportsView..hroXacns_Items it
		on src.ItemCode = it.ItemCode
		left join ReportsView..hroXacns tar 
		on tar.ItemCode = src.ItemCode 
			and tar.Loc = src.Loc 
			and tar.Shpmt = src.Shpmt
	where tar.ItemCode is null;
	
	commit transaction;
END TRY
BEGIN CATCH
    if @@trancount > 0 rollback transaction
    declare @msg5 nvarchar(2048) = error_message()  
    raiserror (@msg5, 16, 1)
END CATCH
-----------------------------------------------]
------------------------------------------------]



----Transaction 6--------------------------------
--Insert new Sale/Rtrn/Xfers/SICC records-------[
--into hroXacns---------------------------------[
--   declare @uDate datetime
SET XACT_ABORT, NOCOUNT ON;
BEGIN TRY
	begin transaction
	set @uDate = getdate()

	--insert into #hroXacns
	insert into ReportsView..hroXacns
	select src.Loc
		,src.ItemCode
		,src.SkuExt
		,src.Shpmt
		,src.Date
		,src.Xacn
		,src.flow
		,src.Qty
		,src.Inv
		,src.PshQty
		,src.mdQty
		,src.SldVal
		,src.mdSldVal
		,src.SldFee
		,src.n
		,@uDate
		,src.Item
		,null[u]
		,null[InvQ]
		,null[crSet]
		,null[nxIn]
	--Still join on tar to ensure no duplicates are added.
	--Also because sometimes late-night xacns fail to replicate
	--to Sage so @sDate has to overlap the previous update cycle.
	--Also because PK would throw an error otherwise...
	from #xacns_final src left join ReportsView..hroXacns tar
		on src.Loc = tar.Loc and src.ItemCode = tar.ItemCode 
		and src.Shpmt = tar.Shpmt and src.Date = tar.Date
		and src.flow = tar.flow and src.Xacn = tar.Xacn
		and src.SkuExt = tar.SkuExt
	where tar.Loc is null
	
	commit transaction;
END TRY
BEGIN CATCH
    if @@trancount > 0 rollback transaction
    declare @msg6 nvarchar(2048) = error_message()  
    raiserror (@msg6, 16, 1)
END CATCH
-----------------------------------------------]
------------------------------------------------]



----Transaction 7--------------------------------
--Update/Refresh u & InvQ fields in hroXacns-----[
-----------------------------------------------[
BEGIN TRY
	begin transaction

	;with uInvQ_refresh as(
		select Loc,ItemCode,SkuExt,Date,Shpmt,Xacn
			,row_number() over(partition by Loc,Item order by Date,ItemCode desc,SkuExt,Shpmt,Xacn)[u]
			,sum(Qty) over(partition by Loc,Item order by Date,ItemCode desc,SkuExt,Shpmt,Xacn rows unbounded preceding)[InvQ]
		from ReportsView..hroXacns)
	update ReportsView..hroXacns 
		set u = src.u
			,InvQ = src.InvQ
		from ReportsView..hroXacns tar inner join uInvQ_refresh src 
			on tar.Loc = src.Loc and tar.ItemCode = src.ItemCode 
			and tar.SkuExt = src.SkuExt and tar.Shpmt = src.Shpmt 
			and tar.Date = src.Date and tar.Xacn = src.Xacn
		where tar.u  <> src.u
			or tar.InvQ <> src.InvQ
			or tar.u is null
			or tar.InvQ is null;
	
	commit transaction;
END TRY
BEGIN CATCH
    if @@trancount > 0 rollback transaction
    declare @msg7 nvarchar(2048) = error_message()  
    raiserror (@msg7, 16, 1)
END CATCH
-----------------------------------------------]
------------------------------------------------]



----Transaction 8--------------------------------
--Update/Recalc SetFlow markers in hroXacns------[
-----------------------------------------------[
BEGIN TRY
	begin transaction

	;with SetUs_refresh as(
		select Loc,ItemCode,u
			,isnull(max(case when flow = 'set' then u end) over(partition by Loc,Item order by u rows between unbounded preceding and current row),1)[crSet]
			,coalesce(min(case when flow = 'in' then u end) over(partition by Loc,Item order by u rows between 1 following and unbounded following)
					  ,min(case when flow = 'set' then u end) over(partition by Loc,Item order by u rows between 1 following and unbounded following)
					  ,count(*) over(partition by Loc,Item))[nxIn]
		from ReportsView..hroXacns)
	update ReportsView..hroXacns 
		set crSet = src.crSet
			,nxIn = src.nxIn
		from ReportsView..hroXacns tar 
			inner join SetUs_refresh src on tar.Loc = src.Loc 
			and tar.ItemCode = src.ItemCode and tar.u = src.u 
		--Edit WHERE to key off of earliest @uDate??
		where tar.crSet  <> src.crSet
			or tar.nxIn <> src.nxIn
			or tar.crSet is null
			or tar.nxIn is null;
	
	commit transaction;
END TRY
BEGIN CATCH
    if @@trancount > 0 rollback transaction
    declare @msg8 nvarchar(2048) = error_message()  
    raiserror (@msg8, 16, 1)
END CATCH
-----------------------------------------------]
------------------------------------------------]



--------------------------------------------------------------------
--Final catch-all Housekeeping--------------------------------------
--------------------------------------------------------------------

drop table if exists #stores
drop table if exists #NewItems
drop table if exists #wms
drop table if exists #ships_det
drop table if exists #ships_ru
drop table if exists #rcvds_det
drop table if exists #rcvds_ru
drop table if exists #ShpRcv_prep
drop table if exists #ShpRcv_prep1
drop table if exists #ShpRcv_prep2
drop table if exists #ShpRcv
drop table if exists #xacns
drop table if exists #online
drop table if exists #xacns_final




GO
