
drop table if exists ReportsView.dbo.genXacns_Ages
create table [dbo].genXacns_Ages(
	[Loc] [char](5) NOT NULL,
	[Item] [varchar](20) NOT NULL,
	[ItemCode] [varchar](20) NOT NULL,
	[SkuExt] [varchar](10) NOT NULL,
	[u] [int] NOT NULL,
	[Xacn] [varchar](10) NOT NULL,
	[Date] [datetime] NOT NULL,
	[flow] [varchar](10) NOT NULL,
	[Qty] [int] NULL,
	[InvDays] [numeric](24, 12) NULL,
	[AjInvDays] [numeric](24, 12) NULL,
	[NAjInvDays] [numeric](24, 12) NULL,
	[FAjInvDays] [numeric](24, 12) NULL,
	[QtyDays] [int] NULL,
	[AjQtyDays] [int] NULL,
	[NAjQtyDays] [int] NULL,
	[FAjQtyDays] [int] NULL,
	[aInvAge] [numeric](24, 12) NULL,
	[mInvAge] [numeric](24, 12) NULL,
	[aAjInvAge] [numeric](24, 12) NULL,
	[mAjInvAge] [numeric](24, 12) NULL,
	[aNAjInvAge] [numeric](24, 12) NULL,
	[mNAjInvAge] [numeric](24, 12) NULL,
	[aFAjInvAge] [numeric](24, 12) NULL,
	[mFAjInvAge] [numeric](24, 12) NULL,
	[QtyAge] [numeric](24, 12) NULL,
	[AjQtyAge] [numeric](24, 12) NULL,
	[NAjQtyAge] [numeric](24, 12) NULL,
	[FAjQtyAge] [numeric](24, 12) NULL,
	constraint PK_genXacnsAges primary key(Loc,Item,ItemCode,u));





