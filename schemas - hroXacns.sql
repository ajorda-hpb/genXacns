USE [ReportsView]
GO

/****** Object:  Table [dbo].[hroXacns_Items]    Script Date: 2/24/2021 5:43:51 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[hroXacns_Items](
	[ItemCode] [char](20) NOT NULL,
	[Item] [char](20) NULL,
	[RptIt] [varchar](20) NULL,
	[riVendorID] [varchar](10) NOT NULL,
	[riSection] [varchar](10) NULL,
	[icCost] [money] NOT NULL,
	[riISBN] [varchar](13) NOT NULL,
	[riUPC] [varchar](20) NULL,
	[LastUpdate] [datetime] NOT NULL
) ON [PRIMARY]
GO


USE [ReportsView]
GO

/****** Object:  Table [dbo].[hroXacns]    Script Date: 2/24/2021 5:43:59 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[hroXacns](
	[Loc] [char](5) NOT NULL,
	[ItemCode] [varchar](20) NOT NULL,
	[SkuExt] [varchar](10) NOT NULL,
	[Shpmt] [varchar](13) NOT NULL,
	[Date] [datetime] NOT NULL,
	[Xacn] [varchar](10) NOT NULL,
	[flow] [varchar](3) NOT NULL,
	[Qty] [int] NULL,
	[Inv] [int] NULL,
	[pshQty] [int] NULL,
	[mdQty] [int] NULL,
	[SldVal] [money] NULL,
	[mdSldVal] [money] NULL,
	[SldFee] [money] NULL,
	[n] [int] NOT NULL,
	[LastUpdate] [datetime] NULL,
	[Item] [varchar](20) NULL,
	[u] [int] NULL,
	[InvQ] [int] NULL,
	[crSet] [int] NULL,
	[nxIn] [int] NULL,
 CONSTRAINT [PK_hroXacns] PRIMARY KEY CLUSTERED 
(
	[Loc] ASC,
	[ItemCode] ASC,
	[Date] ASC,
	[SkuExt] ASC,
	[Shpmt] ASC,
	[Xacn] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO


USE [ReportsView]
GO

/****** Object:  Table [dbo].[hroXacns_ShRvDetail]    Script Date: 2/24/2021 5:44:05 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[hroXacns_ShRvDetail](
	[Loc] [char](5) NOT NULL,
	[ItemCode] [varchar](20) NOT NULL,
	[Shpmt] [varchar](13) NOT NULL,
	[Xacn] [varchar](10) NOT NULL,
	[Date] [datetime] NOT NULL,
	[Qty] [int] NULL,
	[PshQty] [int] NULL,
	[ShipDate] [smalldatetime] NULL,
	[ShipQty] [int] NULL,
	[ruShpDt] [smalldatetime] NULL,
	[sumShpQ] [int] NULL,
	[RcvDate] [datetime] NULL,
	[RcvQty] [int] NULL,
	[ruRcvDt] [datetime] NULL,
	[sumRcvQ] [int] NULL,
	[Src] [varchar](10) NULL,
	[ShpTnsDt] [smalldatetime] NULL,
	[LastUpdate] [datetime] NULL,
 CONSTRAINT [PK_hroXacns_ShRvDetail] PRIMARY KEY CLUSTERED 
(
	[Loc] ASC,
	[ItemCode] ASC,
	[Shpmt] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO


USE [ReportsView]
GO

/****** Object:  Table [dbo].[hroXacns_SetAdjs]    Script Date: 2/24/2021 5:44:17 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[hroXacns_SetAdjs](
	[Loc] [char](5) NOT NULL,
	[ItemCode] [varchar](20) NOT NULL,
	[Item] [varchar](20) NOT NULL,
	[crSet] [int] NOT NULL,
	[nxIn] [int] NOT NULL,
	[AdjQ] [int] NULL,
	[minInvQ] [int] NULL,
	[minAInvQ] [int] NULL,
	[nAdjQ] [int] NULL,
	[fAdjQ] [int] NULL,
	[fCC] [int] NULL,
	[Span] [int] NULL,
	[PctAdj] [numeric](24, 12) NULL,
	[fPctAdj] [numeric](24, 12) NULL,
 CONSTRAINT [PK_hroXacnsSetAdjs] PRIMARY KEY CLUSTERED 
(
	[Loc] ASC,
	[ItemCode] ASC,
	[Item] ASC,
	[crSet] ASC,
	[nxIn] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO


USE [ReportsView]
GO

/****** Object:  Table [dbo].[hroXacns_Ages]    Script Date: 2/24/2021 5:44:25 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[hroXacns_Ages](
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
 CONSTRAINT [PK_hroXacnsAges] PRIMARY KEY CLUSTERED 
(
	[Loc] ASC,
	[Item] ASC,
	[ItemCode] ASC,
	[u] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO


