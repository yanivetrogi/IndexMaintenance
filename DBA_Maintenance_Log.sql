USE [DBA]
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

CREATE TABLE [dbo].[DBA_Maintenance_Log]
(
	[Id] [int] IDENTITY(1,1) NOT NULL,
	[Action] [varchar](50) NOT NULL,
	[Database] [varchar](128) NOT NULL,
	[Schema] [varchar](128) NOT NULL,
	[Table] [varchar](128) NOT NULL,
	[Index] [varchar](128) NOT NULL,
	[StartTime] [datetime] NULL,
	[EndTime] [datetime] NULL,
	[SizeKB] [bigint] NOT NULL,
	[AvgFragmentationPercent] [numeric](9, 2) NULL,
	[AvgFragmentationPercentAfter] [numeric](9, 2) NULL,
	[Command] [varchar](8000) NULL,
	[Error] [varchar](4000) NULL,
 CONSTRAINT [PK_DBA_Maintenance_Log] PRIMARY KEY NONCLUSTERED ([Id] ASC)
);
GO

CREATE CLUSTERED INDEX [IXC_DBA_Maintenance_Log__StartTime] ON [dbo].[DBA_Maintenance_Log]([StartTime] ASC);
GO



