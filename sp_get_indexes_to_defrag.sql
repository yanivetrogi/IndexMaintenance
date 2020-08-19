USE [master]
SET ANSI_NULLS ON; SET QUOTED_IDENTIFIER ON;
GO

IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_NAME = 'sp_get_indexes_to_defrag')
	EXEC ('CREATE PROC dbo.sp_get_indexes_to_defrag AS SELECT ''stub version, to be replaced''')
GO
/*
	Yaniv Etrogi - 20200425
	Populates the index_defrag_queue table with indexes information to be used as a queue.

	Sample Execution:
	EXEC dbo.sp_get_indexs_to_defrag @min_index_size_mb = 10, ,@exclude_current_partition = 1;
*/
ALTER PROCEDURE dbo.sp_get_indexes_to_defrag
(
	 @min_index_size_mb int = 0
	,@exclude_current_partition int = 1
)
AS 
SET NOCOUNT ON;  
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;  


IF OBJECT_ID('dbo.index_defrag_queue') IS NULL 
	CREATE TABLE dbo.index_defrag_queue (schemaname sysname , tablename sysname , objectid int , indexname sysname , indexid int , [blob] bit, index_size_mb int, rows bigint, partition_count int, partition_number int, max_partition_number int);  
ELSE	
	TRUNCATE TABLE dbo.index_defrag_queue;

  
-- Get indexes information for the rebuild/reorganize process.  
INSERT dbo.index_defrag_queue (schemaname , tablename , objectid , indexname , indexid , blob, index_size_mb, rows, partition_count, partition_number, max_partition_number)
SELECT 
	 SCHEMA_NAME(table_info.[schema_id]) AS schemaname
	,table_info.tablename AS tablename
	,table_info.[object_id] AS tableid
	,i.name AS indexname
	,i.index_id AS indexid
	,table_info.blob AS blob
	,indexe_info.index_size_mb
	,indexe_info.rows
	,indexe_info.partition_count
	,indexe_info.partition_number
	,ISNULL(partition_info.max_partition_number, 1)	
FROM (SELECT  
			t.[object_id] AS [object_id]
			, t.[name] AS tablename
			, t.[schema_id] AS [schema_id]
			, CASE WHEN EXISTS ( SELECT  1
								FROM    sys.columns c
								WHERE   c.object_id = t.object_id
										AND (system_type_id IN (35, 34, 241, 99) /* column type that doesn't support on-line index rebuild (text,image,XML,ntext) */ OR max_length < 0
											) ) THEN 1 ELSE 0 END AS blob
	FROM sys.tables t
	) table_info
INNER JOIN sys.indexes i ON table_info.[object_id] = i.[object_id]
-- Indexes size and partition count
INNER JOIN 
	(
		SELECT
				i.index_id
				,i.object_id
				,SUM(au.used_pages) / 128 AS index_size_mb
				,SUM(p.rows)rows		
				,COUNT(DISTINCT p.partition_number) AS partition_count
				,p.partition_number
		FROM sys.indexes i
		INNER JOIN sys.partitions p ON p.index_id = i.index_id AND p.object_id = i.object_id
		INNER JOIN sys.allocation_units au ON au.container_id = p.partition_id
		GROUP BY i.index_id, i.object_id, p.partition_number
	) indexe_info ON indexe_info.index_id = i.index_id AND indexe_info.object_id = i.object_id
-- max_partition_number
LEFT JOIN 
	(
		SELECT 
			i.object_id	
			,OBJECT_NAME(i.object_id) tabl
			,i.index_id
			,MAX(p.partition_number) - CASE WHEN @exclude_current_partition = 1 THEN 1 ELSE 0 END max_partition_number -- remove the last partition 
		FROM sys.indexes i
		INNER JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
		WHERE p.rows > 0 -- eliminate empty partitions
		GROUP BY i.object_id, i.index_id, OBJECT_NAME(i.object_id)
		HAVING  MAX(partition_number) > 1
	)partition_info ON partition_info.index_id = i.index_id AND partition_info.object_id = i.object_id
WHERE i.type >= 1 
AND indexe_info.index_size_mb > @min_index_size_mb	--Eliminate small indexes
--AND table_info.tablename NOT LIKE '%_Empty'		--Eliminate the Empty tables used for partiton managment
ORDER BY NEWID();	


SELECT @@ROWCOUNT;
GO

USE master;EXEC sp_MS_marksystemobject 'sp_get_indexes_to_defrag';
GO
