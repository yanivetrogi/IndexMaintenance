USE [master]
SET ANSI_NULLS ON; SET QUOTED_IDENTIFIER ON;
GO

IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_NAME = 'sp_index_defrag')
	EXEC ('CREATE PROC dbo.sp_index_defrag AS SELECT ''stub version, to be replaced''')
GO

/*
	Yaniv Etrogi - 20200425
	The procedure is dependent on the queue table index_defrag_queue populated by the stored procedure sp_get_indexes_to_defrag 

	When @single_index = 1 then the procedure will process a single index and terminate.
		This is to allow the Poershell script to parallel the indexes task using many threads where each thread performs a single index task and terminates
	When @single_index = 0 the the procedure will process all indexes and terminate only when the dbo.index_defrag_queue is empty
		

	Sample Execution:
	--1.
	EXEC dbo.sp_get_indexes_to_defrag @min_index_size_mb = 0, @exclude_current_partition = 1;

	--2.
	EXEC dbo.sp_index_defrag 
					 @reorg_threshold = 1, @rebuild_threshold = 70, @online = 1, @maxdop = 0
					,@sort_in_tempdb = 0,  @single_index = 1, @debug = 0;

*/
ALTER PROCEDURE [dbo].[sp_index_defrag]
(
	 @reorg_threshold tinyint = 0
	,@rebuild_threshold tinyint = 70
	,@online bit = 0
	,@maxdop tinyint = 0
	,@sort_in_tempdb bit = 0
	,@debug bit = 1
	,@single_index bit = 1
)
AS 
SET NOCOUNT ON;  
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;  


DECLARE 
 @objectid int
,@indexid int
,@page_count bigint
,@schemaname sysname
,@objectname sysname
,@indexname sysname
,@partitionnum bigint
,@avg_fragmentation_in_percent tinyint
,@blob bit
,@command varchar(8000)
,@ident int
,@command_type varchar(10)
,@check_date datetime
,@edition_check bit
,@start_time datetime
,@max_partition_number int;


-- Validation
BEGIN;
	-- Make sure we're not exceeding the number of processors we have available
	DECLARE @processor TABLE ([index] int, name varchar(128), Internal_Value int, Character_Value int);
	INSERT INTO @processor EXEC xp_msver 'ProcessorCount';

		
	IF @maxdop IS NULL 
		SELECT @maxdop = Internal_Value -1 FROM @processor;
	ELSE 
		IF @maxdop IS NOT NULL AND @maxdop > (SELECT Internal_Value FROM @processor)
			RAISERROR('Maxdop value exceeds the maximum number of processors available; Re-Execute correct.', 16, 0) WITH NOWAIT;
		
	-- Check our server version; 1804890536 = Enterprise(cal based lic), 610778273 = Enterprise Evaluation, -2117995310 = Developer, 1872460670 = Enterprise(core based lic)
	IF (SELECT  SERVERPROPERTY('EditionID')) IN (1804890536, 1872460670, 610778273, -2117995310) 
		SELECT @edition_check = 1; -- supports online rebuilds
	ELSE 
		SELECT @edition_check = 0; -- does not support online rebuilds	
	    
	IF @online = 1 AND @edition_check = 0 
		RAISERROR('SQL Edition does not support online rebuild; Modify the @online value to 0.', 16, 0) WITH NOWAIT;
            
	-- SQL 2014 supports online partition rebuild
	DECLARE @online_partition_rebuild bit = 0;
	IF (SELECT SERVERPROPERTY('ProductVersion') ) >= '12' SELECT @online_partition_rebuild = 1;
END;




IF OBJECT_ID('tempdb.dbo.#work_to_do') IS NOT NULL DROP TABLE dbo.#work_to_do;
CREATE TABLE dbo.#work_to_do(schemaname sysname , tablename sysname , objectid int , indexname sysname , indexid int , [blob] bit, index_size_mb int, rows bigint, partition_count int, partition_number int, max_partition_number int);

-- Outer loop. 
-- Dequeue one index each iteration from the index_defrag_queue table and process
WHILE 1=1
BEGIN;
	TRUNCATE TABLE dbo.#work_to_do;
	DELETE TOP (1) FROM dbo.index_defrag_queue OUTPUT deleted.* INTO dbo.#work_to_do;
		IF @@ROWCOUNT = 0 BEGIN; PRINT CHAR(10) + '-- There are no more indexe_info to process.'; BREAK; END;
  
	SELECT   
		 @objectid = objectid, @schemaname = schemaname, @objectname = tablename, @indexid = indexid, @indexname = indexname, @partitionnum = partition_number, @blob = blob
		--,@max_partition_number = CASE WHEN max_partition_number = 1 THEN 1 ELSE max_partition_number - 1 END --eliminate working on the current 
		,@max_partition_number = max_partition_number
	FROM dbo.#work_to_do;  
	--SELECT * FROM #work_to_do	

	SELECT 
		@page_count = s.page_count, @avg_fragmentation_in_percent = s.avg_fragmentation_in_percent
	FROM sys.dm_db_index_physical_stats(DB_ID(DB_NAME()), @objectid, @indexid, @partitionnum, N'LIMITED') s;	

	DECLARE @len int, @pos int;
	SELECT @len = 40, @pos = @len - LEN(@objectname);
	--PRINT REPLICATE('x', @pos)

	PRINT '-- ' + @objectname + REPLICATE(' ', @pos) + '		| partitionnum: ' + CAST(@partitionnum AS sysname)
			+ ' | frag: ' + CAST(@avg_fragmentation_in_percent AS sysname ) + ' | indexid: ' + CAST(@indexid AS sysname )
			+ ' | blob: ' + CAST(@blob AS sysname );
	

	-- Basic Syntax  
	SELECT @command = N'ALTER INDEX [' + @indexname + N'] ON [' + @schemaname + N'].[' + @objectname + N']';  

	-- Construct the command based on the fragmantation
	WHILE 1 = 1
	BEGIN;
		-- If the frag is bellow the reorg threshold exit the inner loop to fetch another index
		IF @avg_fragmentation_in_percent < @reorg_threshold BREAK;

		IF 
		(  
			-- Should be reorganized due to the fragmantation percent and the thresholds 
			(@avg_fragmentation_in_percent BETWEEN @reorg_threshold AND @rebuild_threshold )
					OR   
			-- Has clustered, blob and marked online, but it cant be rebuilt online due to the blob so we reorganize 
			(@blob = 1 AND @indexid = 1 AND @online = 1 )
					OR   
			-- If the version is prior 2014 and table has multiple partitions it can only be reorgenized online
			(@partitionnum > 1 AND @online = 1 AND @online_partition_rebuild = 0)
		) 
		BEGIN;   
			-- Reorg
			SELECT @command = @command + N' REORGANIZE' , @command_type = N'REORGANIZE';    
			IF @partitionnum > 1 
				SELECT @command = @command + N' PARTITION = ' + CAST(@partitionnum AS varchar(10));  
		END;  
			ELSE 
		BEGIN;
			-- Rebuild
			SELECT @command = @command + ' REBUILD' , @command_type = 'REBUILD';                  
            IF @partitionnum > 1 
                SELECT @command = @command + N' PARTITION = ' + CAST(@partitionnum AS varchar(10));  
            
			SELECT @command = @command + ' WITH (' + CASE WHEN @online = 1 THEN 'ONLINE = ON,' ELSE 'ONLINE = OFF,'
						END + CASE WHEN @maxdop > 0 THEN ' MAXDOP = ' + CAST(@maxdop AS NCHAR(2)) + ',' ELSE ' MAXDOP = 0,' END
						+ CASE WHEN @sort_in_tempdb = 1 THEN ' SORT_IN_TEMPDB = ON' ELSE ' SORT_IN_TEMPDB = OFF' END + ');';  
		END;

		PRINT (@command);

		-- Log and execuet
		IF @debug = 0
		BEGIN;
			--Log before execution
			SELECT @start_time = CURRENT_TIMESTAMP;

			INSERT DBA.dbo.DBA_Maintenance_Log([Action], [Database], [Schema], [Table], [Index], StartTime, EndTime, SizeKB, AvgFragmentationPercent, AvgFragmentationPercentAfter, [Command] ) 
				SELECT @command_type, DB_NAME(),@schemaname,@objectname,@indexname,@start_time, NULL, @page_count * 8, @avg_fragmentation_in_percent, NULL, @command;
			SELECT @ident = SCOPE_IDENTITY();  
		
			EXEC (@command);				
					
			-- Get the new fragmantation value
			SELECT 
				@avg_fragmentation_in_percent = s.avg_fragmentation_in_percent
			FROM sys.dm_db_index_physical_stats(DB_ID(DB_NAME()), @objectid, @indexid, @partitionnum, 'LIMITED') s;	

			-- Log after the defrag and update the new @avg_fragmentation_in_percent
			UPDATE DBA.dbo.DBA_Maintenance_Log SET EndTime = CURRENT_TIMESTAMP, AvgFragmentationPercentAfter = @avg_fragmentation_in_percent WHERE Id = @ident;
		END;

		IF @single_index = 1 RETURN; -- Exit procedure here
				ELSE BREAK;			 -- Exit inner loop and return to the outer loop fetch another index
	END;
END;
GO

USE master;EXEC sp_MS_marksystemobject 'sp_index_defrag';
GO

