# IndexMaintenance
Use Powershell to multi thread SQL Server Index Maintenance tasks

Please see this post for additional information

https://sqlserverutilities.com/use-powershell-to-multi-thread-sql-server-index-maintenance-tasks/

The Powershell script executes 2 Stored Procedures:

sp_get_indexes_to_defrag

sp_index_defrag




A sample execution of the T_SQL code would look like this

USE AdventureWorks2017;

--1.

EXEC dbo.sp_get_indexes_to_defrag  
   @min_index_size_mb = 0 
  ,@exclude_current_partition = 1;

--2.

EXEC dbo.sp_index_defrag 
           @reorg_threshold = 20
          ,@rebuild_threshold = 70
          ,@online = 1
          ,@maxdop = 0
          ,@sort_in_tempdb = 0
          ,@single_index = 1
          ,@debug = 0;
