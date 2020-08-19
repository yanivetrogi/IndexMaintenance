<#
.Synopsis
  Perform SQL Server Index Maintenance tasks in parallel in order to cut down the overall duration

.DESCRIPTION
  Use .NET RunspaceFactory Class and the CreateRunspacePool method in order to carry out Index Maintenance tasks multi threaded 

.INPUTS
  $MaxThreads
  $Server

.OUTPUTS
    None

.NOTES
  This script can be usefull and practical for VLDBs where the available maintenance window is never enough to complete the Index Maitenance tasks
  Whatch out for the value asigned to $MaxThreads and validate that it matches your enviroment for 2 main things:
  1. Disk letancy (do not overload the disk subsystem in order not to negetivly effect other processes that may be running on the system )
  2. Always On availability groups (verify there is no latency to Secondary Replica(s) )

  Modify the parameters asigned to the stored procedure sp_index_defrag to meet your needs (at the moment the values are hard coded)

#>


# The number of threads to be used for processing index tasks in prallel
[int]$MaxThreads = 8

# The SQL Server instance we work on
[string]$Server = $env:COMPUTERNAME; 


#region <databases>
$Database = 'master';
$CommandText = 'SELECT name FROM sys.databases WHERE database_id > 4 ORDER BY name;';
$ConnectionString = "Server=$Server; Database=$Database; Integrated Security=True; Application Name=index_defrag;";
[System.Data.DataSet]$ds_Databases = New-Object System.Data.DataSet;

# Get the list of databases.
try
{
    $SqlConnection = New-Object System.Data.SqlClient.SqlConnection($ConnectionString);
    $SqlCommand = $sqlConnection.CreateCommand();      
    $SqlConnection.Open();
    $SqlCommand.CommandText = $CommandText;      
       
    $SqlAdapter = New-Object System.Data.SqlClient.SqlDataAdapter;
    $SqlAdapter.SelectCommand = $SqlCommand;    
    $SqlAdapter.Fill($ds_Databases);              
}
catch {throw $_ };
#endregion



# Loop over the databases
foreach($Row in $ds_Databases.Tables[0].Rows)
{
    $Database = $Row.name;    

    #region <indexes_defrag_queue>
    # Populate the index_defrag_queue table with rows to be processed and return the count of rows.
    try
    {
       $ConnectionString = "Server=$Server; Database=$Database; Integrated Security=True; Application Name=index_defrag;";
       $CommandText = 'EXEC dbo.sp_get_indexes_to_defrag @min_index_size_mb = 0;';  

       $SqlConnection = New-Object System.Data.SqlClient.SqlConnection($ConnectionString);
       $SqlCommand = $sqlConnection.CreateCommand();
       $SqlConnection.Open();
       $SqlCommand.CommandText = $CommandText;
       # Get the number of rows to be used as the upper bound of the loop.
       $NumRows = $SqlCommand.ExecuteScalar();      
    }
    catch {throw $_ };
    #endregion
   


    [string]$CommandText = "SET NOCOUNT ON; EXEC sp_index_defrag @reorg_threshold = 10, @rebuild_threshold = 99, @online = 1, @maxdop = 0, @sort_in_tempdb = 0, @single_index = 1, @debug = 0";

    $ScriptBlock =
    {
        param(
           $CommandText       = $CommandText,
           $ConnectionString  = $ConnectionString
       )
      $ConnectionString = $ConnectionString;
     
       # Process the index
       try
       {
           $SqlConnection = New-Object System.Data.SqlClient.SqlConnection($ConnectionString);
           $SqlCommand = $sqlConnection.CreateCommand();
           $SqlCommand.CommandTimeout = 0;
           $SqlConnection.Open();
           $SqlCommand.CommandText = $CommandText;      
           $SqlCommand.ExecuteNonQuery();      
       }
       catch {throw $_ };
    }

    $RunspacePool = [runspacefactory]::CreateRunspacePool(1, $MaxThreads);
    $RunspacePool.Open();
    $Jobs = @();

    1..$NumRows | Foreach-Object {
       $PowerShell = [powershell]::Create();
       $PowerShell.RunspacePool = $RunspacePool;
       $PowerShell.AddScript($ScriptBlock).AddParameter("CommandText",$CommandText).AddParameter("ConnectionString",$ConnectionString)
       $Jobs += $PowerShell.BeginInvoke();       
    }    
    while ($Jobs.IsCompleted -contains $false)
    {        
       Start-Sleep -Milliseconds 10;       
    };
    $RunspacePool.Close();
    $RunspacePool.Dispose();
}