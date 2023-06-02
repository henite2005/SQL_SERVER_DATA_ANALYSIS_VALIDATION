USE [dba]
go 

DECLARE @SchemaName VARCHAR(100) 
DECLARE @TableName VARCHAR(100) 
DECLARE @DatabaseName VARCHAR(100) 

--Create Temp Table to Save Results 
IF Object_id('tempdb..#Results') IS NOT NULL 
  DROP TABLE #results 

CREATE TABLE #results 
  ( 
     databasename       VARCHAR(100), 
     schemaname         VARCHAR(100), 
     tablename          VARCHAR(100), 
     columnlist         VARCHAR(max), 
     duplicatevalue     VARCHAR(max), 
     totaltablerowcount INT, 
     duplicaterowcnt    INT 
  ) 

DECLARE cur CURSOR FOR 
  SELECT table_catalog, 
         table_schema, 
         table_name 
  FROM   information_schema.tables 
  WHERE  table_type = 'BASE TABLE' 

OPEN cur 

FETCH next FROM cur INTO @DatabaseName, @SchemaName, @TableName 

WHILE @@FETCH_STATUS = 0 
  BEGIN 
      --Get List of the Columns from Table without Identity Column 
      DECLARE @ColumnList NVARCHAR(max)=NULL 

      SELECT @ColumnList = COALESCE(@ColumnList + '],[', '') + c.NAME 
      FROM   sys.columns c 
             INNER JOIN sys.tables t 
                     ON c.object_id = t.object_id 
      WHERE  Object_name(c.object_id) = @TableName 
             AND Schema_name(schema_id) = @SchemaName 
             AND is_identity = 0 

      SET @ColumnList='[' + @ColumnList + ']' 

      --Print @ColumnList 
      DECLARE @ColumnListConcat VARCHAR(max)=NULL 

      SET @ColumnListConcat=Replace(Replace(Replace(Replace(@ColumnList, '[', 
                                                    'ISNULL(Cast(['), ']', 
                                            '] AS VARCHAR(MAX)),''NULL'')'), 
                                            ',ISNULL', '+ISNULL'), '+', 
                            '+'',''+') 

      --Create Dynamic Query for Finding duplicate Records 
      DECLARE @DuplicateSQL NVARCHAR(max)=NULL 

      SET @DuplicateSQL= ';With CTE as   (select  ''' 
                         + @DatabaseName + ''' AS DBName,' + '''' 
                         + @SchemaName + ''' AS SchemaName,' + '''' 
                         + @TableName + ''' AS TableName,' + '''' 
                         + @ColumnList + ''' AS ColumnList,' 
                         + @ColumnListConcat 
                         + ' AS ColumnConcat,    (Select count(*) from [' + @SchemaName 
                         + '].[' + @TableName 
                         + '] With (Nolock))             AS TotalTableRowCount    ,RN = row_number()             over(PARTITION BY ' 
                         + @ColumnList + '  order by ' + @ColumnList 
                         + ')             from [' + @SchemaName + '].[' 
                         + @TableName + ']  ) Select * From CTE WHERE RN>1' 

      PRINT @DuplicateSQL 

      INSERT INTO #results 
      EXEC(@DuplicateSQL) 

      FETCH next FROM cur INTO @DatabaseName, @SchemaName, @TableName 
  END 

CLOSE cur 

DEALLOCATE cur 

SELECT * 
FROM   #results 
--drop table #Results 

