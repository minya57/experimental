USE [DBA]
GO

/****** Object:  StoredProcedure [dbo].[sp_Audit_CreateAuditObject]    Script Date: 8/3/2022 10:14:28 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[sp_Audit_CreateAuditObject]
(
		@vchr_TblName				VARCHAR(300),
		@vchr_AuditDBName			VARCHAR(50),
		@vchr_DataDBName			VARCHAR(50),
		@vchr_PKList				VARCHAR(MAX)	= NULL,
		@vchr_TR_Insert_CustomSQL	VARCHAR(MAX)	= NULL,
		@vchr_TR_Update_CustomSQL	VARCHAR(MAX)	= NULL,
		@vchr_TR_Delete_CustomSQL	VARCHAR(MAX)	= NULL,
		@vchr_ColumnName_UpdatedBy	VARCHAR(300)	= NULL,
		@vchr_ColumnName_UpdatedOn	VARCHAR(300)	= NULL,
		@vchr_ColumnName_UpdatedByHost	VARCHAR(300)	= NULL,
		@bool_DoCreateMissingOnly	BIT				= 0,
		@bool_DoDebug				BIT				= 1,
		@bool_DoExec				BIT				= 1
)
AS
-- =============================================o
-- Author:		Mikhail Peterburgskiy
-- Description:	Creates corresponding audit 
--				 object with required triggers
--
--
--	Rules for in-place audit (same database)
--	a. schema name: audit
--	b. file group for audit objects: audit
--
--
-- =============================================
SET NOCOUNT ON
BEGIN
	DECLARE
		@vchrTblName_Adt	VARCHAR(300),
		@vchrViewName		VARCHAR(300),
		@vchrSQLStmt		VARCHAR(MAX),
		@vchrSQLStmt_GetMaxDate	VARCHAR(MAX),
		@vchrSQLStmt_CTbl	VARCHAR(MAX),
		@nvchrSQLStmt_Syn	NVARCHAR(MAX),
		@nvchrSQLStmt_View	NVARCHAR(MAX),
		@vchrSQLStmt_ITrg	VARCHAR(MAX),
		@vchrSQLStmt_UTrg	VARCHAR(MAX),
		@vchrSQLStmt_DTrg	VARCHAR(MAX),
		@vchrSQLStmt_Index1	VARCHAR(MAX),
		@vchrSQLStmt_Index2	VARCHAR(MAX),
		@vchrPKList			VARCHAR(MAX),
		@intError			INT,
		@intLocalTran		BIT,
		@cnstNewLine		CHAR(2),
		@vchrObjectName		VARCHAR(500),
		@vchrMsg			VARCHAR(MAX),
		@boolIsInPlaceAudit	BIT

	SELECT
		@vchrTblName_Adt	= @vchr_TblName + '_AUDIT',
		-- view should be the same thing but without t
		@vchrViewName		= RIGHT(@vchr_TblName, LEN(@vchr_TblName) - 1) + '_AUDIT',
		@vchrSQLStmt		= '',
		@intLocalTran		= 0,
		@cnstNewLine		= CHAR(9) + CHAR(10),
		@intError			= 0,
		@boolIsInPlaceAudit	= CASE WHEN @vchr_AuditDBName = @vchr_DataDBName THEN 1 ELSE 0 END

	DECLARE @tblIndex TABLE
	(
		indexName	VARCHAR(300),
		indexDscr	VARCHAR(300),
		indexKeys	VARCHAR(300)
	)

	DECLARE @tblSysObjects TABLE
	(
		[name]					SYSNAME,
		[object_id]				INT,
		[principal_id]			INT,
		[schema_id]				INT,
		[parent_object_id]		INT,
		[type]					VARCHAR(2),
		[type_desc]				NVARCHAR(120),
		[create_date]			DATETIME,
		[modify_date]			DATETIME,
		[is_ms_shipped]			BIT,
		[is_published]			BIT,
		[is_schema_published]	BIT,
		PRIMARY KEY ([object_id])
	)

	DECLARE @tblSysColumns TABLE
	(
		[object_id]				INT,
		[name]					SYSNAME,
		[column_id]				INT,
		[system_type_id]		TINYINT,
		[user_type_id]			INT,
		[max_length]			SMALLINT,
		[precision]				TINYINT,
		[scale]					TINYINT,
		[collation_name]		SYSNAME		NULL,
		[is_nullable]			BIT,
		[is_ansi_padded]		BIT,
		[is_rowguidcol]			BIT,
		[is_identity]			BIT,
		[is_computed]			BIT,
		[is_filestream]			BIT,
		[is_replicated]			BIT,
		[is_non_sql_subscribed]	BIT,
		[is_merge_published]	BIT,
		[is_dts_replicated]		BIT,
		[is_xml_document]		BIT,
		[xml_collection_id]		INT,
		[default_object_id]		INT,
		[rule_object_id]		INT,
		PRIMARY KEY ([object_id], [column_id])
	)

	DECLARE @tblSysTypes TABLE
	(
		[name]				SYSNAME,
		system_type_id		TINYINT,
		user_type_id		INT,
		[schema_id]			INT,
		principal_id		INT,
		max_length			SMALLINT,
		[precision]			TINYINT,
		scale				TINYINT,
		collation_name		SYSNAME		NULL,
		is_nullable			BIT,
		is_user_defined		BIT,
		is_assembly_type	BIT,
		default_object_id	INT,
		rule_object_id		INT
	)

	DECLARE @tblSysIndexes_Audit TABLE
	(
		[object_id]				INT,
		name					VARCHAR(MAX),		
		index_id				INT,
		[type]					TINYINT,
		type_desc				NVARCHAR(60),
		is_unique				BIT,
		data_space_id			INT,
		[ignore_dup_key]		BIT,
		is_primary_key			BIT,
		is_unique_constraint	BIT,
		fill_factor				TINYINT,
		is_padded				BIT,
		is_disabled				BIT,
		is_hypothetical			BIT,
		[allow_row_locks]		BIT,
		[allow_page_locks]		BIT
		-- sql 2008
		--has_filter				bit,
		--filter_definition		nvarchar(max)
	)

	DECLARE @tblSysSchemas TABLE
	(
		name		VARCHAR(300)
	)

	DECLARE @tblSysFileGroups TABLE
	(
		name		VARCHAR(300)
	)

	SELECT @vchrSQLStmt = 
		'SELECT ' +
			'[name] ' +
		'FROM [' + @vchr_DataDBName + '].sys.schemas'
	INSERT INTO @tblSysSchemas
	 EXEC (@vchrSQLStmt)
	 
	SELECT @vchrSQLStmt = 
		'SELECT ' +
			'[name] ' +
		'FROM [' + @vchr_DataDBName + '].sys.filegroups'
	INSERT INTO @tblSysFileGroups
	 EXEC (@vchrSQLStmt)

	SELECT @vchrSQLStmt = 
		'SELECT ' +
			'[name], ' +
			'[object_id], ' +
			'[principal_id], ' +
			'[schema_id], ' +
			'[parent_object_id], ' +
			'[type], ' +
			'[type_desc], ' +
			'[create_date], ' +
			'[modify_date], ' +
			'[is_ms_shipped], ' +
			'[is_published], ' +
			'[is_schema_published] ' +
		'FROM [' + @vchr_DataDBName + '].sys.objects'
	INSERT INTO @tblSysObjects
	 EXEC (@vchrSQLStmt)
	
	SELECT @vchrSQLStmt = 
		'SELECT ' +
			'[object_id], ' +			
			'[name], ' +			
			'[column_id], ' +
			'[system_type_id], ' +
			'[user_type_id], ' +
			'[max_length], ' +
			'[precision], ' +
			'[scale], ' +
			'[collation_name], ' +
			'[is_nullable], ' +
			'[is_ansi_padded], ' +
			'[is_rowguidcol], ' +
			'[is_identity], ' +
			'[is_computed], ' +
			'[is_filestream], ' +
			'[is_replicated], ' +
			'[is_non_sql_subscribed], ' +
			'[is_merge_published], ' +
			'[is_dts_replicated], ' +
			'[is_xml_document], ' +
			'[xml_collection_id], ' +
			'[default_object_id], ' +
			'[rule_object_id] ' +	
		 'FROM [' + @vchr_DataDBName + '].sys.columns'
	INSERT INTO @tblSysColumns
	 EXEC (@vchrSQLStmt)
	
	SELECT @vchrSQLStmt = 
		'SELECT ' + 
			'[name], ' +		
			'system_type_id, ' +
			'user_type_id, ' +
			'[schema_id], ' +			
			'principal_id, ' +	
			'max_length, ' +	
			'[precision], ' +			
			'scale, ' +	
			'collation_name, ' +
			'is_nullable, ' +		
			'is_user_defined, ' +
			'is_assembly_type, ' +
			'default_object_id, ' +
			'rule_object_id ' +
		'FROM [' + @vchr_DataDBName + '].sys.types'
	INSERT INTO @tblSysTypes
	 EXEC (@vchrSQLStmt)
	
	SELECT @vchrSQLStmt = 
		'SELECT ' + 		
			'[object_id], ' +		
			'name, ' +	
			'index_id, ' +		
			'[type], ' +		
			'type_desc, ' +		
			'is_unique, ' +		
			'data_space_id, ' +
			'[ignore_dup_key], ' +
			'is_primary_key, ' +
			'is_unique_constraint, ' +
			'fill_factor, ' +
			'is_padded, ' +	
			'is_disabled, ' +			
			'is_hypothetical, ' +		
			'[allow_row_locks], ' +
			'[allow_page_locks] ' +
		'FROM [' + @vchr_AuditDBName + '].sys.indexes'
	INSERT INTO @tblSysIndexes_Audit
	 EXEC (@vchrSQLStmt)

	IF @boolIsInPlaceAudit = 1
	 BEGIN
		IF NOT EXISTS (SELECT TOP 1 1 FROM @tblSysSchemas WHERE name = 'audit')
		 BEGIN
			SELECT @vchrMsg = 'Cannot create audit objects in [' + @vchr_AuditDBName + '] becuase it is missing [audit] schema.'
			RAISERROR(@vchrMsg, 16, 1)
			RETURN
		 END

		IF NOT EXISTS (SELECT TOP 1 1 FROM @tblSysFileGroups WHERE name = 'audit')
		 BEGIN
			SELECT @vchrMsg = 'Cannot create audit objects in [' + @vchr_AuditDBName + '] becuase it is missing [audit] filegroup.'
			RAISERROR(@vchrMsg, 16, 1)
			RETURN
		 END
	 END

	IF (@vchr_PKList IS NULL)
	 BEGIN
		SELECT @vchrSQLStmt = 'EXEC [' + @vchr_DataDBName + '].dbo.sp_helpindex ''' + @vchr_TblName + ''''

		INSERT INTO @tblIndex
		EXEC (@vchrSQLStmt)

		DELETE @tblIndex WHERE indexName NOT LIKE 'PK_%'

		IF ( (SELECT COUNT(*) FROM @tblIndex) != 1)
		 BEGIN
			RAISERROR ('Table does not have PK index or PK has incorrect name (it should be PK_<table name>), please adjust table or specify one.', 15, 1)
			GOTO TheEnd
		 END

		SELECT
			@vchrPKList = REPLACE(indexKeys, '(-)', '')
		 FROM
			@tblIndex
	 END
	ELSE
	 BEGIN
		SELECT @vchrPKList = @vchr_PKList
	 END

	SELECT
		@vchrMsg = DBLibrary.dbo.ConcatStr(DISTINCT t.name, ', ', 'ASC')
	 FROM
		@tblSysObjects o
		INNER JOIN @tblSysColumns c
		 ON (c.object_id = o.object_id)
		INNER JOIN @tblSysTypes t
		 ON (t.user_type_id = c.user_type_id)
	 WHERE
		o.name = @vchr_TblName AND
		LEN(ISNULL([dbo].[GetFormattedType](t.NAME, c.[precision], c.max_length, c.scale), '')) = 0
	 
	IF LEN(ISNULL(@vchrMsg, '')) > 0
	 BEGIN
		SELECT @vchrMsg = 'Audit does not support the following datatypes, please contact your DBA to adjust: ' + @vchrMsg
		RAISERROR (@vchrMsg, 15, 1)
		GOTO TheEnd
	 END

	-- resolve UpdatedBy and UpdatedOn column names
	IF LEN(ISNULL(@vchr_ColumnName_UpdatedBy, '')) = 0
	 BEGIN
		IF EXISTS (
			SELECT TOP 1 1 
			 FROM 
				@tblSysObjects o
				INNER JOIN @tblSysColumns c
				 ON (c.object_id = o.object_id) 
			 WHERE 
				o.name =  @vchr_TblName AND
				c.name = 'UpdatedBy'
			)
		 BEGIN
			SELECT @vchr_ColumnName_UpdatedBy = 'UpdatedBy'
		 END
		IF EXISTS (
			SELECT TOP 1 1 
			 FROM 
				@tblSysObjects o
				INNER JOIN @tblSysColumns c
				 ON (c.object_id = o.object_id) 
			 WHERE 
				o.name =  @vchr_TblName AND
				c.name = 'UpdateName'
			)
		BEGIN
			SELECT @vchr_ColumnName_UpdatedBy = 'UpdateName'
		END
	 END
	IF LEN(ISNULL(@vchr_ColumnName_UpdatedOn, '')) = 0
	 BEGIN
		IF EXISTS (
			SELECT TOP 1 1 
			 FROM 
				@tblSysObjects o
				INNER JOIN @tblSysColumns c
				 ON (c.object_id = o.object_id) 
			 WHERE 
				o.name =  @vchr_TblName AND
				c.name = 'UpdatedOn'
			)
		 BEGIN
			SELECT @vchr_ColumnName_UpdatedOn = 'UpdatedOn'
		 END
		IF EXISTS (
			SELECT TOP 1 1 
			 FROM 
				@tblSysObjects o
				INNER JOIN @tblSysColumns c
				 ON (c.object_id = o.object_id) 
			 WHERE 
				o.name =  @vchr_TblName AND
				c.name = 'UpdateDate'
			)
		BEGIN
			SELECT @vchr_ColumnName_UpdatedOn = 'UpdateDate'
		END
	 END 
	IF LEN(ISNULL(@vchr_ColumnName_UpdatedByHost, '')) = 0
	 BEGIN
		IF EXISTS (
			SELECT TOP 1 1 
			 FROM 
				@tblSysObjects o
				INNER JOIN @tblSysColumns c
				 ON (c.object_id = o.object_id) 
			 WHERE 
				o.name =  @vchr_TblName AND
				c.name = 'UpdatedByHost'
			)
		 BEGIN
			SELECT @vchr_ColumnName_UpdatedByHost = 'UpdatedByHost'
		 END
	 END 
	
	
	-- create helping funcitons
	SELECT @vchrSQLStmt = 
		'CREATE FUNCTION [dbo].[GetMaxDate]() ' +
		 'RETURNS DATETIME ' +
		 'AS ' +
		  'BEGIN ' +
			'RETURN ''1/1/3000'' ' +
		  'END '

	SELECT @vchrSQLStmt_GetMaxDate = 
				CASE
					WHEN OBJECT_ID(@vchr_AuditDBName + '..GetMaxDate') IS NOT NULL THEN ''
					ELSE @vchrSQLStmt
				END

	-- create table
	SELECT 
		@vchrSQLStmt	= 
			'CREATE TABLE ' + CASE WHEN @boolIsInPlaceAudit = 1 THEN '[audit]' ELSE '[dbo]' END + '.' + @vchrTblName_Adt +	
			' ( ' +
			'AuditRecId	int identity(1, 1) NOT NULL, ',
		@vchrObjectName	= @vchrTblName_Adt

	

	SELECT
		@vchrSQLStmt = @vchrSQLStmt +  
		'[' + c.name + '] ' + 
		CASE 
			WHEN [dbo].[GetFormattedType](t.NAME, c.[precision], c.max_length, c.scale) = 'timestamp' THEN 'VARBINARY(8)' 
			ELSE [dbo].[GetFormattedType](t.NAME, c.[precision], c.max_length, c.scale)
		END +
		--t.name + 
		--CASE
		--	WHEN t.name IN ('NVARCHAR', 'NCHAR', 'VARCHAR', 'CHAR', 'VARBINARY', 'BINARY') AND c.max_length > 0 THEN '(' + CONVERT(VARCHAR(10), c.max_length) + ')'
		--	WHEN t.name IN ('NVARCHAR', 'NCHAR', 'VARCHAR', 'CHAR', 'VARBINARY', 'BINARY') AND c.max_length = -1 THEN '(MAX)'
		--	WHEN t.name IN ('NUMERIC') THEN '(' + CAST(c.precision AS VARCHAR) + ',' + CAST(c.scale AS VARCHAR) + ')'
		--	ELSE ''
		--END + 
		', '
	 FROM
		@tblSysObjects o
		INNER JOIN @tblSysColumns c
		 ON (c.object_id = o.object_id)
		INNER JOIN @tblSysTypes t
		 ON (t.user_type_id = c.user_type_id)
	 WHERE
		o.name = @vchr_TblName
	 ORDER BY
		column_id

	SELECT @vchrSQLStmt = @vchrSQLStmt + 
			'[AuditCreatedBy] [varchar](50)	NOT NULL CONSTRAINT [DF_' + @vchrTblName_Adt + '_AuditCreatedBy]  DEFAULT (suser_sname()), ' +
			'[AuditStartDate] [datetime]		NOT NULL CONSTRAINT [DF_' + @vchrTblName_Adt + '_AuditStartDate]  DEFAULT (getdate()), ' +
			'[AuditEndDate]	 [datetime]		NOT NULL CONSTRAINT [DF_' + @vchrTblName_Adt + '_AuditEndDate]  DEFAULT ([dbo].[GetMaxDate]()), ' +
			'[AuditAction]	 [char](1)		NOT NULL ' +
			'CONSTRAINT [PK_' + @vchrTblName_Adt + '] PRIMARY KEY NONCLUSTERED ' +
			'( ' +
				'[AuditRecId] DESC ' +
			') ON ' + CASE WHEN @boolIsInPlaceAudit = 1 THEN '[AUDIT]' ELSE '[PRIMARY]' END +
			') ON ' + CASE WHEN @boolIsInPlaceAudit = 1 THEN '[AUDIT]' ELSE '[PRIMARY]' END +
			''

	SELECT @vchrSQLStmt_CTbl = 
				CASE
					WHEN @bool_DoCreateMissingOnly = 1 AND OBJECT_ID(@vchr_AuditDBName + '.' + CASE WHEN @boolIsInPlaceAudit = 1 THEN '[audit]' ELSE '[dbo]' END + '.' + @vchrObjectName) IS NOT NULL THEN ''
					ELSE @vchrSQLStmt
				END

	-- create synonym
--	SELECT 
--		@vchrSQLStmt	=
--			'CREATE SYNONYM [' + @vchrTblName_Adt + '] FOR WholeLoanAudit.dbo.' + @vchrTblName_Adt,
--		@vchrObjectName	= @vchrTblName_Adt
--
--	SELECT @nvchrSQLStmt_Syn = 
--				CASE
--					WHEN @bool_DoCreateMissingOnly = 1 AND OBJECT_ID('WholeLoan.dbo.' + @vchrObjectName) IS NOT NULL THEN ''
--					ELSE @vchrSQLStmt
--				END

	-- create view
	SELECT 
		@vchrSQLStmt	=
			'CREATE VIEW ' + CASE WHEN @boolIsInPlaceAudit = 1 THEN '[audit]' ELSE '[dbo]' END + '.' + @vchrViewName + @cnstNewLine +
			' AS ' + @cnstNewLine +
			'  SELECT ' + @cnstNewLine +
--			'	AuditRecId, ' + @cnstNewLine +
			'	AuditAction, ' + @cnstNewLine,
		@vchrObjectName	= @vchrViewName

	SELECT
		@vchrSQLStmt = @vchrSQLStmt +  
		'	[' + c.name + '], ' + @cnstNewLine
	 FROM
		@tblSysObjects o
		INNER JOIN @tblSysColumns c
		 ON (c.object_id = o.object_id)
		INNER JOIN @tblSysTypes t
		 ON (t.user_type_id = c.user_type_id)
	 WHERE
		o.name = @vchr_TblName
	 ORDER BY
		column_id

	SELECT @vchrSQLStmt = @vchrSQLStmt + 
			'	[AuditCreatedBy], ' + @cnstNewLine +
			'	[AuditStartDate], ' + @cnstNewLine +
			'	[AuditEndDate] ' + @cnstNewLine +
			'  FROM ' + @cnstNewLine +
			'	' + CASE WHEN @boolIsInPlaceAudit = 1 THEN '[audit]' ELSE '[dbo]' END + '.[' + @vchrTblName_Adt + ']'

	SELECT @nvchrSQLStmt_View = 
				CASE
					WHEN @bool_DoCreateMissingOnly = 1 AND OBJECT_ID(@vchr_AuditDBName + '.' + CASE WHEN @boolIsInPlaceAudit = 1 THEN '[audit]' ELSE '[dbo]' END + '.' + @vchrObjectName) IS NOT NULL THEN ''
					ELSE @vchrSQLStmt
				END

	-- create insert triger
	SELECT @vchrSQLStmt = ''
	SELECT 
		@vchrSQLStmt = @vchrSQLStmt + ' l.[' + item + '] = i.[' + item + '] AND '
	 FROM
		dbo.fnSplit(@vchrPKList, ',')

	SELECT @vchrSQLStmt = LEFT(@vchrSQLStmt, LEN(@vchrSQLStmt) - 4)
	
	SELECT 
		@vchrObjectName = 'TR_' + @vchrTblName_Adt + '_INSERT',
		@vchrSQLStmt = 
			'CREATE TRIGGER ' + CASE WHEN @boolIsInPlaceAudit = 1 THEN '[audit]' ELSE '[dbo]' END + '.[' + @vchrObjectName + '] ' + @cnstNewLine +
			'ON  ' + CASE WHEN @boolIsInPlaceAudit = 1 THEN '[audit]' ELSE '[dbo]' END + '.[' + @vchrTblName_Adt + ']  ' + @cnstNewLine +
			'	FOR INSERT ' + @cnstNewLine +
			'AS  ' + @cnstNewLine +
			'SET NOCOUNT ON ' + @cnstNewLine +
			'BEGIN ' + @cnstNewLine +
			'	DECLARE ' + @cnstNewLine +
			'		@dtCurrentDate	DATETIME ' + @cnstNewLine +
			'	SELECT ' + @cnstNewLine +
			'		@dtCurrentDate	= CONVERT(DATETIME, CONVERT(VARCHAR(30), GETDATE(), 120)) ' + @cnstNewLine +
			'	UPDATE  ' + @cnstNewLine +
			'		l ' + @cnstNewLine +
			'	 SET ' + @cnstNewLine +
			'		AuditEndDate =  ' + @cnstNewLine +
			'				CASE ' + @cnstNewLine +
			'					WHEN l.AuditStartDate != i.AuditStartDate THEN DATEADD(ss, -1, @dtCurrentDate) ' + @cnstNewLine +
			'					ELSE l.AuditEndDate ' + @cnstNewLine +
			'				END, ' + @cnstNewLine +
			'		AuditStartDate = ' +  @cnstNewLine +
			'				CASE ' + @cnstNewLine +
			'					WHEN l.AuditStartDate = i.AuditStartDate THEN @dtCurrentDate ' + @cnstNewLine +
			'					ELSE l.AuditStartDate ' + @cnstNewLine +
			'				END ' + @cnstNewLine +
			'	 FROM  ' + @cnstNewLine +
			'		[' + CASE WHEN @boolIsInPlaceAudit = 1 THEN 'audit' ELSE 'dbo' END + '].[' + @vchrTblName_Adt + '] l ' + @cnstNewLine +
			'		INNER JOIN INSERTED i ' + @cnstNewLine +
			'		 ON ' + @vchrSQLStmt + ' ' + @cnstNewLine +
			'	 WHERE ' + @cnstNewLine +
			'		l.AuditEndDate = dbo.GetMaxDate() ' + @cnstNewLine +
			'END '

	SELECT @vchrSQLStmt_ITrg = 
				CASE
					WHEN @bool_DoCreateMissingOnly = 1 AND OBJECT_ID(@vchr_AuditDBName + '.' + CASE WHEN @boolIsInPlaceAudit = 1 THEN '[audit]' ELSE '[dbo]' END + '.' + @vchrObjectName) IS NOT NULL THEN ''
					ELSE @vchrSQLStmt
				END

	-- create delete triger
	SELECT 
		@vchrObjectName = 'TR_' + @vchrTblName_Adt + '_DELETE',
		@vchrSQLStmt = 
			'CREATE TRIGGER ' + CASE WHEN @boolIsInPlaceAudit = 1 THEN '[audit]' ELSE '[dbo]' END + '.[' + @vchrObjectName + '] ' + @cnstNewLine +
			'   ON  ' + CASE WHEN @boolIsInPlaceAudit = 1 THEN '[audit]' ELSE '[dbo]' END + '.[' + @vchrTblName_Adt + ']  ' + @cnstNewLine +
			'   FOR DELETE ' + @cnstNewLine +
			'AS  ' + @cnstNewLine +
			'SET NOCOUNT ON ' + @cnstNewLine +
			'BEGIN ' + @cnstNewLine +
			'	RAISERROR (''Cannot delete records from [' + @vchr_AuditDBName + '].' + CASE WHEN @boolIsInPlaceAudit = 1 THEN '[audit]' ELSE '[dbo]' END + '.' + @vchrTblName_Adt + ' table'', 15, 1) ' + @cnstNewLine +
			'	ROLLBACK ' + @cnstNewLine +
			'	RETURN ' + @cnstNewLine +
			'END '

	SELECT @vchrSQLStmt_DTrg = 
				CASE
					WHEN @bool_DoCreateMissingOnly = 1 AND OBJECT_ID(@vchr_AuditDBName + '.' + CASE WHEN @boolIsInPlaceAudit = 1 THEN '[audit]' ELSE '[dbo]' END + '.' + @vchrObjectName) IS NOT NULL THEN ''
					ELSE @vchrSQLStmt
				END

	-- create update triger
	SELECT 
		@vchrObjectName = 'TR_' + @vchrTblName_Adt + '_UPDATE',
		@vchrSQLStmt = 
			'CREATE TRIGGER ' + CASE WHEN @boolIsInPlaceAudit = 1 THEN '[audit]' ELSE '[dbo]' END + '.[' + @vchrObjectName + '] ' +  @cnstNewLine +
			'ON  ' + CASE WHEN @boolIsInPlaceAudit = 1 THEN '[audit]' ELSE '[dbo]' END + '.[' + @vchrTblName_Adt + ']  ' + @cnstNewLine +
			'FOR UPDATE ' + @cnstNewLine +
			'AS  ' + @cnstNewLine +
			'SET NOCOUNT ON ' + @cnstNewLine +
			'BEGIN	 ' + @cnstNewLine +
			'	IF NOT (UPDATE(AuditEndDate) OR UPDATE(AuditStartDate)) ' + @cnstNewLine +
			'	 BEGIN ' + @cnstNewLine +
			'		RAISERROR (''Cannot update records from [' + @vchr_AuditDBName + '].' + CASE WHEN @boolIsInPlaceAudit = 1 THEN '[audit]' ELSE '[dbo]' END + '.' + @vchrTblName_Adt + ' table'', 15, 1) ' + @cnstNewLine +
			'		ROLLBACK ' + @cnstNewLine +
			'		RETURN ' + @cnstNewLine +
			'	 END ' + @cnstNewLine +
			'END '

	SELECT @vchrSQLStmt_UTrg = 
				CASE
					WHEN @bool_DoCreateMissingOnly = 1 AND OBJECT_ID(@vchr_AuditDBName + '.' + CASE WHEN @boolIsInPlaceAudit = 1 THEN '[audit]' ELSE '[dbo]' END + '.' + @vchrObjectName) IS NOT NULL THEN ''
					ELSE @vchrSQLStmt
				END


	-- create index for ID
	SELECT 
		@vchrObjectName = 'IDX_' + @vchrTblName_Adt + '__' + REPLACE(REPLACE(@vchrPKList, ',', '_'), ' ', ''),
		@vchrSQLStmt = 
			'CREATE NONCLUSTERED INDEX [' + @vchrObjectName + '] ON ' + CASE WHEN @boolIsInPlaceAudit = 1 THEN '[audit]' ELSE '[dbo]' END + '.[' + @vchrTblName_Adt + '] ' +
			'( ' +
				'[' + REPLACE(REPLACE(@vchrPKList, ', ', ','), ',', '] DESC, [') + '] DESC ' +
			') ON ' + CASE WHEN @boolIsInPlaceAudit = 1 THEN '[AUDIT]' ELSE '[PRIMARY]
			' END

	SELECT @vchrSQLStmt_Index1 = 
				CASE
					WHEN 
						@bool_DoCreateMissingOnly = 1 AND 
						EXISTS(SELECT TOP 1 1 FROM @tblSysIndexes_Audit WHERE Name = @vchrObjectName) THEN ''
					WHEN 
						@bool_DoCreateMissingOnly = 1 AND 
						EXISTS(SELECT TOP 1 1 FROM @tblSysIndexes_Audit WHERE Name = REPLACE(@vchrObjectName, '__', '_')) THEN ''
					ELSE @vchrSQLStmt
				END
				
	-- create index for AuditEndDate
	SELECT 
		@vchrObjectName = 'IDX_' + @vchrTblName_Adt + '__AuditEndDate',
		@vchrSQLStmt = 
			'CREATE CLUSTERED INDEX [' + @vchrObjectName + '] ON ' + CASE WHEN @boolIsInPlaceAudit = 1 THEN '[audit]' ELSE '[dbo]' END + '.[' + @vchrTblName_Adt + '] ' +
			'( ' +
				'[AuditEndDate] DESC ' +
			') ON ' + CASE WHEN @boolIsInPlaceAudit = 1 THEN '[AUDIT]' ELSE '[PRIMARY]
			' END

	SELECT @vchrSQLStmt_Index2 = 
				CASE
					WHEN 
						@bool_DoCreateMissingOnly = 1 AND 
						EXISTS(SELECT TOP 1 1 FROM @tblSysIndexes_Audit WHERE Name = @vchrObjectName) THEN ''
					WHEN 
						@bool_DoCreateMissingOnly = 1 AND 
						EXISTS(SELECT TOP 1 1 FROM @tblSysIndexes_Audit WHERE Name = REPLACE(@vchrObjectName, '__', '_')) THEN ''
					ELSE @vchrSQLStmt
				END


	IF @bool_DoExec = 0 AND @bool_DoDebug = 1
	 BEGIN
		PRINT @vchrSQLStmt_CTbl
		PRINT @nvchrSQLStmt_View
		PRINT @vchrSQLStmt_ITrg
		PRINT @vchrSQLStmt_DTrg
		PRINT @vchrSQLStmt_UTrg
		PRINT @vchrSQLStmt_Index1
		PRINT @vchrSQLStmt_Index2
		EXEC @intError = dbo.sp_Audit_CreateAuditTriggers
						@vchr_TblName				= @vchr_TblName,
						@vchr_AuditDBName			= @vchr_AuditDBName,
						@vchr_DataDBName			= @vchr_DataDBName,
						@vchr_PKList				= @vchr_PKList,
						@vchr_TR_Insert_CustomSQL	= @vchr_TR_Insert_CustomSQL,
						@vchr_TR_Update_CustomSQL	= @vchr_TR_Update_CustomSQL,
						@vchr_TR_Delete_CustomSQL	= @vchr_TR_Delete_CustomSQL,
						@vchr_ColumnName_UpdatedBy	= @vchr_ColumnName_UpdatedBy,
						@vchr_ColumnName_UpdatedOn	= @vchr_ColumnName_UpdatedOn,
						@vchr_ColumnName_UpdatedByHost	= @vchr_ColumnName_UpdatedByHost,
						@bool_DoCreateMissingOnly	= @bool_DoCreateMissingOnly,
						@bool_DoDebug				= @bool_DoDebug, 
						@bool_DoExec				= @bool_DoExec
	 END

	IF @bool_DoExec = 1
	 BEGIN
		
		IF (@@TRANCOUNT = 0)
		 BEGIN
			BEGIN TRAN
			SELECT @intLocalTran = 1
		 END

		IF @intError = 0 
		 BEGIN
			IF @bool_DoDebug = 1 AND LEN(@vchrSQLStmt_GetMaxDate) > 0
			 BEGIN
				PRINT @vchrSQLStmt_GetMaxDate + 'GO' + @cnstNewLine
			 END

			EXEC @intError = dbo.sp_SQLExecute 
						@vchr_DBName = @vchr_AuditDBName,
						@vchr_SQLStmt = @vchrSQLStmt_GetMaxDate,
						@bool_DoDebug = 0,
						@bool_DoExec = @bool_DoExec
		 END

		IF @intError = 0 
		 BEGIN		
			IF @bool_DoDebug = 1 AND LEN(@vchrSQLStmt_CTbl) > 0
			 BEGIN
				PRINT @vchrSQLStmt_CTbl + 'GO' + @cnstNewLine
			 END

			EXEC @intError = dbo.sp_SQLExecute 
							@vchr_DBName = @vchr_AuditDBName,
							@vchr_SQLStmt = @vchrSQLStmt_CTbl,
							@bool_DoDebug = 0,
							@bool_DoExec = @bool_DoExec
		 END

		IF @intError = 0 
		 BEGIN
			IF @bool_DoDebug = 1 AND LEN(@nvchrSQLStmt_View) > 0
			 BEGIN
				PRINT @nvchrSQLStmt_View + 'GO' + @cnstNewLine
			 END

			EXEC @intError = dbo.sp_SQLExecute 
						@vchr_DBName = @vchr_AuditDBName,
						@vchr_SQLStmt = @nvchrSQLStmt_View,
						@bool_DoDebug = 0,
						@bool_DoExec = @bool_DoExec
		 END

		IF @intError = 0 
		 BEGIN
			IF @bool_DoDebug = 1 AND LEN(@vchrSQLStmt_ITrg) > 0
			 BEGIN
				PRINT @vchrSQLStmt_ITrg + 'GO' + @cnstNewLine
			 END

			EXEC @intError = dbo.sp_SQLExecute 
						@vchr_DBName = @vchr_AuditDBName,
						@vchr_SQLStmt = @vchrSQLStmt_ITrg,
						@bool_DoDebug = 0,
						@bool_DoExec = @bool_DoExec
		 END

		IF @intError = 0 
		 BEGIN
			IF @bool_DoDebug = 1 AND LEN(@vchrSQLStmt_UTrg) > 0
			 BEGIN
				PRINT @vchrSQLStmt_UTrg + 'GO' + @cnstNewLine
			 END

			EXEC @intError = dbo.sp_SQLExecute 
						@vchr_DBName = @vchr_AuditDBName,
						@vchr_SQLStmt = @vchrSQLStmt_UTrg,
						@bool_DoDebug = 0,
						@bool_DoExec = @bool_DoExec
		 END

		IF @intError = 0 
		 BEGIN
			IF @bool_DoDebug = 1 AND LEN(@vchrSQLStmt_DTrg) > 0
			 BEGIN
				PRINT @vchrSQLStmt_DTrg + 'GO' + @cnstNewLine
			 END
			
			EXEC @intError = dbo.sp_SQLExecute 
						@vchr_DBName = @vchr_AuditDBName,
						@vchr_SQLStmt = @vchrSQLStmt_DTrg,
						@bool_DoDebug = 0,
						@bool_DoExec = @bool_DoExec
		 END

		IF @intError = 0
		 BEGIN
			IF @bool_DoDebug = 1 AND LEN(@vchrSQLStmt_Index1) > 0
			 BEGIN
				PRINT @vchrSQLStmt_Index1 + 'GO' + @cnstNewLine
			 END
			
			EXEC @intError = dbo.sp_SQLExecute 
						@vchr_DBName = @vchr_AuditDBName,
						@vchr_SQLStmt = @vchrSQLStmt_Index1,
						@bool_DoDebug = 0,
						@bool_DoExec = @bool_DoExec
		 END
		 
		IF @intError = 0
		 BEGIN
			IF @bool_DoDebug = 1 AND LEN(@vchrSQLStmt_Index2) > 0
			 BEGIN
				PRINT @vchrSQLStmt_Index2 + 'GO' + @cnstNewLine
			 END
			
			EXEC @intError = dbo.sp_SQLExecute 
						@vchr_DBName = @vchr_AuditDBName,
						@vchr_SQLStmt = @vchrSQLStmt_Index2,
						@bool_DoDebug = 0,
						@bool_DoExec = @bool_DoExec
		 END

		IF @intError = 0
		 BEGIN
			EXEC @intError = dbo.sp_Audit_CreateAuditTriggers
						@vchr_TblName				= @vchr_TblName,
						@vchr_AuditDBName			= @vchr_AuditDBName,
						@vchr_DataDBName			= @vchr_DataDBName,
						@vchr_PKList				= @vchr_PKList,
						@bool_DoCreateMissingOnly	= @bool_DoCreateMissingOnly,
						@vchr_TR_Insert_CustomSQL	= @vchr_TR_Insert_CustomSQL,
						@vchr_TR_Update_CustomSQL	= @vchr_TR_Update_CustomSQL,
						@vchr_TR_Delete_CustomSQL	= @vchr_TR_Delete_CustomSQL,
						@vchr_ColumnName_UpdatedBy	= @vchr_ColumnName_UpdatedBy,
						@vchr_ColumnName_UpdatedOn	= @vchr_ColumnName_UpdatedOn,
						@vchr_ColumnName_UpdatedByHost	= @vchr_ColumnName_UpdatedByHost,
						@bool_DoDebug				= @bool_DoDebug, 
						@bool_DoExec				= @bool_DoExec
		 END

		IF (@intError = 0 AND @intLocalTran = 1)
		 BEGIN
			COMMIT TRAN
			IF @bool_DoDebug = 1
			 BEGIN
				PRINT 'TRANSACTION HAS BEEN COMMITED'
			 END
		 END
		ELSE IF (@intError != 0 AND @intLocalTran = 1)
		 BEGIN
			ROLLBACK TRAN
			IF @bool_DoDebug = 1
			 BEGIN
				PRINT 'TRANSACTION HAS BEEN ROLLEDBACK'
			 END
		 END
	 END

TheEnd:
	RETURN
END
GO

