USE [DBA]
GO

/****** Object:  StoredProcedure [dbo].[sp_Audit_CreateAuditTriggers]    Script Date: 8/3/2022 10:14:43 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[sp_Audit_CreateAuditTriggers]
(
	@vchr_TblName				VARCHAR(300),
	@vchr_AuditDBName			VARCHAR(50),
	@vchr_DataDBName			VARCHAR(50),
	@vchr_PKList				VARCHAR(MAX)	= NULL,
	@bool_DoCreateMissingOnly	BIT				= 0,
	@vchr_TR_Insert_CustomSQL	VARCHAR(MAX)	= NULL,
	@vchr_TR_Update_CustomSQL	VARCHAR(MAX)	= NULL,
	@vchr_TR_Delete_CustomSQL	VARCHAR(MAX)	= NULL,
	@vchr_ColumnName_UpdatedBy	VARCHAR(300)	= NULL,
	@vchr_ColumnName_UpdatedOn	VARCHAR(300)	= NULL,
	@vchr_ColumnName_UpdatedByHost	VARCHAR(300)	= NULL,
	@bool_SupportForUpdatedColumnsOnly	BIT			= 0,
	@bool_DoDebug				BIT				= 1,
	@bool_DoExec				BIT				= 1
)
AS
-- =============================================
-- Author:		Mikhail Peterburgskiy
-- Description:	Creates missing triggers for Audit purpose
-- =============================================
SET NOCOUNT ON
BEGIN

	DECLARE
		@vchrTblName		VARCHAR(300),
		@vchrTblName_Adt	VARCHAR(300),
		@vchrSQLStmt		VARCHAR(MAX),
		@vchrSQLStmt_ITrg	VARCHAR(MAX),
		@vchrSQLStmt_UTrg	VARCHAR(MAX),
		@vchrSQLStmt_DTrg	VARCHAR(MAX),
		@vchrColumnList		VARCHAR(MAX),
		@vchrColumnList_Upd	VARCHAR(MAX),
		@vchrPKList			VARCHAR(MAX),
		@cnstNewLine		CHAR(2),
		@intError			INT,
		@intLocalTran		BIT,
		@intExecStatus		INT,
		@vchrObjectName		VARCHAR(500),
		@boolIsInPlaceAudit	BIT

	SELECT
		@vchrTblName		= @vchr_TblName,
		@vchrTblName_Adt	= @vchrTblName + '_AUDIT',
		@vchrSQLStmt		= '',
		@cnstNewLine		= CHAR(9) + CHAR(10),
		@intExecStatus		= -1,
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

	SELECT
		@vchr_TR_Insert_CustomSQL	= ISNULL(@vchr_TR_Insert_CustomSQL, ''),
		@vchr_TR_Update_CustomSQL	= ISNULL(@vchr_TR_Update_CustomSQL, ''),
		@vchr_TR_Delete_CustomSQL	= ISNULL(@vchr_TR_Delete_CustomSQL, '')

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

	IF (@vchr_PKList IS NULL)
	 BEGIN
		SELECT @vchrSQLStmt = 'EXEC [' + @vchr_DataDBName + '].dbo.sp_helpindex ''' + @vchr_TblName + ''''

		INSERT INTO @tblIndex
		EXEC (@vchrSQLStmt)

		DELETE @tblIndex WHERE indexName NOT LIKE 'PK_%'

		IF ( (SELECT Count(*) FROM @tblIndex) != 1)
		 BEGIN
			RAISERROR ('Table does not have PK index or has more than one, please adjust table or specify one.', 15, 1)
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

	-- collect table columns
	SELECT @vchrColumnList	= ''
	SELECT
		@vchrColumnList = @vchrColumnList + '		[' + c.name + '], ' + @cnstNewLine
	 FROM
		@tblSysObjects o
		INNER JOIN @tblSysColumns c
		 ON (c.object_id = o.object_id)
		INNER JOIN @tblSysTypes t
		 ON (t.user_type_id = c.user_type_id)
	 WHERE
		o.name = @vchrTblName AND      
		t.name NOT IN ('image', 'text', 'ntext')
	 ORDER BY
		column_id

	SELECT @vchrColumnList = LEFT(@vchrColumnList, LEN(@vchrColumnList) - 2 - LEN(@cnstNewLine))
	SELECT @vchrColumnList_Upd = REPLACE(REPLACE(REPLACE(@vchrColumnList, 
		'[' + ISNULL(@vchr_ColumnName_UpdatedBy, 'UpdatedBy') + ']', 'SUSER_SNAME()'), 
		'[' + ISNULL(@vchr_ColumnName_UpdatedByHost, 'UpdatedByHost') + ']', 'HOST_NAME()'), 
		'[' + ISNULL(@vchr_ColumnName_UpdatedOn, 'UpdatedOn') + ']', 'GETDATE()')
	
	-- create delete trigger
	SELECT 
		@vchrObjectName = 'TR_' + @vchrTblName + '_DELETE',
		@vchrSQLStmt = 
			'CREATE TRIGGER [dbo].[' + @vchrObjectName + '] ' + @cnstNewLine +
			'	ON [dbo].[' + @vchrTblName + '] ' +  @cnstNewLine +
			'	FOR DELETE ' +  @cnstNewLine +
			'AS  ' +  @cnstNewLine +
			'SET NOCOUNT ON ' +  @cnstNewLine +
			'BEGIN ' +  @cnstNewLine + @cnstNewLine +
			'-- **** Custom Code **** -- '  + @cnstNewLine +
			'' + @vchr_TR_Delete_CustomSQL + @cnstNewLine +
			'-- ********************* -- ' + @cnstNewLine + @cnstNewLine +
			'' +  @cnstNewLine +
			'	INSERT INTO [' + @vchr_AuditDBName + '].[' + CASE WHEN @boolIsInPlaceAudit = 1 THEN 'audit' ELSE 'dbo' END + '].[' + @vchrTblName_Adt + ']' + @cnstNewLine +
			'	(' + @cnstNewLine +
				@vchrColumnList + ', ' + @cnstNewLine +
			'		AuditAction' + @cnstNewLine +
			'	)' + @cnstNewLine +
			'	SELECT ' + @cnstNewLine +
				@vchrColumnList_Upd + ', ' + @cnstNewLine +
			'		''D''' + @cnstNewLine +
			'	 FROM ' + @cnstNewLine +
			'		DELETED' + @cnstNewLine +
			'' + @cnstNewLine +
			'END '

	SELECT @vchrSQLStmt_DTrg = 
				CASE
					WHEN @bool_SupportForUpdatedColumnsOnly = 1 THEN ''
					WHEN @bool_DoCreateMissingOnly = 1 AND OBJECT_ID(@vchr_DataDBName + '.[dbo].' + @vchrObjectName) IS NOT NULL THEN ''
					ELSE @vchrSQLStmt
				END

	-- create update trigger
	SELECT @vchrSQLStmt = ''
	SELECT 
		@vchrSQLStmt = @vchrSQLStmt + ' l.[' + item + '] = i.[' + item + '] AND '
	 FROM
		dbo.fnSplit(@vchrPKList, ',')
	SELECT @vchrSQLStmt = LEFT(@vchrSQLStmt, LEN(@vchrSQLStmt) - 4)
	SELECT 
		@vchrObjectName = 'TR_' + @vchrTblName + '_UPDATE',
		@vchrSQLStmt = 
			'CREATE TRIGGER [dbo].[' + @vchrObjectName + '] ' + @cnstNewLine +
			'	ON [dbo].[' + @vchrTblName + '] ' +  @cnstNewLine +
			'	FOR UPDATE ' +  @cnstNewLine +
			'AS  ' +  @cnstNewLine +
			'SET NOCOUNT ON ' +  @cnstNewLine +
			'BEGIN ' +  @cnstNewLine +@cnstNewLine +
			'-- **** Custom Code **** -- '  + @cnstNewLine +
			'' + @vchr_TR_Update_CustomSQL + @cnstNewLine +
			'-- ********************* -- ' + @cnstNewLine + @cnstNewLine +
			'' +  @cnstNewLine +
			CASE
				WHEN @vchrColumnList_Upd != @vchrColumnList THEN
			'	UPDATE ' + @cnstNewLine +
			'		l ' + @cnstNewLine +
			'	 SET ' + @cnstNewLine +
			'		' + ISNULL(@vchr_ColumnName_UpdatedBy, 'UpdatedBy') + ' = SUSER_SNAME(), ' + @cnstNewLine +
			'		' + ISNULL(@vchr_ColumnName_UpdatedByHost + ' = HOST_NAME(), ' + @cnstNewLine, '') +
			'		' + ISNULL(@vchr_ColumnName_UpdatedOn, 'UpdatedOn') + ' = GETDATE() ' + @cnstNewLine +
			'	 FROM ' + @cnstNewLine +
			'		dbo.[' + @vchrTblName + '] l ' + @cnstNewLine +
			'		INNER JOIN INSERTED i  ' + @cnstNewLine +
			'		 ON ' + @vchrSQLStmt + ' ' + @cnstNewLine
				ELSE ''
			END +
			CASE
				WHEN @bool_SupportForUpdatedColumnsOnly = 0 THEN
			'' +  @cnstNewLine +
			'	INSERT INTO [' + @vchr_AuditDBName + '].[' + CASE WHEN @boolIsInPlaceAudit = 1 THEN 'audit' ELSE 'dbo' END + '].[' + @vchrTblName_Adt + ']' + @cnstNewLine +
			'	(' + @cnstNewLine +
				@vchrColumnList + ', ' + @cnstNewLine +
			'		AuditAction' + @cnstNewLine +
			'	)' + @cnstNewLine +
			'	SELECT ' + @cnstNewLine +
				@vchrColumnList_Upd + ', ' + @cnstNewLine +
			'		''U''' + @cnstNewLine +
			'	 FROM ' + @cnstNewLine +
			'		INSERTED' + @cnstNewLine +
			'' + @cnstNewLine
				ELSE ''
			END +
			'END '

	SELECT @vchrSQLStmt_UTrg = 
				CASE
					WHEN @bool_DoCreateMissingOnly = 1 AND OBJECT_ID(@vchr_DataDBName + '.[dbo].' + @vchrObjectName) IS NOT NULL THEN ''
					ELSE @vchrSQLStmt
				END

	-- create insert trigger
	SELECT 
		@vchrObjectName = 'TR_' + @vchrTblName + '_INSERT',
		@vchrSQLStmt = 
			'CREATE TRIGGER [dbo].[' + @vchrObjectName + '] ' + @cnstNewLine +
			'	ON [dbo].[' + @vchrTblName + '] ' +  @cnstNewLine +
			'	FOR INSERT ' +  @cnstNewLine +
			'AS  ' +  @cnstNewLine +
			'SET NOCOUNT ON ' +  @cnstNewLine +
			'BEGIN ' +  @cnstNewLine + @cnstNewLine +
			'-- **** Custom Code **** -- '  + @cnstNewLine +
			'' + @vchr_TR_Insert_CustomSQL + @cnstNewLine +
			'-- ********************* -- ' + @cnstNewLine + @cnstNewLine +
			'' +  @cnstNewLine +
			'	INSERT INTO [' + @vchr_AuditDBName + '].[' + CASE WHEN @boolIsInPlaceAudit = 1 THEN 'audit' ELSE 'dbo' END + '].[' + @vchrTblName_Adt + ']' + @cnstNewLine +
			'	(' + @cnstNewLine +
				@vchrColumnList + ', ' + @cnstNewLine +
			'		AuditAction' + @cnstNewLine +
			'	)' + @cnstNewLine +
			'	SELECT ' + @cnstNewLine +
				@vchrColumnList_Upd + ', ' + @cnstNewLine +
			'		''I''' + @cnstNewLine +
			'	 FROM ' + @cnstNewLine +
			'		INSERTED' + @cnstNewLine +
			'' + @cnstNewLine +
			'END '

	SELECT @vchrSQLStmt_ITrg = 
				CASE
					WHEN @bool_SupportForUpdatedColumnsOnly = 1 THEN ''
					WHEN @bool_DoCreateMissingOnly = 1 AND OBJECT_ID(@vchr_DataDBName + '.[dbo].' + @vchrObjectName) IS NOT NULL THEN ''
					ELSE @vchrSQLStmt
				END

	IF (@bool_DoDebug = 1 AND @bool_DoExec = 0)
	 BEGIN
		PRINT @vchrSQLStmt_ITrg + @cnstNewLine + 'GO'
		PRINT @vchrSQLStmt_UTrg + @cnstNewLine + 'GO'
		PRINT @vchrSQLStmt_DTrg + @cnstNewLine + 'GO'
	 END
	ELSE IF (@bool_DoExec = 1)
	 BEGIN

		IF (@@TRANCOUNT = 0)
		 BEGIN
			BEGIN TRAN
			SELECT @intLocalTran = 1
		 END

		IF (@bool_DoDebug = 1)
		 BEGIN
			PRINT @vchrSQLStmt_ITrg + @cnstNewLine + 'GO'
		 END		

		EXEC @intError = dbo.sp_SQLExecute 
						@vchr_DBName = @vchr_DataDBName,
						@vchr_SQLStmt = @vchrSQLStmt_ITrg,
						@bool_DoDebug = 0,
						@bool_DoExec = @bool_DoExec

		IF @intError = 0
		 BEGIN
			IF (@bool_DoDebug = 1)
			 BEGIN
				PRINT @vchrSQLStmt_UTrg + @cnstNewLine + 'GO'
			 END		

			EXEC @intError = dbo.sp_SQLExecute 
						@vchr_DBName = @vchr_DataDBName,
						@vchr_SQLStmt = @vchrSQLStmt_UTrg,
						@bool_DoDebug = 0,
						@bool_DoExec = @bool_DoExec

		 END
	
		IF @intError = 0
		 BEGIN
			IF (@bool_DoDebug = 1)
			 BEGIN
				PRINT @vchrSQLStmt_DTrg + @cnstNewLine + 'GO'
			 END		

			EXEC @intError = dbo.sp_SQLExecute 
						@vchr_DBName = @vchr_DataDBName,
						@vchr_SQLStmt = @vchrSQLStmt_DTrg,
						@bool_DoDebug = 0,
						@bool_DoExec = @bool_DoExec
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

		SELECT @intExecStatus = @intError

	 END

TheEnd:
	RETURN @intExecStatus

END
GO

