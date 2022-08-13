USE [DBA]
GO

/****** Object:  UserDefinedFunction [dbo].[fnSplit]    Script Date: 8/13/2022 10:04:31 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[fnSplit]
(
    @sInputList VARCHAR(8000), -- List of delimited items
	@sDelimiter VARCHAR(8000) = ',' -- delimiter that separates items
) 
RETURNS @List TABLE 
		(
			RecId INT IDENTITY(1, 1), 
			item VARCHAR(8000)
		)

BEGIN
	DECLARE @sItem VARCHAR(8000)
	WHILE CHARINDEX(@sDelimiter,@sInputList,0) <> 0
	 BEGIN
		SELECT
			@sItem = RTRIM(LTRIM(SUBSTRING(@sInputList,1,CHARINDEX(@sDelimiter,@sInputList,0)-1))),
			@sInputList = RTRIM(LTRIM(SUBSTRING(@sInputList,CHARINDEX(@sDelimiter,@sInputList,0)+LEN(@sDelimiter),LEN(@sInputList))))
	 
		IF LEN(@sItem) > 0
		 BEGIN
			INSERT INTO @List (item)
			 SELECT @sItem
		 END
	 END

	IF LEN(@sInputList) > 0
	 BEGIN
		INSERT INTO @List (item)
			SELECT @sInputList -- Put the last item in
	 END
	RETURN
END
GO


