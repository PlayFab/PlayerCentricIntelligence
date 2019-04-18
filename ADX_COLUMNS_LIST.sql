DECLARE @tableSchema NVARCHAR(100) = N'@item().TABLE_SCHEMA'
DECLARE @tableName NVARCHAR(100) = N'@item().TABLE_NAME'

DECLARE @colList NVARCHAR(MAX)
SET @colList = N''
DECLARE @i INT = 1
DECLARE @rowCount INT = (
	SELECT MAX(ORDINAL_POSITION) 
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE 
            TABLE_SCHEMA = @tableSchema
		AND	TABLE_NAME = @tableName
)


WHILE @i <= @rowCount
	BEGIN

		SET @colList = @colList + (

			SELECT 
				CASE WHEN ORDINAL_POSITION > 1 THEN ', ' ELSE '' END 
				+ '[''' + COLUMN_NAME + ''']'	--TL: Need to delimit in case column names match reserved words
				+ ': '
				+ CASE 
					WHEN DATA_TYPE LIKE 'date%'
						THEN 'datetime'
					WHEN DATA_TYPE LIKE '%char%'
						THEN 'string'
					WHEN DATA_TYPE = 'bigint'
						THEN 'long'
					WHEN DATA_TYPE LIKE '%int%'
						THEN 'int'
					WHEN DATA_TYPE = 'bit'
						THEN 'bool'
					WHEN DATA_TYPE IN ('decimal', 'numeric')
						THEN 'decimal'
					WHEN DATA_TYPE IN ('floatl', 'real')
						THEN 'real'
					ELSE 'string'
				END 
    
			FROM INFORMATION_SCHEMA.COLUMNS
			WHERE 
					ORDINAL_POSITION = @i
				AND TABLE_SCHEMA = @tableSchema
				AND TABLE_NAME = @tableName
		
		)

		SET @i += 1

	END

SELECT @colList AS COL_LIST