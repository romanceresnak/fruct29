CREATE OR REPLACE PIPE predictionpipe auto_ingest=true AS
  COPY INTO prediction_result
  FROM (
    SELECT 
        SPLIT_PART(REPLACE(metadata$filename, '.csv', ''), '/', -1)
        , metadata$file_row_number
        , $1 
        , TO_TIMESTAMP_LTZ(current_timestamp)
    FROM @snowflake2sagemakerstage/snowflake
  )
  file_format = (format_name = mycsvunloadformat); 