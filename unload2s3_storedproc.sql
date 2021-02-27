CREATE OR REPLACE PROCEDURE call_ml_prediction(prediction_job String, table_name String)
  RETURNS String NOT null
  LANGUAGE javascript
  AS     
  $$
  try {
    var go_nogo_sql = "SELECT COUNT(*) FROM prediction_status WHERE status <> 'Completed'"
    var go_or_nogo = snowflake.createStatement( {sqlText: go_nogo_sql} ).execute()
    go_or_nogo.next()    
    var go_or_nogo_res = go_or_nogo.getColumnValue(1)
    
    if (go_or_nogo.getColumnValue(1) == 0) {
        var uuid_sql = "CALL get_predictionid('" + TABLE_NAME + "')"
        var uuid_exec = snowflake.createStatement( {sqlText: uuid_sql} ).execute()
        uuid_exec.next() 
        var uuid = uuid_exec.getColumnValue(1)
        
        var unload_sql = "COPY INTO @unload_onto_s3/" + uuid + ".csv" + " FROM " + TABLE_NAME + " " +
                         "max_file_size = 4900000000 \
                         single = true \
                         overwrite = true \
                         header = true;"        
        var unload_res = snowflake.createStatement({sqlText: unload_sql}).execute()  
        unload_res.next()
        
      if (unload_res.getColumnValue(1) > 0) {
        var sql = `INSERT INTO prediction_status (predictionid, prediction_job, input_table, status) VALUES (:1, :2, :3, 'Submitted');` ;
        var execute = snowflake.execute({
            sqlText: sql,
            binds: [uuid, PREDICTION_JOB, TABLE_NAME] 
        })
        return uuid
      } else {
        return "Error: Unsuccessful Unload"
      }
    } else {
        return "Error: You may online run 1 prediction running at any given time"
    }
  } catch (err) {
    return "Procedure Error: " + err
  }    
  $$
