CREATE OR REPLACE PROCEDURE populate_prediction_result()
  RETURNS String
  LANGUAGE javascript
  EXECUTE AS caller
  AS     
  $$
  try {
      var get_pred_info_sql = "SELECT predictionid, input_table \
                                FROM prediction_status \
                                WHERE predictionid IN (SELECT predictionid FROM prediction_result_stream ORDER BY insert_datetime desc LIMIT 1)"
      var get_pred_info_sql_exec = snowflake.createStatement({sqlText: get_pred_info_sql}).execute() 
      
      if (get_pred_info_sql_exec.next())  {
          var predictionid = get_pred_info_sql_exec.getColumnValue(1)      
          var table_name = get_pred_info_sql_exec.getColumnValue(2)

          var set_task_variable = "CALL system$set_return_value(:1)"
          var set_task_variable_exec = snowflake.execute({
              sqlText: set_task_variable,
              binds: [predictionid] 
          })
          
          var updt_pred_input_tbl = "UPDATE " + table_name + " t1" + " " +
                                        "SET t1.prediction = t2.prediction \
                                     FROM prediction_result_stream t2 \
                                     WHERE \
                                        t1.PREDICTION_SEQ = t2.PREDICTION_SEQ \
                                        AND t1.PREDICTIONID = t2.PREDICTIONID"                                     
          snowflake.createStatement({sqlText: updt_pred_input_tbl}).execute()
          return table_name + " Updated Successfully!"                                        
      } else {
        return "Error: There is no record in prediction_status for this stream"
      }      
  } catch (err) {
      return "Error: " + err
  } 
  $$
;

CREATE OR REPLACE TASK populate_prediction_result
  WAREHOUSE = WH
  SCHEDULE = '5 MINUTE'
WHEN
  SYSTEM$STREAM_HAS_DATA('prediction_result_stream')
AS
    CALL populate_prediction_result();