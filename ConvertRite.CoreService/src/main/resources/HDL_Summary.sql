SELECT summry.KEY,
summry.ucm_content_id,
summry.data_set_name,
summry.imported_status,
summry.transfer_status,
summry.batch_submission_date,
summry.import_lines_total_count,
summry.import_error_count,
summry.loaded_count,
summry.error_count
FROM(SELECT '1' KEY, ucm_content_id, data_set_name, imported_status,
       transfer_status,
       TO_CHAR (ds.last_update_date,
                'dd-mon-rrrr hh24:mi:ss'
               ) batch_submission_date,
       import_lines_total_count, import_error_count, loaded_count,
       error_count
  FROM hrc_dl_data_sets ds
 WHERE ucm_content_id =  '{0}') summry

