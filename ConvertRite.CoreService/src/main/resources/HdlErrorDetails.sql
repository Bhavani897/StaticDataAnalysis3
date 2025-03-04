SELECT DISTINCT err.orderby,
                err.err_location,
                err.ucm_content_id,
				err.data_file_name,
                err.message_type,
                err.msg_text,
                err.stack_trace,
                err.ui_user_key,
                err.metadata,
                err.file_line,
                err.request_id
FROM            (
                       SELECT 1          orderby ,
                              'Zip file' err_location ,
                              ds.ucm_content_id ,
							  bo.data_file_name,
                              l.message_type ,
                              l.msg_text ,
                              l.stack_trace ,
                              To_number(NULL) seq_num ,
                              ''              ui_user_key ,
                              ''              metadata ,
                              ''              file_line ,
                              ds.request_id
                       FROM   fusion.hrc_dl_message_lines l ,
					          fusion.hrc_dl_data_set_bus_objs bo ,
                              fusion.hrc_dl_data_sets ds
                       WHERE  l.message_source_table_name = 'HRC_DL_DATA_SETS'
                       AND    l.message_source_line_id = ds.data_set_id
					   AND    bo.data_set_bus_obj_id = l.data_set_bus_obj_id
                       AND    ds.data_set_id = bo.data_set_id
                       UNION
                       SELECT 2          orderby ,
                              'Dat file' err_location ,
                              ds.ucm_content_id ,
							  bo.data_file_name,
                              l.message_type ,
                              l.msg_text ,
                              l.stack_trace ,
                              To_number('') seq_num ,
                              ''            ui_user_key ,
                              ''            metadata ,
                              ''            file_line ,
                              ds.request_id
                       FROM   fusion.hrc_dl_message_lines l ,
                              fusion.hrc_dl_data_set_bus_objs bo ,
                              fusion.hrc_dl_data_sets ds
                       WHERE  l.message_source_table_name = 'HRC_DL_DATA_SET_BUS_OBJS'
                       AND    bo.data_set_bus_obj_id = l.data_set_bus_obj_id
                       AND    ds.data_set_id = bo.data_set_id
                       UNION
                       SELECT 3                        orderby ,
                              'File line not imported' err_location ,
                              ds.ucm_content_id ,
							  bo.data_file_name,
                              l.message_type ,
                              l.msg_text ,
                              l.stack_trace ,
                              fl.seq_num ,
                              ''      ui_user_key ,
                              ''      metadata ,
                              fl.text file_line ,
                              ds.request_id
                       FROM   fusion.hrc_dl_message_lines l ,
                              fusion.hrc_dl_data_set_bus_objs bo ,
                              fusion.hrc_dl_data_sets ds ,
                              fusion.hrc_dl_file_lines fl
                       WHERE  l.message_source_table_name = 'HRC_DL_FILE_LINES'
                       AND    bo.data_set_bus_obj_id = l.data_set_bus_obj_id
                       AND    ds.data_set_id = bo.data_set_id
                       AND    fl.line_id = l.message_source_line_id
                       UNION
                       -- File Headers
                       SELECT 4          orderby ,
                              'METADATA' err_location ,
                              ds.ucm_content_id ,
							  bo.data_file_name,
                              l.message_type ,
                              l.msg_text ,
                              l.stack_trace ,
                              fl.seq_num ,
                              ''      ui_user_key ,
                              fl.text metadata ,
                              ''      file_line ,
                              ds.request_id
                       FROM   fusion.hrc_dl_message_lines l ,
                              fusion.hrc_dl_data_set_bus_objs bo ,
                              fusion.hrc_dl_data_sets ds ,
                              fusion.hrc_dl_file_headers fh ,
                              fusion.hrc_dl_file_lines fl
                       WHERE  l.message_source_table_name = 'HRC_DL_FILE_HEADERS'
                       AND    bo.data_set_bus_obj_id = l.data_set_bus_obj_id
                       AND    ds.data_set_id = bo.data_set_id
                       AND    fh.header_id = l.message_source_line_id
                       AND    fl.line_id = fh.line_id
                       UNION
                       -- file rows
                       SELECT 5           orderby ,
                              'Hierarchy' err_location ,
                              ds.ucm_content_id ,
							  bo.data_file_name,
                              l.message_type ,
                              l.msg_text ,
                              l.stack_trace ,
                              fl.seq_num ,
                              '' ui_user_key ,
                              (
                                     SELECT hl.text
                                     FROM   fusion.hrc_dl_file_lines hl ,
                                            fusion.hrc_dl_file_headers fh
                                     WHERE  fh.header_id= fr.header_id
                                     AND    hl.line_id = fh.line_id) metadata ,
                              fl.text                                file_line ,
                              ds.request_id
                       FROM   fusion.hrc_dl_message_lines l ,
                              fusion.hrc_dl_data_set_bus_objs bo ,
                              fusion.hrc_dl_data_sets ds ,
                              fusion.hrc_dl_file_rows fr ,
                              fusion.hrc_dl_file_lines fl
                       WHERE  l.message_source_table_name LIKE 'HRC_DL_FILE_ROWS'
                       AND    bo.data_set_bus_obj_id = l.data_set_bus_obj_id
                       AND    ds.data_set_id = bo.data_set_id
                       AND    fr.row_id = l.message_source_line_id
                       AND    fl.line_id = fr.line_id
                       UNION
                       SELECT 6                orderby ,
                              'Logical object' err_location ,
                              ds.ucm_content_id ,
							  bo.data_file_name,
                              l.message_type ,
                              l.msg_text ,
                              l.stack_trace ,
                              fl.seq_num ,
                              ll.ui_user_key ui_user_key ,
                              (
                                     SELECT hl.text
                                     FROM   fusion.hrc_dl_file_lines hl ,
                                            fusion.hrc_dl_file_headers fh
                                     WHERE  fh.header_id= fr.header_id
                                     AND    hl.line_id = fh.line_id) metadata ,
                              fl.text                                file_line ,
                              ds.request_id
                       FROM   fusion.hrc_dl_message_lines l ,
                              fusion.hrc_dl_data_set_bus_objs bo ,
                              fusion.hrc_dl_data_sets ds ,
                              fusion.hrc_dl_logical_lines ll ,
                              fusion.hrc_dl_file_rows fr ,
                              fusion.hrc_dl_file_lines fl
                       WHERE  l.message_source_table_name = 'HRC_DL_LOGICAL_LINES'
                       AND    bo.data_set_bus_obj_id = l.data_set_bus_obj_id
                       AND    ds.data_set_id = bo.data_set_id
                       AND    ll.logical_line_id = l.message_source_line_id
                       AND    fr.logical_line_id = ll.logical_line_id
                       AND    fl.line_id = fr.line_id
                       UNION
                       SELECT 7              orderby ,
                              'Physical Row' err_location ,
                              ds.ucm_content_id ,
							  bo.data_file_name,
                              l.message_type ,
                              l.msg_text ,
                              l.stack_trace ,
                              fl.seq_num ,
                              pl.ui_user_key
                                     ||' '
                                     ||pl.ui_date_from
                                     ||' '
                                     ||pl.ui_date_to ui_user_key ,
                              (
                                     SELECT hl.text
                                     FROM   fusion.hrc_dl_file_lines hl ,
                                            fusion.hrc_dl_file_headers fh
                                     WHERE  fh.header_id= fr.header_id
                                     AND    hl.line_id = fh.line_id) metadata ,
                              fl.text                                file_line ,
                              ds.request_id
                       FROM   fusion.hrc_dl_message_lines l ,
                              fusion.hrc_dl_data_set_bus_objs bo ,
                              fusion.hrc_dl_data_sets ds ,
                              fusion.hrc_dl_physical_lines pl ,
                              fusion.hrc_dl_file_rows fr ,
                              fusion.hrc_dl_file_lines fl
                       WHERE  l.message_source_table_name = 'HRC_DL_PHYSICAL_LINES'
                       AND    bo.data_set_bus_obj_id = l.data_set_bus_obj_id
                       AND    ds.data_set_id = bo.data_set_id
                       AND    pl.physical_line_id = l.message_source_line_id
                       AND    fr.row_id = pl.row_id
                       AND    fl.line_id = fr.line_id ) err
WHERE err.ucm_content_id IN nvl('{0}', err.ucm_content_id)
and err.msg_text like nvl(:errorText, '%')