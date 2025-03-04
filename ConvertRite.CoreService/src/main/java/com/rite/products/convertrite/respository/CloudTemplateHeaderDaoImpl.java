package com.rite.products.convertrite.respository;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import javax.persistence.EntityManager;
import javax.persistence.ParameterMode;
import javax.persistence.PersistenceContext;
import javax.persistence.Query;
import javax.persistence.StoredProcedureQuery;
import javax.servlet.http.HttpServletResponse;

import com.rite.products.convertrite.po.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Repository;

import com.jcraft.jsch.ChannelSftp;
import com.opencsv.CSVWriter;
import com.rite.products.convertrite.model.XxrCloudTemplateHeader;
import com.rite.products.convertrite.utils.DataSourceUtil;
import com.rite.products.convertrite.utils.Utils;

import static com.rite.products.convertrite.utils.Utils.clobToString;

@Repository
public class CloudTemplateHeaderDaoImpl {

	@PersistenceContext
	private EntityManager entityManager;

	/*
	 * @Value("${datasource.hostname}") private String datasourceHostName;
	 *
	 * @Value("${datasource.port}") private int datasourcePort;
	 *
	 * @Value("${spring.datasource.username}") private String datasourceUserName;
	 *
	 * @Value("${spring.datasource.password}") private String datasourcePassword;
	 *
	 * @Value("${datasource.name}") private String dataSourceName;
	 */

	@Value("${file.upload-dir}")
	private String fileUploadDir;
	@Autowired
	Utils utils;
	@Autowired
	DataSourceUtil dataSourceUtil;

	private static final Logger log = LoggerFactory.getLogger(CloudTemplateHeaderDaoImpl.class);

	@SuppressWarnings("unchecked")
	public List<XxrCloudTemplateHeader> getCloudTemplate(CloudTemplatePo cloudTemplatePo) {

		List<XxrCloudTemplateHeader> list = new ArrayList<>();
		try {
			StringBuilder sqlBuilder = new StringBuilder().append("select x from XxrCloudTemplateHeader x");

			if (!isNullOrEmpty(cloudTemplatePo.getObjectCode()))
				sqlBuilder.append(" where x.objectCode = :objectCode");
			if (cloudTemplatePo.getTemplateId() != null)
				sqlBuilder.append(" and x.templateId = :templateId");
			if (cloudTemplatePo.getPodId() != null)
				sqlBuilder.append(" and x.podId = :podId");
			if (cloudTemplatePo.getBu() != null)
				sqlBuilder.append(" and x.bu = :bu");
			if (!isNullOrEmpty(cloudTemplatePo.getBuSpecific()))
				sqlBuilder.append(" and x.buSpecific = :buSpecific");
			if (!isNullOrEmpty(cloudTemplatePo.getCloudDataTableName()))
				sqlBuilder.append(" and x.cloudDataTableName = :cloudDataTableName");
			if (!isNullOrEmpty(cloudTemplatePo.getParentObjectCode()))
				sqlBuilder.append(" and x.parentObjectCode = :parentObjectCode");
			if (!isNullOrEmpty(cloudTemplatePo.getProjectName()))
				sqlBuilder.append(" and x.projectName = :projectName");
			if (!isNullOrEmpty(cloudTemplatePo.getSourceHeaderTemplate()))
				sqlBuilder.append(" and x.sourceHeaderTemplate = :sourceHeaderTemplate");
			if (!isNullOrEmpty(cloudTemplatePo.getTableName()))
				sqlBuilder.append(" and x.tableName LIKE :tableName");
			if (!isNullOrEmpty(cloudTemplatePo.getTemplateName()))
				sqlBuilder.append(" and x.templateName LIKE :templateName");
			if (!isNullOrEmpty(cloudTemplatePo.getTemplateType()))
				sqlBuilder.append(" and x.templateType = :templateType");
			if (!isNullOrEmpty(cloudTemplatePo.getViewName()))
				sqlBuilder.append(" and x.viewName LIKE :viewName");
			String sql = sqlBuilder.toString();

			/* EntityManager en = em.createEntityManager(); */
			Query query = entityManager.createQuery(sql);

			if (!isNullOrEmpty(cloudTemplatePo.getObjectCode()))
				query.setParameter("objectCode", cloudTemplatePo.getObjectCode());
			if (cloudTemplatePo.getTemplateId() != null)
				query.setParameter("templateId", cloudTemplatePo.getTemplateId());
			if (cloudTemplatePo.getPodId() != null)
				query.setParameter("podId", cloudTemplatePo.getPodId());
			if (cloudTemplatePo.getBu() != null)
				query.setParameter("bu", cloudTemplatePo.getBu());
			if (!isNullOrEmpty(cloudTemplatePo.getBuSpecific()))
				query.setParameter("buSpecific", cloudTemplatePo.getBuSpecific());
			if (!isNullOrEmpty(cloudTemplatePo.getCloudDataTableName()))
				query.setParameter("cloudDataTableName", cloudTemplatePo.getCloudDataTableName());
			if (!isNullOrEmpty(cloudTemplatePo.getParentObjectCode()))
				query.setParameter("parentObjectCode", cloudTemplatePo.getParentObjectCode());
			if (!isNullOrEmpty(cloudTemplatePo.getProjectName()))
				query.setParameter("projectName", cloudTemplatePo.getProjectName());
			if (!isNullOrEmpty(cloudTemplatePo.getSourceHeaderTemplate()))
				query.setParameter("sourceHeaderTemplate", cloudTemplatePo.getSourceHeaderTemplate());
			if (!isNullOrEmpty(cloudTemplatePo.getTableName()))
				query.setParameter("tableName", "%" + cloudTemplatePo.getTableName() + "%");
			if (!isNullOrEmpty(cloudTemplatePo.getTemplateName()))
				query.setParameter("templateName", "%" + cloudTemplatePo.getTemplateName() + "%");
			if (!isNullOrEmpty(cloudTemplatePo.getTemplateType()))
				query.setParameter("templateType", cloudTemplatePo.getTemplateType());
			if (!isNullOrEmpty(cloudTemplatePo.getViewName()))
				query.setParameter("viewName", "%" + cloudTemplatePo.getViewName() + "%");

			list = query.getResultList();
		} catch (Exception e) {
			log.error(e.getMessage());
		}

		entityManager.clear();
		entityManager.close();
		return list;
	}

	private boolean isNullOrEmpty(String string) {
		return string == null || string.isEmpty();
	}

	public void generateFbdi(String tableName, PrintWriter writer) throws Exception {
		log.info("Start of generateFbdi Method in DaoImpl######");
		ResultSet rs = null;
		Connection con = null;
		try {
			String sql = "select" + " *" + " from " + tableName;

			con = dataSourceUtil.createConnection();

			// step3 create the statement object
			PreparedStatement stmt = con.prepareStatement(sql);

			// step4 execute query
			rs = stmt.executeQuery();

			CSVWriter csvWriter = new CSVWriter(writer, CSVWriter.DEFAULT_SEPARATOR, CSVWriter.NO_QUOTE_CHARACTER,
					CSVWriter.NO_ESCAPE_CHARACTER, CSVWriter.DEFAULT_LINE_END);

			csvWriter.writeAll(rs, false);
			csvWriter.flush();
			csvWriter.close();

		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e.getMessage());
		} finally {
			if (con != null)
				con.close();
		}
	}

	public void generateFbdiFromLob(Long cloudTemplateId, PrintWriter writer) throws Exception {
		log.info("Start of generateFbdi Method in DaoImpl######");
		ResultSet rs = null;
		Connection con = null;
		try {
			long startTime = System.currentTimeMillis();
			log.info("startTime:::::" + startTime);
			String sql = "select FBDI_FILEGEN_TEST(" + cloudTemplateId + ")" + " from dual";
			log.info(sql);
			con = dataSourceUtil.createConnection();
			// con = dataSource.getConnection();

			// step3 create the statement object
			PreparedStatement stmt = con.prepareStatement(sql);

			// step4 execute query
			rs = stmt.executeQuery();

			CSVWriter csvWriter = new CSVWriter(writer, CSVWriter.DEFAULT_SEPARATOR, CSVWriter.NO_QUOTE_CHARACTER,
					CSVWriter.NO_ESCAPE_CHARACTER, CSVWriter.DEFAULT_LINE_END);
			int columnCount = 0;
			if (rs.next()) {
				columnCount = rs.getMetaData().getColumnCount();
				log.info("true###########" + columnCount);
				String clobString = Utils.clobToString(rs.getClob(1));
				log.info("clobString::::::" + clobString);
				csvWriter.writeNext(clobString.split(","));
			}
			csvWriter.flush();
			csvWriter.close();
			long endTime = System.currentTimeMillis();
			long diffTime = startTime - endTime;
			log.info("diff time :::::" + diffTime);

		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e.getMessage());
		} finally {
			if (con != null)
				con.close();
		}

	}

	public void generateFbdiFromLobV1(Long cloudTemplateId, String batchName, PrintWriter writer) throws Exception {
		log.info("Start of generateFbdi Method in DaoImpl######");
		ResultSet rs = null;
		Connection con = null;
		try {
			String sql = "select xxr_conversion_utils_v1_pkg.fbdi_filegen(" + cloudTemplateId + ",'" + batchName + "')"
					+ " from dual";
			log.info(sql);
			con = dataSourceUtil.createConnection();
			// con = dataSource.getConnection();

			// step3 create the statement object
			PreparedStatement stmt = con.prepareStatement(sql);

			// step4 execute query
			rs = stmt.executeQuery();

			CSVWriter csvWriter = new CSVWriter(writer, CSVWriter.DEFAULT_SEPARATOR, CSVWriter.NO_QUOTE_CHARACTER,
					CSVWriter.NO_ESCAPE_CHARACTER, CSVWriter.DEFAULT_LINE_END);
			int columnCount = 0;
			if (rs.next()) {
				columnCount = rs.getMetaData().getColumnCount();
				log.info("true###########" + columnCount);
				String clobString = Utils.clobToString(rs.getClob(1));
				log.info("clobString::::::" + clobString);
				csvWriter.writeNext(clobString.split(","));
			}
			csvWriter.flush();
			csvWriter.close();

		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e.getMessage());
		} finally {
			if (con != null)
				con.close();
		}

	}

	public void generateFailedRec(Long cloudTemplateId, PrintWriter writer) throws Exception {
		log.info("Start of generateFbdi Method in DaoImpl######");
		ResultSet rs = null;
		Connection con = null;
		try {
			String sql = "select failed_clob from xxr_source_load_fail_records where template_id=2584 and Id=1741";
			log.info(sql);
			con = dataSourceUtil.createConnection();
			// con = dataSource.getConnection();

			// step3 create the statement object
			PreparedStatement stmt = con.prepareStatement(sql);

			// step4 execute query
			rs = stmt.executeQuery();

			CSVWriter csvWriter = new CSVWriter(writer, CSVWriter.DEFAULT_SEPARATOR, CSVWriter.NO_QUOTE_CHARACTER,
					CSVWriter.NO_ESCAPE_CHARACTER, CSVWriter.DEFAULT_LINE_END);
			int columnCount = 0;
			if (rs.next()) {
				columnCount = rs.getMetaData().getColumnCount();
				log.info("true###########" + columnCount);
				String clobString = Utils.clobToString(rs.getClob(1));
				log.info("clobString::::::" + clobString);
				csvWriter.writeNext(clobString.split(","));
			}
			csvWriter.flush();
			csvWriter.close();

		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e.getMessage());
		} finally {
			if (con != null)
				con.close();
		}

	}

	public void generateHdlFromLob(String cloudTemplateId, String batchName, String isIntialLoad, PrintWriter writer) throws Exception {
		log.info("Start of generateHdlFromLob Method in DaoImpl######");
			StoredProcedureQuery createStaggingStoredProcedure = entityManager
					.createStoredProcedureQuery("cr_hdl_filegen_proc")
					.registerStoredProcedureParameter("p_cld_template_id", String.class, ParameterMode.IN)
					.registerStoredProcedureParameter("p_batch_name", String.class, ParameterMode.IN)
					.registerStoredProcedureParameter("p_intial_load", String.class, ParameterMode.IN)


					.registerStoredProcedureParameter("p_clob_hdl_file", Clob.class, ParameterMode.OUT)
					.registerStoredProcedureParameter("p_ret_code", String.class, ParameterMode.OUT)
					.registerStoredProcedureParameter("p_ret_msg", String.class, ParameterMode.OUT)

					.setParameter("p_cld_template_id", cloudTemplateId)
					.setParameter("p_batch_name", batchName)
					.setParameter("p_intial_load", isIntialLoad);


			createStaggingStoredProcedure.execute();
			Object clobString = createStaggingStoredProcedure.getOutputParameterValue("p_clob_hdl_file");
			String res = Utils.clobToString((Clob) clobString);
			log.info("res---->"+res);
			writer.write(res);
			entityManager.clear();
			entityManager.close();
	}

	public String generateHdlFromLob1(String cloudTemplateId, String batchName, String isIntialLoad) throws Exception {
		log.info("Start of generateHdlFromLob Method in DaoImpl######");

		String res;
		try {
			StoredProcedureQuery createStaggingStoredProcedure = entityManager
					.createStoredProcedureQuery("cr_hdl_filegen_proc")
					.registerStoredProcedureParameter("p_cld_template_id", String.class, ParameterMode.IN)
					.registerStoredProcedureParameter("p_batch_name", String.class, ParameterMode.IN)
					.registerStoredProcedureParameter("p_intial_load", String.class, ParameterMode.IN)

					.registerStoredProcedureParameter("p_clob_hdl_file", Clob.class, ParameterMode.OUT)
					.registerStoredProcedureParameter("p_ret_code", String.class, ParameterMode.OUT)
					.registerStoredProcedureParameter("p_ret_msg", String.class, ParameterMode.OUT)

					.setParameter("p_cld_template_id", cloudTemplateId)
					.setParameter("p_batch_name", batchName)
					.setParameter("p_intial_load", isIntialLoad);

			createStaggingStoredProcedure.execute();

			String resCode = createStaggingStoredProcedure.getOutputParameterValue("p_ret_code").toString();
			String resMsg = createStaggingStoredProcedure.getOutputParameterValue("p_ret_msg").toString();
			log.error("Process  resMsg : " + resMsg);

			if (resCode.equalsIgnoreCase("N")) {
				log.error("Process failed: " + resCode);
				throw new Exception(resMsg);
			}

			Object clobString = createStaggingStoredProcedure.getOutputParameterValue("p_clob_hdl_file");
			log.info("clobToString --->" + clobString.toString().length());
			res = Utils.clobToString((Clob) clobString);
			//  writer.write(res);
			entityManager.clear();
			entityManager.close();

		} catch (Exception e) {
			e.printStackTrace();
			log.error("Exception occurred in generateHdlFromLob1 method: " + e);
			throw new Exception(e.getMessage());
		}
		return res;
	}
	public UpdateFailedRecResp updateFailedRec(List<String> sqlQueryLi) throws Exception {
		log.info("Start of updateFailedRec Method in DaoImpl######");
		Connection con = null;
		UpdateFailedRecResp updateFailedRecResp = new UpdateFailedRecResp();
		try {
			con = dataSourceUtil.createConnection();
			Statement stmt = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
			con.setAutoCommit(false);
			for (String sqlQuery : sqlQueryLi) {
				stmt.addBatch(sqlQuery);
			}
			stmt.executeBatch();
			con.commit();
			updateFailedRecResp.setMessage("Suceessfully update edited failed Records");
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return updateFailedRecResp;
	}

	public void getTransformationReport(Long cloudTemplateId, String batchName, PrintWriter writer) throws Exception {
		log.info("Start of getTransformationReport Method in DaoImpl######");
		ResultSet rs = null;
		Connection con = null;
		try {
			String sql = "SELECT xxr_conversion_utils_v1_pkg.conversion_report_filegen(" + cloudTemplateId + ",'"
					+ batchName + "')" + " from dual";
			con = dataSourceUtil.createConnection();
			// con = dataSource.getConnection();
			// step3 create the statement object
			PreparedStatement stmt = con.prepareStatement(sql);
			// step4 execute query
			rs = stmt.executeQuery();
			// int columnCount=0;
			if (rs.next()) {
				// columnCount= rs.getMetaData().getColumnCount();
				String clobString = Utils.clobToString(rs.getClob(1));
			}
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		} finally {
			if (con != null)
				con.close();
		}

	}

	public DeleteStagingDataRes deleteStagingData(DeleteStagingDataReq deleteStagingDataReq) throws Exception {
		log.info("Start of deleteStagingData Method in Dao####");
		Connection con = null;
		Statement stmt = null;
		int[] countOfRowsDeleted = null;
		DeleteStagingDataRes deleteStagingDataRes = new DeleteStagingDataRes();
		try {
			List<String> srcStagingTables = deleteStagingDataReq.getSrcStagingTables();
			List<String> cldStagingTables = deleteStagingDataReq.getCldStagingTables();
			con = dataSourceUtil.createConnection();
			// step3 create the statement object
			stmt = con.createStatement();
			for (int i = 0; i < srcStagingTables.size(); i++) {
				String srcSql = "delete from " + srcStagingTables.get(i);
				String cldSql = "delete from " + cldStagingTables.get(i);
				stmt.addBatch(srcSql);
				stmt.addBatch(cldSql);
			}
			countOfRowsDeleted = stmt.executeBatch();
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		} finally {
			if (con != null)
				con.close();
		}
		deleteStagingDataRes.setCountOfRowsDeleted(countOfRowsDeleted);
		deleteStagingDataRes.setMessage("Successfully deleted data from staging tables");
		return deleteStagingDataRes;
	}

	public String generateXlsFromLob(Long cloudTemplateId) throws Exception {
		log.info("Start of generateFbdi Method in DaoImpl######");
		ResultSet rs = null;
		Connection con = null;
		String clobString = "";
		try {
			String sql = "select xxr_conversion_utils_pkg.fbdi_filegen(" + cloudTemplateId + ")" + " from dual";
			log.info(sql);
			con = dataSourceUtil.createConnection();
			// con = dataSource.getConnection();

			// step3 create the statement object
			PreparedStatement stmt = con.prepareStatement(sql);

			// step4 execute query
			rs = stmt.executeQuery();

			int columnCount = 0;
			if (rs.next()) {
				columnCount = rs.getMetaData().getColumnCount();
				log.info("true###########" + columnCount);
				clobString = Utils.clobToString(rs.getClob(1));
				log.info("clobString::::::" + clobString);

			}

		} finally {
			if (con != null)
				con.close();
		}
		return clobString;

	}

	public String downloadFbdiImport(Long cloudTemplateId, String batchName, String fileName, ChannelSftp channelSftp) throws Exception {
		log.info("Start of downloadFbdi in Dao ####");
		String result = "";
		try {
			long startTime = System.currentTimeMillis();
			log.info("startTime:::::" + startTime);

			// Execution of proc
			StoredProcedureQuery createStaggingStoredProcedure = entityManager
					.createStoredProcedureQuery("xxr_conversion_utils_v1_pkg.fbdi_filegen_ftp")
					.registerStoredProcedureParameter("p_template_id", Long.class, ParameterMode.IN)
					.registerStoredProcedureParameter("p_batch_name", String.class, ParameterMode.IN)
					.setParameter("p_template_id", cloudTemplateId).setParameter("p_batch_name", batchName);

			createStaggingStoredProcedure.execute();
			entityManager.clear();
			entityManager.close();

			InputStream inputStream = channelSftp.get(fileName);
			result = new BufferedReader(new InputStreamReader(inputStream)).lines().parallel()
					.collect(Collectors.joining("\n"));

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new Exception(e.getMessage());
		}
		return result;

	}

	 public void downloadFbdi(Long cloudTemplateId, String batchName, HttpServletResponse response) throws Exception {
         log.info("Start of downloadFbdi in Dao ####");
         String resultMsg = null;
         try {
             StoredProcedureQuery createStaggingStoredProcedure = entityManager
                     .createStoredProcedureQuery("CR_FBDI_FILEGEN_PROC")
                     .registerStoredProcedureParameter("p_cld_template_id", Long.class, ParameterMode.IN)
                     .registerStoredProcedureParameter("p_batch_name", String.class, ParameterMode.IN)
                     .registerStoredProcedureParameter("p_clob_fbdi_file", Clob.class, ParameterMode.OUT)
                     .registerStoredProcedureParameter("p_result_code", String.class, ParameterMode.OUT)
                     .registerStoredProcedureParameter("p_result_msg", String.class, ParameterMode.OUT)
                     .setParameter("p_cld_template_id", cloudTemplateId)
                     .setParameter("p_batch_name", batchName);

             createStaggingStoredProcedure.execute();

             // Retrieve output parameters
             String resultCode = createStaggingStoredProcedure.getOutputParameterValue("p_result_code").toString();
             resultMsg = createStaggingStoredProcedure.getOutputParameterValue("p_result_msg").toString();

             log.info("resultCode" + resultCode);
             log.info("resultMsg" + resultMsg);
             if (resultCode != null && resultMsg != null) {
                 Object clobString;
                 if ("Y".equals(resultCode.trim())) {
                     // Success scenario
                     clobString = createStaggingStoredProcedure.getOutputParameterValue("p_clob_fbdi_file");
                     if (clobString != null && ((Clob) clobString).length() != 0) {
                         String res = Utils.clobToString((Clob) clobString);
                         PrintWriter writer = response.getWriter();
                         writer.write(res);
                         entityManager.clear();
                         entityManager.close();
                     } else {
                         log.info("======No Data, FBDI File is empty.======");
                         response.setStatus(HttpServletResponse.SC_OK);
						 response.getWriter().write("No Data, FBDI File is empty.");
                     }
                 } else {
                     log.error("Unexpected error in CR_FBDI_FILEGEN_PROC: " + resultMsg);
                     response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                     response.getWriter().write(resultMsg);
                 }
             } else {
                 // Handling if output parameters are null
                 log.error("Result code or message is null--->"+resultMsg);
                 response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                 response.getWriter().write("Unexpected error: Result code or message is null");
             }
         } catch (Exception e) {
             log.error("Error occurred during downloadFbdi: " + e.getMessage(), e);
             response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
             response.getWriter().write( e.getMessage());
         }
     }



    public String downloadFbdiData(Long cloudTemplateId, String batchName) throws Exception {
        String res = "";

        try {
            StoredProcedureQuery createStaggingStoredProcedure = entityManager
                    .createStoredProcedureQuery("CR_FBDI_FILEGEN_PROC")
                    .registerStoredProcedureParameter("p_cld_template_id", Long.class, ParameterMode.IN)
                    .registerStoredProcedureParameter("p_batch_name", String.class, ParameterMode.IN)
                    .registerStoredProcedureParameter("p_clob_fbdi_file", Clob.class, ParameterMode.OUT)
                    .registerStoredProcedureParameter("p_result_code", String.class, ParameterMode.OUT)
                    .registerStoredProcedureParameter("p_result_msg", String.class, ParameterMode.OUT)
                    .setParameter("p_cld_template_id", cloudTemplateId)
                    .setParameter("p_batch_name", batchName);

            createStaggingStoredProcedure.execute();

            String resultCode = createStaggingStoredProcedure.getOutputParameterValue("p_result_code").toString();
            String resultMsg = createStaggingStoredProcedure.getOutputParameterValue("p_result_msg").toString();
            log.info("resultCode" + resultCode);
            log.info("resultMsg" + resultMsg);
            if (resultCode != null && resultMsg != null) {
                if ("Y".equals(resultCode.trim())) {
                    Object clobString = createStaggingStoredProcedure.getOutputParameterValue("p_clob_fbdi_file");
                    if (clobString != null && ((Clob)clobString).length()!=0) {
                        res = Utils.clobToString((Clob) clobString);
                    }
                } else {
                    log.error("Error while downloading FBDI data");
                    throw new Exception("Error while downloading FBDI data");
                }
            } else {
                log.error("Result code or message is null");
                throw new Exception("Error while downloading FBDI data");
            }
            entityManager.clear();
            entityManager.close();

        } catch (Exception e) {
            log.error("Error occurred during FBDI downloadData for templateId : "+ cloudTemplateId +"--->"+ e.getMessage());
            throw new Exception("Error occurred during FBDI downloadData");
        }
        return res;
    }
	public Object downloadCloudImportSuccessRec(Long cloudTemplateId, String batchName, HttpServletResponse response) throws Exception {
        String clobStr = null;
        ReconcileResPo res = new ReconcileResPo();
        try {
            String sql = "SELECT CR_RECON_BASE_SUCC_FUNC(:cloudTemplateId, :batchName) FROM DUAL";
            Object result = entityManager.createNativeQuery(sql)
                    .setParameter("cloudTemplateId", cloudTemplateId)
                    .setParameter("batchName", batchName)
                    .getSingleResult();
            Clob clobString = (Clob) result;
            if (clobString != null) {
                response.setContentType("text/csv");
                response.setHeader(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=CloudImportSuccessRec.csv");
                clobStr = clobToString(clobString);
                PrintWriter writer = response.getWriter();
                writer.write(clobStr);
            } else {
                System.out.println("No Success Records Found");
                PrintWriter writer = response.getWriter();
                writer.write("\"No Success Records Found\"");
                response.setStatus(404);
            }
            res.setResCode("200");
            res.setResMsg("Success");
            res.setClobString(clobStr);
            entityManager.clear();
            entityManager.close();
        } catch (Exception e) {
            res.setResCode("500");
            res.setResMsg(e.getMessage());
            log.error(e.getMessage());
            throw new Exception(e.getMessage());
        }
        return res;

    }

	 public Object downloadCloudImportRejRec(Long cloudTemplateId, String batchName, String userId, HttpServletResponse response) throws Exception {
        String clobStr = null;
        ReconcileResPo res = new ReconcileResPo();
        try {
            StoredProcedureQuery crCloudImportSucProc = entityManager
                    .createStoredProcedureQuery("cr_src_val_fail_proc")
                    .registerStoredProcedureParameter("p_user_id", String.class, ParameterMode.IN)
                    .registerStoredProcedureParameter("p_cld_template_id", Long.class, ParameterMode.IN)
                    .registerStoredProcedureParameter("p_batch_name", String.class, ParameterMode.IN)
                    .registerStoredProcedureParameter("p_clob_rej_file", Clob.class, ParameterMode.OUT)
                    .registerStoredProcedureParameter("p_ret_code", String.class, ParameterMode.OUT)
                    .registerStoredProcedureParameter("p_ret_msg", String.class, ParameterMode.OUT)

                    .setParameter("p_user_id", userId)
                    .setParameter("p_cld_template_id", cloudTemplateId)
                    .setParameter("p_batch_name", batchName);

            crCloudImportSucProc.execute();
            Object clobString = crCloudImportSucProc.getOutputParameterValue("p_clob_rej_file");
            String resCode = crCloudImportSucProc.getOutputParameterValue("p_ret_code").toString();
            String resMsg = crCloudImportSucProc.getOutputParameterValue("p_ret_msg").toString();

            if (clobString != null) {
                //Fix for Cross-site Scripting (XSS)
                response.setContentType("text/csv");
                response.setHeader(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=CloudImportRejRec.csv");
                clobStr = Utils.clobToString((Clob) clobString);
                PrintWriter writer = response.getWriter();
                writer.write(clobStr);
            } else {
                log.info("======No Records Found=======");
                PrintWriter writer = response.getWriter();
                writer.write("\"No failed Records Found\"");
                response.setStatus(404);
            }
            res.setResMsg(resCode);
            res.setResMsg(resMsg);
            res.setClobString(clobStr);
            entityManager.clear();
            entityManager.close();

        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception(e.getMessage());
        }
        return res;
    }


	public Object downloadCloudImportFailRec(Long cloudTemplateId, String batchName, HttpServletResponse response) throws Exception {
        ReconcileResPo res = new ReconcileResPo();
        String clobStr = null;
        try {
            String sql = "SELECT CR_RECON_BASE_FAIL_FUNC(:cloudTemplateId, :batchName) FROM DUAL";
            Object result = entityManager.createNativeQuery(sql)
                    .setParameter("cloudTemplateId", cloudTemplateId)
                    .setParameter("batchName", batchName)
                    .getSingleResult();
            Clob clobString = (Clob) result;
            if (clobString != null) {
                response.setContentType("text/csv");
                response.setHeader(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=CloudImportFailRec.csv");
                clobStr = clobToString(clobString);
                PrintWriter writer = response.getWriter();
                writer.write(clobStr);
            } else {
                System.out.println("No Fail Records Found");
                clobStr = "\"No Fail Records Found\"";
                response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                response.getWriter().write(clobStr);
            }
            res.setResCode("200"); // Assuming this should be set to indicate the success of the operation
            res.setResMsg("Success");
            res.setClobString(clobStr);

            entityManager.clear();
            entityManager.close();

        } catch (Exception e) {
            res.setResCode("500");
            res.setResMsg(e.getMessage());
            log.error(e.getMessage());
            throw new Exception(e.getMessage());
        }
        return res;
    }
}
