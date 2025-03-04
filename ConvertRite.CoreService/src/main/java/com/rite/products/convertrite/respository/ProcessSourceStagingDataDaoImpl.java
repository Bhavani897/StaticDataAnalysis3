package com.rite.products.convertrite.respository;

import java.sql.Clob;
import java.util.ArrayList;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.ParameterMode;
import javax.persistence.PersistenceContext;
import javax.persistence.Query;
import javax.persistence.StoredProcedureQuery;
import javax.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import com.rite.products.convertrite.po.ProcessJobPo;
import com.rite.products.convertrite.po.ProcessSourceDataResPo;

@Repository
public class ProcessSourceStagingDataDaoImpl {

	private static final Logger log = LoggerFactory.getLogger(ProcessSourceStagingDataDaoImpl.class);

	@PersistenceContext
	private EntityManager entityManager;

	public ProcessSourceDataResPo processSourceStagingData(String fileName, String templateName,String batchName,HttpServletRequest request)
			throws Exception {
		log.info("Start of processSourceStagingData Method in DaoImpl### ");
		ProcessSourceDataResPo processSourceDataResPo = new ProcessSourceDataResPo();
		try {
			StoredProcedureQuery processSourceStagingDataProcedure = entityManager
					.createStoredProcedureQuery("XXR_INSERTION_UTILS_PKG.insert_data_with_clob")
					.registerStoredProcedureParameter("p_template_name", String.class, ParameterMode.IN)
					.registerStoredProcedureParameter("p_clob_name", String.class, ParameterMode.IN)
					.registerStoredProcedureParameter("p_user_id", String.class, ParameterMode.IN)
					.registerStoredProcedureParameter("p_batch_name", String.class, ParameterMode.IN)
					.registerStoredProcedureParameter("p_request_id", Integer.class, ParameterMode.OUT)
					.registerStoredProcedureParameter("p_msg", String.class, ParameterMode.OUT)
					.setParameter("p_template_name", templateName)
					.setParameter("p_clob_name", fileName)
					.setParameter("p_batch_name", batchName)
					.setParameter("p_user_id", request.getHeader("userId"));
			processSourceStagingDataProcedure.execute();

			processSourceDataResPo
					.setRequestId((Integer) processSourceStagingDataProcedure.getOutputParameterValue("p_request_id"));
			String status = ((String) processSourceStagingDataProcedure.getOutputParameterValue("p_msg"));
			if (status.equalsIgnoreCase("SUCCESS"))
				processSourceDataResPo.setMessage(status);
			else
				processSourceDataResPo.setError(status);

			entityManager.clear();
			entityManager.close();
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return processSourceDataResPo;
	}

	public ProcessJobPo processSourceData(Clob clob, String templateName,HttpServletRequest request) throws Exception {
		log.info("Start of processSourceData Method in DaoImpl### ");
		ProcessJobPo processJobPo = new ProcessJobPo();
		try {
			StoredProcedureQuery processSourceStagingDataProcedure = entityManager
					.createStoredProcedureQuery("xxr_insertion_utils_pkg.parse_clob")
					.registerStoredProcedureParameter("p_template_name", String.class, ParameterMode.IN)
					.registerStoredProcedureParameter("p_clob", Clob.class, ParameterMode.IN)
					.registerStoredProcedureParameter("p_user_id", String.class, ParameterMode.IN)
					.registerStoredProcedureParameter("p_msg", String.class, ParameterMode.OUT)
					.setParameter("p_template_name", templateName).setParameter("p_clob", clob)
					.setParameter("p_user_id", request.getHeader("userId"));

			processSourceStagingDataProcedure.execute();

			processJobPo.setMsg((String) processSourceStagingDataProcedure.getOutputParameterValue("p_msg"));

			entityManager.clear();
			entityManager.close();
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return processJobPo;
	}

	/*
	 * public List<Object> getTemplateStatistics(String filterBy) throws Exception {
	 * log.info("Start of getTemplateStatistics Method in DaoImpl### ");
	 * List<Object> list=new ArrayList<>(); try {
	 * 
	 * String columnNames="";
	 * 
	 * if(filterBy.equalsIgnoreCase("POD")) columnNames="POD_ID,POD"; else
	 * if(filterBy.equalsIgnoreCase("Template"))
	 * columnNames="CLOUD_TEMPLATE_ID,CLOUD_TEMPLATE_NAME"; else
	 * if(filterBy.equalsIgnoreCase("OBJECT")) columnNames="OBJECT_ID,OBJECT"; else
	 * if(filterBy.equalsIgnoreCase("PARENT OBJECT"))
	 * columnNames="PARENT_OBJECT_ID,PARENT_OBJECT_CODE"; else
	 * if(filterBy.equalsIgnoreCase("PROJECT")) columnNames="PROJECT_ID,PROJECT";
	 * 
	 * 
	 * String sqlString="select "+columnNames+",SUM(TOTAL_CLOUD_RECORDS)  SUCCESS,"
	 * +
	 * "SUM((TOTAL_SOURCE_RECORDS - SOURCE_UNVERFIED) - TOTAL_CLOUD_RECORDS)  Failed,"
	 * + "SUM(SOURCE_UNVERFIED)  NEW from XXR_TEMPLATE_STATISTICS group by "+
	 * columnNames;
	 * 
	 * Query query=entityManager.createNativeQuery(sqlString);
	 * 
	 * list=query.getResultList();
	 * 
	 * } catch (Exception e) { throw new Exception(e.getMessage()); } return list; }
	 */

	@SuppressWarnings("unchecked")
	public List<Object> getTemplateStatistics() throws Exception {
		log.info("Start of getTemplateStatistics Method in DaoImpl### ");
		List<Object> list = new ArrayList<>();
		try {

			String sqlString = "SELECT CLOUD_TEMPLATE_ID CRITERIA_ID, CLOUD_TEMPLATE_NAME CRITERIA_NAME, 'TEMPLATE' CRITERIA_TYPE,\r\n"
					+ "TOTAL_CLOUD_RECORDS SUCCESS,\r\n"
					+ "(TOTAL_SOURCE_RECORDS - SOURCE_UNVERFIED) - TOTAL_CLOUD_RECORDS FAILED,\r\n"
					+ "SOURCE_UNVERFIED UNVERIFIED\r\n" + "FROM XXR_TEMPLATE_STATISTICS\r\n" + "UNION ALL\r\n"
					+ "SELECT POD_ID,POD,'POD',SUM(TOTAL_CLOUD_RECORDS)  SUCCESS,\r\n"
					+ "SUM((TOTAL_SOURCE_RECORDS - SOURCE_UNVERFIED) - TOTAL_CLOUD_RECORDS)  FAILED,\r\n"
					+ "SUM(SOURCE_UNVERFIED)  NEW FROM XXR_TEMPLATE_STATISTICS GROUP BY  POD_ID,POD\r\n"
					+ "UNION ALL\r\n" + "SELECT PROJECT_ID,PROJECT,'PROJECT' ,SUM(TOTAL_CLOUD_RECORDS)  SUCCESS,\r\n"
					+ "SUM((TOTAL_SOURCE_RECORDS - SOURCE_UNVERFIED) - TOTAL_CLOUD_RECORDS)  FAILED,\r\n"
					+ "SUM(SOURCE_UNVERFIED)  NEW FROM XXR_TEMPLATE_STATISTICS GROUP BY  PROJECT_ID,PROJECT\r\n"
					+ "UNION ALL\r\n"
					+ "SELECT PARENT_OBJECT_ID,PARENT_OBJECT_CODE,'PARENT_OBJECT_CODE' ,SUM(TOTAL_CLOUD_RECORDS)  SUCCESS,\r\n"
					+ "SUM((TOTAL_SOURCE_RECORDS - SOURCE_UNVERFIED) - TOTAL_CLOUD_RECORDS)  FAILED,\r\n"
					+ "SUM(SOURCE_UNVERFIED)  NEW FROM XXR_TEMPLATE_STATISTICS GROUP BY  PARENT_OBJECT_ID,PARENT_OBJECT_CODE\r\n"
					+ "UNION ALL\r\n" + "SELECT OBJECT_ID,OBJECT,'OBJECT', SUM(TOTAL_CLOUD_RECORDS)  SUCCESS,\r\n"
					+ "SUM((TOTAL_SOURCE_RECORDS - SOURCE_UNVERFIED) - TOTAL_CLOUD_RECORDS)  FAILED,\r\n"
					+ "SUM(SOURCE_UNVERFIED)  NEW FROM XXR_TEMPLATE_STATISTICS GROUP BY  OBJECT_ID,OBJECT";

			Query query = entityManager.createNativeQuery(sqlString);

			list = query.getResultList();

		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return list;
	}

	@SuppressWarnings("unchecked")
	public List<Object> getSourceTemplateStatistics() throws Exception {
		log.info("Start of getSourceTemplateStatistics Method in DaoImpl### ");
		List<Object> list = new ArrayList<>();
		try {
			String sqlString = "SELECT TEMPLATE_ID CRITERIA_ID, FILE_NAME CRITERIA_NAME, 'FileName' CRITERIA_TYPE, \n"
					+ "SUCCESS, \n" + "FAILED \n" + "FROM XXR_SOURCE_LOAD_FAIL_RECORDS\n" + "UNION ALL \n"
					+ "SELECT FR.TEMPLATE_ID, ST.TEMPLATE_NAME,'TEMPLATE_NAME', SUM(FR.SUCCESS) SUCCESS, SUM(FR.FAILED) FAILED  \n"
					+ " FROM XXR_SOURCE_LOAD_FAIL_RECORDS FR, XXR_SRC_TEMP_HDRS ST WHERE FR.TEMPLATE_ID = ST.TEMPLATE_ID GROUP BY FR.TEMPLATE_ID, ST.TEMPLATE_NAME \n"
					+ "UNION ALL \n"
					+ "SELECT ST.POD_ID, LV.LOOKUP_VALUE, 'POD', SUM(FR.SUCCESS) SUCCESS,SUM(FR.FAILED) FAILED \n"
					+ " FROM XXR_SOURCE_LOAD_FAIL_RECORDS FR, XXR_SRC_TEMP_HDRS ST, XXR_LOOKUP_VALUES LV WHERE FR.TEMPLATE_ID = ST.TEMPLATE_ID AND \n"
					+ "ST.POD_ID = LV.LOOKUP_VALUE_ID GROUP BY ST.POD_ID, LV.LOOKUP_VALUE\n" + "UNION ALL \n"
					+ "SELECT ST.OBJECT_ID, LV.LOOKUP_VALUE, 'OBJECT_CODE', SUM(FR.SUCCESS) SUCCESS,SUM(FR.FAILED) FAILED \n"
					+ " FROM XXR_SOURCE_LOAD_FAIL_RECORDS FR, XXR_SRC_TEMP_HDRS ST, XXR_LOOKUP_VALUES LV WHERE FR.TEMPLATE_ID = ST.TEMPLATE_ID AND \n"
					+ "ST.OBJECT_ID = LV.LOOKUP_VALUE_ID GROUP BY ST.OBJECT_ID, LV.LOOKUP_VALUE\n" + "UNION ALL \n"
					+ "SELECT ST.PARENT_OBJECT_ID, LV.LOOKUP_VALUE, 'PARENT_OBJECT_CODE', SUM(FR.SUCCESS) SUCCESS,SUM(FR.FAILED) FAILED \n"
					+ " FROM XXR_SOURCE_LOAD_FAIL_RECORDS FR, XXR_SRC_TEMP_HDRS ST, XXR_LOOKUP_VALUES LV WHERE FR.TEMPLATE_ID = ST.TEMPLATE_ID AND \n"
					+ "ST.PARENT_OBJECT_ID = LV.LOOKUP_VALUE_ID GROUP BY ST.PARENT_OBJECT_ID, LV.LOOKUP_VALUE\n"
					+ "UNION ALL \n"
					+ "SELECT ST.PROJECT_ID, LV.LOOKUP_VALUE, 'PROJECT_ID', SUM(FR.SUCCESS) SUCCESS,SUM(FR.FAILED) FAILED \n"
					+ " FROM XXR_SOURCE_LOAD_FAIL_RECORDS FR, XXR_SRC_TEMP_HDRS ST, XXR_LOOKUP_VALUES LV WHERE FR.TEMPLATE_ID = ST.TEMPLATE_ID AND \n"
					+ "ST.PROJECT_ID = LV.LOOKUP_VALUE_ID GROUP BY ST.PROJECT_ID, LV.LOOKUP_VALUE\n" + "";

			Query query = entityManager.createNativeQuery(sqlString);

			list = query.getResultList();

		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return list;
	}
}
