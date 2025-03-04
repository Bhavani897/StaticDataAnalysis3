package com.rite.products.convertrite.respository;

import javax.persistence.EntityManager;
import javax.persistence.ParameterMode;
import javax.persistence.PersistenceContext;
import javax.persistence.StoredProcedureQuery;
import javax.servlet.http.HttpServletRequest;
import javax.transaction.Transactional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import com.rite.products.convertrite.po.ProcessJobPo;

@Repository
public class ProcessJobDaoImpl {

	private static final Logger log = LoggerFactory.getLogger(CreateDynamicViewDaoImpl.class);

	@PersistenceContext
	private EntityManager entityManager;

	@Transactional
	public ProcessJobPo processJob(String cloudTemplateName, String type, HttpServletRequest request) throws Exception {
		log.info("Start of processJob Method in DaoImpl### ");
		ProcessJobPo processJobPo = new ProcessJobPo();
		try {
			if ("Validation".equalsIgnoreCase(type)) {
				StoredProcedureQuery processValidationJobProcedure = entityManager
						.createStoredProcedureQuery("XXR_CONVERSION_UTILS_PKG.initiate_validation")
						.registerStoredProcedureParameter("p_cloud_template_name", String.class, ParameterMode.IN)
						.registerStoredProcedureParameter("p_reprocess_flag", String.class, ParameterMode.IN)
						.registerStoredProcedureParameter("p_user_id", String.class, ParameterMode.IN)
						.registerStoredProcedureParameter("p_request_id", Integer.class, ParameterMode.OUT)
						.registerStoredProcedureParameter("p_msg", String.class, ParameterMode.OUT)
						.setParameter("p_cloud_template_name", cloudTemplateName).setParameter("p_reprocess_flag", "N")
						.setParameter("p_user_id", request.getHeader("userId"));
				processValidationJobProcedure.execute();

				processJobPo
						.setRequestId((Integer) processValidationJobProcedure.getOutputParameterValue("p_request_id"));
				processJobPo.setMsg((String) processValidationJobProcedure.getOutputParameterValue("p_msg"));
			} else if ("ReProcesses".equalsIgnoreCase(type)) {
				StoredProcedureQuery processReProcessesJobProcedure = entityManager
						.createStoredProcedureQuery("XXR_CONVERSION_UTILS_PKG.initiate_validation")
						.registerStoredProcedureParameter("p_cloud_template_name", String.class, ParameterMode.IN)
						.registerStoredProcedureParameter("p_reprocess_flag", String.class, ParameterMode.IN)
						.registerStoredProcedureParameter("p_user_id", String.class, ParameterMode.IN)
						.registerStoredProcedureParameter("p_request_id", Integer.class, ParameterMode.OUT)
						.registerStoredProcedureParameter("p_msg", String.class, ParameterMode.OUT)
						.setParameter("p_cloud_template_name", cloudTemplateName).setParameter("p_reprocess_flag", "Y")
						.setParameter("p_user_id", request.getHeader("userId"));
				processReProcessesJobProcedure.execute();

				processJobPo
						.setRequestId((Integer) processReProcessesJobProcedure.getOutputParameterValue("p_request_id"));
				processJobPo.setMsg((String) processReProcessesJobProcedure.getOutputParameterValue("p_msg"));

			} else if ("Conversion".equalsIgnoreCase(type)) {
				StoredProcedureQuery processConversionJobProcedure = entityManager
						.createStoredProcedureQuery("XXR_CONVERSION_UTILS_PKG.initiate_conversion")
						.registerStoredProcedureParameter("p_cloud_template_name", String.class, ParameterMode.IN)
						.registerStoredProcedureParameter("p_user_id", String.class, ParameterMode.IN)
						.registerStoredProcedureParameter("p_request_id", Integer.class, ParameterMode.OUT)
						.registerStoredProcedureParameter("p_msg", String.class, ParameterMode.OUT)
						.setParameter("p_cloud_template_name", cloudTemplateName)
						.setParameter("p_user_id", request.getHeader("userId"));
				processConversionJobProcedure.execute();
				processJobPo
						.setRequestId((Integer) processConversionJobProcedure.getOutputParameterValue("p_request_id"));
				processJobPo.setMsg((String) processConversionJobProcedure.getOutputParameterValue("p_msg"));

			}

			entityManager.clear();
			entityManager.close();
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return processJobPo;
	}

	public ProcessJobPo processJobV1(String cloudTemplateName, String type, String batchName,
			HttpServletRequest request) throws Exception {
		log.info("Start of processJobV1 ##########");
		ProcessJobPo processJobPo = new ProcessJobPo();
		try {
			if ("Validation".equalsIgnoreCase(type)) {
				StoredProcedureQuery processValidationJobProcedure = entityManager
						.createStoredProcedureQuery("XXR_CONVERSION_UTILS_V1_PKG.initiate_validation")
						.registerStoredProcedureParameter("p_cloud_template_name", String.class, ParameterMode.IN)
						.registerStoredProcedureParameter("p_reprocess_flag", String.class, ParameterMode.IN)
						.registerStoredProcedureParameter("p_batch_flag", String.class, ParameterMode.IN)
						.registerStoredProcedureParameter("p_user_id", String.class, ParameterMode.IN)
						.registerStoredProcedureParameter("p_batch_name", String.class, ParameterMode.IN)
						.registerStoredProcedureParameter("p_request_id", Integer.class, ParameterMode.OUT)
						.registerStoredProcedureParameter("p_msg", String.class, ParameterMode.OUT)
						.setParameter("p_cloud_template_name", cloudTemplateName).setParameter("p_reprocess_flag", "N")
						.setParameter("p_user_id", request.getHeader("userId")).setParameter("p_batch_flag", "Y").setParameter("p_batch_name", batchName);
				processValidationJobProcedure.execute();

				processJobPo
						.setRequestId((Integer) processValidationJobProcedure.getOutputParameterValue("p_request_id"));
				processJobPo.setMsg((String) processValidationJobProcedure.getOutputParameterValue("p_msg"));
			} else if ("ReProcesses".equalsIgnoreCase(type)) {
				StoredProcedureQuery processReProcessesJobProcedure = entityManager
						.createStoredProcedureQuery("XXR_CONVERSION_UTILS_V1_PKG.initiate_validation")
						.registerStoredProcedureParameter("p_cloud_template_name", String.class, ParameterMode.IN)
						.registerStoredProcedureParameter("p_reprocess_flag", String.class, ParameterMode.IN)
						.registerStoredProcedureParameter("p_user_id", String.class, ParameterMode.IN)
						.registerStoredProcedureParameter("p_batch_flag", String.class, ParameterMode.IN)
						.registerStoredProcedureParameter("p_batch_name", String.class, ParameterMode.IN)
						.registerStoredProcedureParameter("p_request_id", Integer.class, ParameterMode.OUT)
						.registerStoredProcedureParameter("p_msg", String.class, ParameterMode.OUT)
						.setParameter("p_cloud_template_name", cloudTemplateName).setParameter("p_reprocess_flag", "Y")
						.setParameter("p_user_id", request.getHeader("userId")).setParameter("p_batch_flag", "Y").setParameter("p_batch_name", batchName);
				processReProcessesJobProcedure.execute();

				processJobPo
						.setRequestId((Integer) processReProcessesJobProcedure.getOutputParameterValue("p_request_id"));
				processJobPo.setMsg((String) processReProcessesJobProcedure.getOutputParameterValue("p_msg"));

			} else if ("Conversion".equalsIgnoreCase(type)) {
				StoredProcedureQuery processConversionJobProcedure = entityManager
						.createStoredProcedureQuery("XXR_CONVERSION_UTILS_V1_PKG.initiate_conversion")
						.registerStoredProcedureParameter("p_cloud_template_name", String.class, ParameterMode.IN)
						.registerStoredProcedureParameter("p_user_id", String.class, ParameterMode.IN)
						.registerStoredProcedureParameter("p_batch_flag", String.class, ParameterMode.IN)
						.registerStoredProcedureParameter("p_batch_name", String.class, ParameterMode.IN)
						.registerStoredProcedureParameter("p_request_id", Integer.class, ParameterMode.OUT)
						.registerStoredProcedureParameter("p_msg", String.class, ParameterMode.OUT)
						.setParameter("p_cloud_template_name", cloudTemplateName)
						.setParameter("p_user_id", request.getHeader("userId")).setParameter("p_batch_flag", "Y").setParameter("p_batch_name", batchName);
				processConversionJobProcedure.execute();
				processJobPo
						.setRequestId((Integer) processConversionJobProcedure.getOutputParameterValue("p_request_id"));
				processJobPo.setMsg((String) processConversionJobProcedure.getOutputParameterValue("p_msg"));

			}

			entityManager.clear();
			entityManager.close();
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return processJobPo;
	}

	public void cvrUpdate(String stagingTableName, Long sourceTemplateId, String batchName,String pLedgerName,String pCcidColumnName) {
		log.info("Start of cvrUpdate ##########");
		log.info(stagingTableName+":stagingTableName"+sourceTemplateId+":sourceTemplateId"+batchName+":batchName"+"pLedgerName:"+pLedgerName+"pCcidColumnName:"+pCcidColumnName);
		try {
			StoredProcedureQuery processConversionJobProcedure = entityManager.createStoredProcedureQuery("CR_CVR_UPDATE_PROC ")
					.registerStoredProcedureParameter("P_CLD_TABLE_NAME", String.class, ParameterMode.IN)
					.registerStoredProcedureParameter("P_SRC_TEMP_ID", Long.class, ParameterMode.IN)
					.registerStoredProcedureParameter("P_BATCH_NAME", String.class, ParameterMode.IN)
					.registerStoredProcedureParameter("P_LEDGER_NAME", String.class, ParameterMode.IN)
					.registerStoredProcedureParameter("P_CCID_COLUMN_NAME", String.class, ParameterMode.IN)
					.setParameter("P_CLD_TABLE_NAME", stagingTableName).setParameter("P_SRC_TEMP_ID", sourceTemplateId)
					.setParameter("P_BATCH_NAME", batchName).setParameter("P_LEDGER_NAME", pLedgerName).setParameter("P_CCID_COLUMN_NAME", pCcidColumnName);
			processConversionJobProcedure.execute();
		} finally {
			entityManager.clear();
			entityManager.close();
		}
	}

}
