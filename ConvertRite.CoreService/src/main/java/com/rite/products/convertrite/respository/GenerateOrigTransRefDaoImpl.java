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

@Repository
public class GenerateOrigTransRefDaoImpl {

	@PersistenceContext
	private EntityManager entityManager;

	private static final Logger log = LoggerFactory.getLogger(GenerateOrigTransRefDaoImpl.class);

	@Transactional
	public void generateOrigTranRef(long templateId, String tableName,String userId,String batchName) throws Exception {
		log.info("Start of generateOrigTranRef Method in DaoImpl--->"+templateId);
		try {
			StoredProcedureQuery generateOrigTransRef = entityManager
					.createStoredProcedureQuery("CR_POPULATE_ORIG_TRANS_ID_PROC")
					.registerStoredProcedureParameter("p_template_id", Long.class, ParameterMode.IN)
					.registerStoredProcedureParameter("p_table_name", String.class, ParameterMode.IN)
					.registerStoredProcedureParameter("p_user_id", String.class, ParameterMode.IN)
					.registerStoredProcedureParameter("p_batch_name", String.class, ParameterMode.IN)
					.registerStoredProcedureParameter("p_ret_code", String.class, ParameterMode.OUT)
					.registerStoredProcedureParameter("p_ret_msg", String.class, ParameterMode.OUT)

					.setParameter("p_table_name", tableName)
					.setParameter("p_template_id", templateId)
					.setParameter("p_user_id", userId)
					.setParameter("p_batch_name", batchName);
			generateOrigTransRef.execute();
			entityManager.clear();
			entityManager.close();
			String p_ret_msg = (String) generateOrigTransRef.getOutputParameterValue("p_ret_msg");
			log.info("Response message from generateOrigTranRef --->"+p_ret_msg);
		}catch(Exception e){
			log.info("Exception in  generateOrigTranRef --->"+e.getMessage());
			throw new Exception("Failed at generateOrigTranRef :"+e.getMessage());
		}
	}

	@Transactional
	public String generateReOrigTranRef(long templateId, String tableName, HttpServletRequest request, String batchName) throws Exception {
		log.info("Start of generateReOrigTranRef Method in DaoImpl### ");
		String p_ret_msg = null;
		try {
			StoredProcedureQuery generateOrigTransRef = entityManager
					.createStoredProcedureQuery("CR_POPULATE_ORIG_TRANS_ID_PROC")
					.registerStoredProcedureParameter("p_template_id", Long.class, ParameterMode.IN)
					.registerStoredProcedureParameter("p_table_name", String.class, ParameterMode.IN)
					.registerStoredProcedureParameter("p_user_id", String.class, ParameterMode.IN)
					.registerStoredProcedureParameter("p_batch_name", String.class, ParameterMode.IN)
					.registerStoredProcedureParameter("p_ret_code", String.class, ParameterMode.OUT)
					.registerStoredProcedureParameter("p_ret_msg", String.class, ParameterMode.OUT)

					.setParameter("p_table_name", tableName)
					.setParameter("p_template_id", templateId)
					.setParameter("p_user_id", request.getHeader("userId"))
					.setParameter("p_batch_name", batchName);
			generateOrigTransRef.execute();
//            String p_ret_code = (String) generateOrigTransRef.getOutputParameterValue("p_ret_code");
			p_ret_msg = (String) generateOrigTransRef.getOutputParameterValue("p_ret_msg");

			entityManager.clear();
			entityManager.close();
			log.info("Response message from generateReOrigTranRef --->"+p_ret_msg);
		} catch (Exception e) {
			log.info("Exception in generateReOrigTranRef --->"+p_ret_msg);
			throw new Exception("Failed at generateReOrigTranRef :" + e.getMessage());
		}
		return p_ret_msg;
	}

}
