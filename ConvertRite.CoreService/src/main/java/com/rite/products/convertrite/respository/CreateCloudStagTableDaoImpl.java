package com.rite.products.convertrite.respository;

import javax.persistence.EntityManager;
import javax.persistence.ParameterMode;
import javax.persistence.PersistenceContext;
import javax.persistence.StoredProcedureQuery;
import javax.servlet.http.HttpServletRequest;
import javax.transaction.Transactional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.rite.products.convertrite.model.XxrCloudTemplateHeader;
import com.rite.products.convertrite.po.CloudStagingTablePo;

@Repository
public class CreateCloudStagTableDaoImpl {

	/*
	 * @Autowired EntityManagerFactory em;
	 */

	private static final Logger log = LoggerFactory.getLogger(CreateCloudStagTableDaoImpl.class);

	@PersistenceContext
	private EntityManager entityManager;

	@Autowired
	XxrCloudTemplateHeadersRepository xxrCloudTemplateHeadersRepository;

	@Transactional
	public CloudStagingTablePo createStaggingTable(String tableName, long templateId,HttpServletRequest request) throws Exception {
		log.info("Start of createStaggingTable Method in DaoImpl### ");
		CloudStagingTablePo stagingPo = new CloudStagingTablePo();
		/* EntityManager entityManager = em.createEntityManager(); */
		try {
			StoredProcedureQuery createStaggingStoredProcedure = entityManager
					.createStoredProcedureQuery("XXR_CLOUD_PKG.XXR_CREATE_STG_TAB_PRC")
					//.registerStoredProcedureParameter("p_pod", String.class, ParameterMode.IN)
					.registerStoredProcedureParameter("p_table_name", String.class, ParameterMode.IN)
					.registerStoredProcedureParameter("p_template_id", Long.class, ParameterMode.IN)
					.registerStoredProcedureParameter("p_user_id", String.class, ParameterMode.IN)
					.registerStoredProcedureParameter("p_result", String.class, ParameterMode.OUT)
					.registerStoredProcedureParameter("p_table_name_out", String.class, ParameterMode.OUT)
					.setParameter("p_table_name", tableName)
					.setParameter("p_template_id", templateId).setParameter("p_user_id", request.getHeader("userId"));

			createStaggingStoredProcedure.execute();
			stagingPo.setResult((String) createStaggingStoredProcedure.getOutputParameterValue("p_result"));
			stagingPo.setTableName((String) createStaggingStoredProcedure.getOutputParameterValue("p_table_name_out"));
			log.info("p_result::::" + createStaggingStoredProcedure.getOutputParameterValue("p_result"));
			log.info("p_result::::" + createStaggingStoredProcedure.getOutputParameterValue("p_table_name_out"));
			entityManager.clear();
			entityManager.close();

			if (stagingPo != null && !(stagingPo.getResult().isBlank())
					&& stagingPo.getResult().equalsIgnoreCase("Y")) {
				XxrCloudTemplateHeader tempHeader = xxrCloudTemplateHeadersRepository.getCloudTemplateById(templateId);
				tempHeader.setStagingTableName(stagingPo.getTableName().split(":")[1]);
				xxrCloudTemplateHeadersRepository.save(tempHeader);
			}
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return stagingPo;
	}

}
