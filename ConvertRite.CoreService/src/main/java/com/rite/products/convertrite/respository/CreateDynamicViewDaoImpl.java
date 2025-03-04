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
import com.rite.products.convertrite.po.CreateDynamicViewPo;

@Repository
public class CreateDynamicViewDaoImpl {

	/*
	 * @Autowired EntityManagerFactory em;
	 */

	private static final Logger log = LoggerFactory.getLogger(CreateDynamicViewDaoImpl.class);

	@PersistenceContext
	private EntityManager entityManager;

	@Autowired
	XxrCloudTemplateHeadersRepository xxrCloudTemplateHeadersRepository;

	@Transactional
	public CreateDynamicViewPo createDynamicView(long templateId, String stgTableName,HttpServletRequest request) throws Exception {
		log.info("Start of createDynamicView Method in DaoImpl### ");
		CreateDynamicViewPo createDynamicViewPo = new CreateDynamicViewPo();
		/* EntityManager entityManager = em.createEntityManager(); */
		try {
			StoredProcedureQuery createDynamicViewProcedure = entityManager
					.createStoredProcedureQuery("XXR_CLOUD_PKG.XXR_CREATE_VIEW_PRC")
					.registerStoredProcedureParameter("p_template_id", Long.class, ParameterMode.IN)
					.registerStoredProcedureParameter("p_table_name", String.class, ParameterMode.IN)
					.registerStoredProcedureParameter("p_user_id", String.class, ParameterMode.IN)
					.registerStoredProcedureParameter("p_result", String.class, ParameterMode.OUT)
					.registerStoredProcedureParameter("p_view_name", String.class, ParameterMode.OUT)
					.setParameter("p_template_id", templateId).setParameter("p_table_name", stgTableName)
					.setParameter("p_user_id", request.getHeader("userId"));

			createDynamicViewProcedure.execute();
			createDynamicViewPo.setResult((String) createDynamicViewProcedure.getOutputParameterValue("p_result"));
			createDynamicViewPo.setViewName((String) createDynamicViewProcedure.getOutputParameterValue("p_view_name"));
			entityManager.clear();
			entityManager.close();

			if (createDynamicViewPo != null && !(createDynamicViewPo.getResult().isBlank())
					&& createDynamicViewPo.getResult().equalsIgnoreCase("Y")) {
				XxrCloudTemplateHeader tempHeader = xxrCloudTemplateHeadersRepository.getCloudTemplateById(templateId);
				tempHeader.setViewName(createDynamicViewPo.getViewName());
				xxrCloudTemplateHeadersRepository.save(tempHeader);
			}
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}

		return createDynamicViewPo;
	}

}
