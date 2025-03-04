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

import com.rite.products.convertrite.model.SourceTemplateHeaders;
import com.rite.products.convertrite.po.CreateDynamicViewPo;
@Repository
public class CreateSourceDynamicViewDaoImpl {
	
	@PersistenceContext
	private EntityManager entityManager;
	
	@Autowired
	SourceTemplateHeadersRepository sourceTemplateHeadersRepository;

	private static final Logger log = LoggerFactory.getLogger(CreateDynamicViewDaoImpl.class);

	@Transactional
	public CreateDynamicViewPo createDynamicView(long templateId, String stgTableName,HttpServletRequest request) {
		log.info("Start of createDynamicView Method in DaoImpl### ");
		CreateDynamicViewPo createDynamicViewPo = new CreateDynamicViewPo();
		StoredProcedureQuery createDynamicViewProcedure = entityManager
				.createStoredProcedureQuery("XXR_SOURCE_PKG.XXR_CREATE_VIEW_PRC")
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
		
		log.info("Status::::::"+createDynamicViewPo.getResult()+"viewName:::"+createDynamicViewPo.getViewName());
		if(createDynamicViewPo!=null && !(createDynamicViewPo.getResult().isBlank()) && createDynamicViewPo.getResult().equalsIgnoreCase("Y")) {
		SourceTemplateHeaders tempHeader = sourceTemplateHeadersRepository.findByTemplateId(templateId).get(0);
		tempHeader.setViewName(createDynamicViewPo.getViewName());
		sourceTemplateHeadersRepository.save(tempHeader);
		}
		return createDynamicViewPo;
	}

}
