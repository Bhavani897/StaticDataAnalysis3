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
public class ProjectEntryDaoImpl {

	private static final Logger log = LoggerFactory.getLogger(ProjectEntryDaoImpl.class);

	@PersistenceContext
	private EntityManager entityManager;

	@Transactional
	public String loadWbs(Long projectId, Long podId,HttpServletRequest request) throws Exception {
		log.info("Start of loadWbs Method in DaoImpl### ");
		String result = "";
		try {
			StoredProcedureQuery loadWbsProcedure = entityManager
					.createStoredProcedureQuery("XXR_PROJECT_SAVE_PKG.XXR_LOAD_WBS_PRC")
					.registerStoredProcedureParameter("p_project_id", Long.class, ParameterMode.IN)
					.registerStoredProcedureParameter("p_pod_id", Long.class, ParameterMode.IN)
					.registerStoredProcedureParameter("p_user_id", String.class, ParameterMode.IN)
					.registerStoredProcedureParameter("p_msg", String.class, ParameterMode.OUT)
					.setParameter("p_project_id", projectId).setParameter("p_pod_id", podId)
					.setParameter("p_user_id", request.getHeader("userId"));
			loadWbsProcedure.execute();
			result = (String) loadWbsProcedure.getOutputParameterValue("p_msg");
			entityManager.clear();
			entityManager.close();
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return result;
	}

	@Transactional
	public String loadTask(Long projectId, Long podId,HttpServletRequest request) throws Exception {
		log.info("Start of loadTask Method in DaoImpl### ");
		String result = "";
		try {
			StoredProcedureQuery loadTaskProcedure = entityManager
					.createStoredProcedureQuery("XXR_PROJECT_SAVE_PKG.XXR_LOAD_TASK_PRC")
					.registerStoredProcedureParameter("p_project_id", Long.class, ParameterMode.IN)
					.registerStoredProcedureParameter("p_pod_id", Long.class, ParameterMode.IN)
					.registerStoredProcedureParameter("p_user_id", String.class, ParameterMode.IN)
					.registerStoredProcedureParameter("p_msg", String.class, ParameterMode.OUT)
					.setParameter("p_project_id", projectId).setParameter("p_pod_id", podId)
					.setParameter("p_user_id", request.getHeader("userId"));
			loadTaskProcedure.execute();
			result = (String) loadTaskProcedure.getOutputParameterValue("p_msg");
			entityManager.clear();
			entityManager.close();
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return result;
	}

}
