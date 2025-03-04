package com.rite.products.convertrite.respository;

import java.text.SimpleDateFormat;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.persistence.Query;
import javax.servlet.http.HttpServletRequest;
import javax.transaction.Transactional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import com.rite.products.convertrite.Validations.Validations;
import com.rite.products.convertrite.po.SaveProjectHeadersPo;
import com.rite.products.convertrite.utils.Utils;

@Repository
public class SaveProjectHeaderDaoImpl {

	@PersistenceContext
	private EntityManager entityManager;

	private static final Logger log = LoggerFactory.getLogger(SaveProjectHeaderDaoImpl.class);

	@Transactional
	public String saveProjectHeader(SaveProjectHeadersPo projectHeadersPo, HttpServletRequest request)
			throws Exception {
		log.info("Start of saveProjectHeader Method in DaoImpl### ");
		String pmsg = "";
		try {
			StringBuilder sqlBuilder = new StringBuilder()
					.append("declare" + "   p_xxr_projects xxr_project_save_pkg.r_xxr_projects;"
							+ "p_user_id VARCHAR2(2000);" + " p_msg VARCHAR2(2000);" + " begin ");
			sqlBuilder.append(" p_user_id\\:=" + "\'" + Utils.cleanEmail(request.getHeader("userId")) + "\';");

			int i = 1;
			/*
			 * if (projectHeadersPo.getPodId() == null ||
			 * Validations.isNullOrEmpty(projectHeadersPo.getProjectName()) ||
			 * Validations.isNullOrEmpty(projectHeadersPo.getProjectManager()) ||
			 * Validations.isNullOrEmpty(projectHeadersPo.getProgramNumber()) ||
			 * Validations.isNullOrEmpty(projectHeadersPo.getClientManager()) ||
			 * Validations.isNullOrEmpty(projectHeadersPo.getKpiAggregationLevel()) ||
			 * Validations.isNullOrEmpty(projectHeadersPo.getProjectStatus()) ||
			 * Validations.isNullOrEmpty(projectHeadersPo.getAccessLevel()) ||
			 * projectHeadersPo.getStartDate()==null) throw new Exception(
			 * "podId,projectName,projectManager,programNumber,clientManager,kpiAggregartionLevel,startDate,projectStatus and accessLevel are Mandatory fields"
			 * );
			 */

			if (projectHeadersPo.getProjectId() != null)
				sqlBuilder.append(" p_xxr_projects(" + i + ").project_id\\:=" + projectHeadersPo.getProjectId() + ";");
			sqlBuilder.append("  p_xxr_projects(" + i + ").pod_id\\:=" + projectHeadersPo.getPodId() + ";");
			if (!Validations.isNullOrEmpty(projectHeadersPo.getProjectName()))
				sqlBuilder.append(" p_xxr_projects(" + i + ").project_name\\:=" + "\'"
						+ projectHeadersPo.getProjectName() + "\';");
			if (!Validations.isNullOrEmpty(projectHeadersPo.getDescription()))
				sqlBuilder.append(" p_xxr_projects(" + i + ").description\\:=" + "\'"
						+ projectHeadersPo.getDescription() + "\';");
			if (!Validations.isNullOrEmpty(projectHeadersPo.getProjectManager()))
				sqlBuilder.append(" p_xxr_projects(" + i + ").project_manager\\:=" + "\'"
						+ projectHeadersPo.getProjectManager() + "\';");
			if (!Validations.isNullOrEmpty(projectHeadersPo.getProgramNumber()))
				sqlBuilder.append(" p_xxr_projects(" + i + ").program_number\\:=" + "\'"
						+ projectHeadersPo.getProgramNumber() + "\';");
			if (!Validations.isNullOrEmpty(projectHeadersPo.getClientManager()))
				sqlBuilder.append(" p_xxr_projects(" + i + ").client_manager\\:=" + "\'"
						+ projectHeadersPo.getClientManager() + "\';");
			if (!Validations.isNullOrEmptyorWhiteSpace(projectHeadersPo.getKpiAggregationLevel()))
				sqlBuilder.append(" p_xxr_projects(" + i + ").kpi_agg_level\\:=" + "\'"
						+ projectHeadersPo.getKpiAggregationLevel() + "\';");
			if (projectHeadersPo.getStartDate() != null)
				sqlBuilder.append(" p_xxr_projects(" + i + ").start_date\\:=" + "\'"
						+ new SimpleDateFormat("dd-MMM-yyyy").format(projectHeadersPo.getStartDate()) + "\';");
			else
				sqlBuilder.append(" p_xxr_projects(" + i + ").start_date\\:=null;");
			if (projectHeadersPo.getCompletionDate() != null)
				sqlBuilder.append(" p_xxr_projects(" + i + ").end_date\\:=" + "\'"
						+ new SimpleDateFormat("dd-MMM-yyyy").format(projectHeadersPo.getCompletionDate()) + "\';");
			else
				sqlBuilder.append(" p_xxr_projects(" + i + ").end_date\\:=null;");
			if (!Validations.isNullOrEmptyorWhiteSpace(projectHeadersPo.getProjectStatus()))
				sqlBuilder.append(" p_xxr_projects(" + i + ").project_status\\:=" + "\'"
						+ projectHeadersPo.getProjectStatus() + "\';");
			if (!Validations.isNullOrEmptyorWhiteSpace(projectHeadersPo.getAccessLevel()))
				sqlBuilder.append(" p_xxr_projects(" + i + ").access_level\\:=" + "\'"
						+ projectHeadersPo.getAccessLevel() + "\';");
			/*
			 * if
			 * (!Validations.isNullOrEmptyorWhiteSpace(projectHeadersPo.getIterationType()))
			 * sqlBuilder.append(" p_xxr_projects(" + i + ").iteration_type\\:=" + "\'" +
			 * projectHeadersPo.getIterationType() + "\';");
			 */
			if (!Validations.isNullOrEmptyorWhiteSpace(projectHeadersPo.getClientProjectNum()))
				sqlBuilder.append(" p_xxr_projects(" + i + ").client_project_number\\:=" + "\'"
						+ projectHeadersPo.getClientProjectNum() + "\';");
			if (!Validations.isNullOrEmptyorWhiteSpace(projectHeadersPo.getAttribute1()))
				sqlBuilder.append(
						" p_xxr_projects(" + i + ").attribute1\\:=" + "\'" + projectHeadersPo.getAttribute1() + "\';");
			if (!Validations.isNullOrEmptyorWhiteSpace(projectHeadersPo.getAttribute2()))
				sqlBuilder.append(
						" p_xxr_projects(" + i + ").attribute2\\:=" + "\'" + projectHeadersPo.getAttribute2() + "\';");
			if (!Validations.isNullOrEmptyorWhiteSpace(projectHeadersPo.getAttribute3()))
				sqlBuilder.append(
						" p_xxr_projects(" + i + ").attribute3\\:=" + "\'" + projectHeadersPo.getAttribute3() + "\';");
			if (!Validations.isNullOrEmptyorWhiteSpace(projectHeadersPo.getAttribute4()))
				sqlBuilder.append(
						" p_xxr_projects(" + i + ").attribute4\\:=" + "\'" + projectHeadersPo.getAttribute4() + "\';");
			if (!Validations.isNullOrEmptyorWhiteSpace(projectHeadersPo.getAttribute5()))
				sqlBuilder.append(
						" p_xxr_projects(" + i + ").attribute5\\:=" + "\'" + projectHeadersPo.getAttribute5() + "\';");

			sqlBuilder.append(" xxr_project_save_pkg.xxr_projects_prc(p_xxr_projects,p_user_id,p_msg);");
			sqlBuilder.append(" end;");

			Query query = entityManager.createNativeQuery(sqlBuilder.toString());

			int count = query.executeUpdate();

			log.info("count" + count);

			pmsg = "Project Header successfully saved";
			/*
			 * StoredProcedureQuery saveSourceTemplateHeaders = entityManager
			 * .createStoredProcedureQuery("xxr_src_tem_save_pkg.xxr_src_tem_hdrs")
			 * .registerStoredProcedureParameter("p_xxr_src_tem_hdrs",
			 * r_xxr_hdrs_tabtype.class, ParameterMode.IN)
			 * .registerStoredProcedureParameter("p_msg", String.class, ParameterMode.OUT)
			 * .setParameter("p_xxr_src_tem_hdrs", sourceTemplateHeadersPo.get(0));
			 * 
			 * saveSourceTemplateHeaders.execute();
			 */

			entityManager.clear();
			entityManager.close();
		} catch (Exception e) {
			pmsg = "Error  while saving Project Header";
			e.printStackTrace();
			//throw new Exception(e.getMessage());
		}
		return pmsg;

	}

}
