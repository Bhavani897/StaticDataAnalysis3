package com.rite.products.convertrite.respository;

import java.text.SimpleDateFormat;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.persistence.Query;
import javax.servlet.http.HttpServletRequest;
import javax.transaction.Transactional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import com.rite.products.convertrite.Validations.Validations;
import com.rite.products.convertrite.exception.BadRequestException;
import com.rite.products.convertrite.po.ActivitiesPo;
import com.rite.products.convertrite.utils.Utils;

@Repository
public class SaveProjectActivitiesDaoImpl {

	@PersistenceContext
	private EntityManager entityManager;

	private static final Logger log = LoggerFactory.getLogger(SaveProjectActivitiesDaoImpl.class);

	@Transactional
	public String saveProjectActivities(List<ActivitiesPo> activitiesPo, HttpServletRequest request)
			throws BadRequestException, Exception {
		log.info("Start of saveProjectActivities Method in DaoImpl### ");

		String pmsg = "";
		try {
			StringBuilder sqlBuilder = new StringBuilder()
					.append("declare" + "   p_xxr_proj_activities  xxr_project_save_pkg.r_xxr_proj_activities;"
							+ "p_user_id VARCHAR2(2000);" + " p_msg VARCHAR2(2000);" + " begin ");

			sqlBuilder.append(" p_user_id\\:=" + "\'" +  Utils.cleanEmail(request.getHeader("userId")) + "\';");

			int i = 1;
			for (ActivitiesPo activities : activitiesPo) {

				if (Validations.isNullOrEmpty(activities.getTaskNum())
						|| Validations.isNullOrEmpty(activities.getTaskName())
						|| Validations.isNullOrEmpty(activities.getTaskType()) || activities.getObjectId() == null
						|| activities.getParentObjectId() == null || activities.getProjectId() == null)
					throw new BadRequestException(
							"taskNum,taskName,taskType,projectId,parentObjectId and objectId are Mandatory fields");

				sqlBuilder.append(" p_xxr_proj_activities(" + i + ").project_id\\:=" + activities.getProjectId() + ";");
				if (activities.getTaskId() != null)
					sqlBuilder.append("  p_xxr_proj_activities(" + i + ").task_id\\:=" + activities.getTaskId() + ";");
				if (activities.getSeq() != null)
					sqlBuilder.append("  p_xxr_proj_activities(" + i + ").seq\\:=" + activities.getSeq() + ";");
				if (activities.getPodId() != null)
					sqlBuilder.append("  p_xxr_proj_activities(" + i + ").pod_id\\:=" + activities.getPodId() + ";");
				if (activities.getWbsId() != null)
					sqlBuilder.append("  p_xxr_proj_activities(" + i + ").wbs_id\\:=" + activities.getWbsId() + ";");
				sqlBuilder.append(
						" p_xxr_proj_activities(" + i + ").task_num\\:=" + "\'" + activities.getTaskNum() + "\';");
				sqlBuilder.append(
						" p_xxr_proj_activities(" + i + ").task_name\\:=" + "\'" + activities.getTaskName() + "\';");
				sqlBuilder.append(" p_xxr_proj_activities(" + i + ").object_id\\:=" + activities.getObjectId() + ";");
				sqlBuilder.append(" p_xxr_proj_activities(" + i + ").parent_object_id\\:="
						+ activities.getParentObjectId() + ";");
				if (!Validations.isNullOrEmptyorWhiteSpace(activities.getTaskType()))
					sqlBuilder.append(" p_xxr_proj_activities(" + i + ").task_type\\:=" + "\'"
							+ activities.getTaskType() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(activities.getPreReqTask()))
					sqlBuilder.append(" p_xxr_proj_activities(" + i + ").pre_req_task\\:=" + "\'"
							+ activities.getPreReqTask() + "\';");
				if (activities.getWeightage() != null)
					sqlBuilder.append(
							"  p_xxr_proj_activities(" + i + ").weightage\\:=" + activities.getWeightage() + ";");
				if (activities.getCompletePercentage() != null)
					sqlBuilder.append("  p_xxr_proj_activities(" + i + ").complete_percentage\\:="
							+ activities.getCompletePercentage() + ";");
				if (activities.getLegacyResourceId() != null)
					sqlBuilder.append("  p_xxr_proj_activities(" + i + ").legacy_resource_id\\:="
							+ activities.getLegacyResourceId() + ";");
				if (!Validations.isNullOrEmptyorWhiteSpace(activities.getTaskStatus()))
					sqlBuilder.append(" p_xxr_proj_activities(" + i + ").task_status\\:=" + "\'"
							+ activities.getTaskStatus() + "\';");
				if (activities.getDestinationResourceId() != null)
					sqlBuilder.append("  p_xxr_proj_activities(" + i + ").destination_resource_id\\:="
							+ activities.getDestinationResourceId() + ";");
				if (activities.getTaskOwnerId() != null)
					sqlBuilder.append(
							"  p_xxr_proj_activities(" + i + ").task_owner_id\\:=" + activities.getTaskOwnerId() + ";");
				if (!Validations.isNullOrEmptyorWhiteSpace(activities.getCompletionFlag()))
					sqlBuilder.append(" p_xxr_proj_activities(" + i + ").completion_flag\\:=" + "\'"
							+ activities.getCompletionFlag() + "\';");
				if (activities.getCloudResourceId() != null)
					sqlBuilder.append("  p_xxr_proj_activities(" + i + ").cloud_resource_id\\:="
							+ activities.getCloudResourceId() + ";");
				if (activities.getIntegratorResourceId() != null)
					sqlBuilder.append("  p_xxr_proj_activities(" + i + ").integrator_resource_id\\:="
							+ activities.getIntegratorResourceId() + ";");
				if (activities.getClientResourceId() != null)
					sqlBuilder.append("  p_xxr_proj_activities(" + i + ").client_resource_id\\:="
							+ activities.getClientResourceId() + ";");
				if (activities.getStartDate() != null)
					sqlBuilder.append(" p_xxr_proj_activities(" + i + ").start_date\\:=" + "\'"
							+ new SimpleDateFormat("dd-MMM-yyyy").format(activities.getStartDate()) + "\';");
				else
					sqlBuilder.append(" p_xxr_proj_activities(" + i + ").start_date\\:=null;");
				if (activities.getEndDate() != null)
					sqlBuilder.append(" p_xxr_proj_activities(" + i + ").end_date\\:=" + "\'"
							+ new SimpleDateFormat("dd-MMM-yyyy").format(activities.getEndDate()) + "\';");
				else
					sqlBuilder.append(" p_xxr_proj_activities(" + i + ").end_date\\:=null;");
				if (!Validations.isNullOrEmptyorWhiteSpace(activities.getAttribute1()))
					sqlBuilder.append(" p_xxr_proj_activities(" + i + ").attribute1\\:=" + "\'"
							+ activities.getAttribute1() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(activities.getAttribute2()))
					sqlBuilder.append(" p_xxr_proj_activities(" + i + ").attribute2\\:=" + "\'"
							+ activities.getAttribute2() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(activities.getAttribute3()))
					sqlBuilder.append(" p_xxr_proj_activities(" + i + ").attribute3\\:=" + "\'"
							+ activities.getAttribute3() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(activities.getAttribute4()))
					sqlBuilder.append(" p_xxr_proj_activities(" + i + ").attribute4\\:=" + "\'"
							+ activities.getAttribute4() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(activities.getAttribute5()))
					sqlBuilder.append(" p_xxr_proj_activities(" + i + ").attribute5\\:=" + "\'"
							+ activities.getAttribute5() + "\';");

				i++;
			}

			sqlBuilder.append(" xxr_project_save_pkg.xxr_proj_activities_prc(p_xxr_proj_activities,p_user_id,p_msg);");
			sqlBuilder.append(" end;");

			Query query = entityManager.createNativeQuery(sqlBuilder.toString());

			int count = query.executeUpdate();

			log.info("count" + count);

			pmsg = "Project Activities saved successfully";
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
		} catch (BadRequestException e) {
			pmsg = "Exception while saving Project Activities";
			throw new BadRequestException(e.getMessage());
		} catch (Exception e) {
			pmsg = "Exception while saving Project Activities";
			throw new Exception(e.getMessage());
		}
		return pmsg;

	}

}
