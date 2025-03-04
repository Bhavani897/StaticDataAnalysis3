package com.rite.products.convertrite.respository;

import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.persistence.Query;
import javax.servlet.http.HttpServletRequest;
import javax.transaction.Transactional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.rite.products.convertrite.Validations.Validations;
import com.rite.products.convertrite.exception.ValidationException;
import com.rite.products.convertrite.po.SaveCloudTemplateHeadersPo;
import com.rite.products.convertrite.utils.Utils;

@Repository
public class SaveCloudTemplateHeadersDaoImpl {

	@PersistenceContext
	private EntityManager entityManager;

	private static final Logger log = LoggerFactory.getLogger(SaveCloudTemplateHeadersDaoImpl.class);

	@Autowired
	XxrCloudTemplateHeadersRepository xxrCloudTemplateHeadersRepository;

	@Transactional
	public String saveCloudHeaders(List<SaveCloudTemplateHeadersPo> cloudHeaders,HttpServletRequest request)
			throws Exception, ValidationException {
		log.info("Start of saveCloudHeaders Method in DaoImpl### ");
		String pmsg = "";
		try {
			StringBuilder sqlBuilder = new StringBuilder()
					.append("declare" + "   p_xxr_cld_temp_hdrs xxr_cld_temp_save_pkg.r_xxr_cld_temp_hdrs;"
							+ "p_user_id VARCHAR2(2000);"
							+ " p_msg VARCHAR2(2000);" + " begin ");
			sqlBuilder.append(
					" p_user_id\\:="+ "\'" + Utils.cleanEmail(request.getHeader("userId")) + "\';");	
			int i = 1;
			for (SaveCloudTemplateHeadersPo cloudHeader : cloudHeaders) {

				/*
				 * if (cloudHeader.getPodId() == null || cloudHeader.getProjectId() == null ||
				 * cloudHeader.getObjectId() == null || cloudHeader.getParentObjectId() == null
				 * || cloudHeader.getMetaDataTableId() == null ||
				 * Validations.isNullOrEmpty(cloudHeader.getTemplateName())) throw new
				 * ValidationException(
				 * "podId,projectId,parentObjectId,objectId,templateName and metaDataTableId are Mandatory fields"
				 * );
				 */
				if (cloudHeader.getTemplateId() != null && cloudHeader.getTemplateId() != 0)
					sqlBuilder.append(
							" p_xxr_cld_temp_hdrs(" + i + ").template_id\\:=" + cloudHeader.getTemplateId() + ";");
				else {
					Long templateId = xxrCloudTemplateHeadersRepository.getTemplateId(cloudHeader.getTemplateName());
					if (templateId != null)
						throw new ValidationException("This Cloud TemplateName already exists ");
					sqlBuilder.append(" p_xxr_cld_temp_hdrs(" + i + ").template_id\\:=null;");
				}
				if (!Validations.isNullOrEmpty(cloudHeader.getTemplateName()))
					sqlBuilder.append(" p_xxr_cld_temp_hdrs(" + i + ").template_name\\:=" + "\'"
							+ cloudHeader.getTemplateName() + "\';");
				//sqlBuilder.append("  p_xxr_cld_temp_hdrs(" + i + ").pod_id\\:=" + cloudHeader.getPodId() + ";");
				sqlBuilder.append("  p_xxr_cld_temp_hdrs(" + i + ").project_id\\:=" + cloudHeader.getProjectId() + ";");
				sqlBuilder.append("  p_xxr_cld_temp_hdrs(" + i + ").parent_object_id\\:="
						+ cloudHeader.getParentObjectId() + ";");
				sqlBuilder.append("  p_xxr_cld_temp_hdrs(" + i + ").object_id\\:=" + cloudHeader.getObjectId() + ";");
				sqlBuilder.append("  p_xxr_cld_temp_hdrs(" + i + ").metadata_table_id\\:="
						+ cloudHeader.getMetaDataTableId() + ";");
				if (cloudHeader.getSourceTemplateId() != null)
					sqlBuilder.append("  p_xxr_cld_temp_hdrs(" + i + ").src_template_id\\:="
							+ cloudHeader.getSourceTemplateId() + ";");
				if (!Validations.isNullOrEmpty(cloudHeader.getStagingTableName()))
					sqlBuilder.append(" p_xxr_cld_temp_hdrs(" + i + ").staging_table_name\\:=" + "\'"
							+ cloudHeader.getStagingTableName() + "\';");
				if (!Validations.isNullOrEmpty(cloudHeader.getTemplateCode()))
					sqlBuilder.append(" p_xxr_cld_temp_hdrs(" + i + ").TEMPLATE_CODE\\:=" + "\'"
							+ cloudHeader.getTemplateCode() + "\';");
				if (!Validations.isNullOrEmpty(cloudHeader.getPrimaryTemplateFlag()))
					sqlBuilder.append(" p_xxr_cld_temp_hdrs(" + i + ").PRIMARY_TEMPLATE_FLAG\\:=" + "\'"
							+ cloudHeader.getPrimaryTemplateFlag() + "\';");
				if (!Validations.isNullOrEmpty(cloudHeader.getViewName()))
					sqlBuilder.append(
							" p_xxr_cld_temp_hdrs(" + i + ").view_name\\:=" + "\'" + cloudHeader.getViewName() + "\';");
				/*
				 * if (!Validations.isNullOrEmpty(cloudHeader.getBu()))
				 * sqlBuilder.append(" p_xxr_cld_temp_hdrs(" + i + ").bu\\:=" + "\'" +
				 * cloudHeader.getBu() + "\';"); if
				 * (!Validations.isNullOrEmpty(cloudHeader.getBuSpecific()))
				 * sqlBuilder.append(" p_xxr_cld_temp_hdrs(" + i + ").bu_specific\\:=" + "\'" +
				 * cloudHeader.getBuSpecific() + "\';");
				 */
				if (!Validations.isNullOrEmpty(cloudHeader.getVersion()))
					sqlBuilder.append(" p_xxr_cld_temp_hdrs(" + i + ").version\\:=" + "\'"
							+ cloudHeader.getVersion() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(cloudHeader.getAttribute1()))
					sqlBuilder.append(" p_xxr_cld_temp_hdrs(" + i + ").attribute1\\:=" + "\'"
							+ cloudHeader.getAttribute1() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(cloudHeader.getAttribute2()))
					sqlBuilder.append(" p_xxr_cld_temp_hdrs(" + i + ").attribute2\\:=" + "\'"
							+ cloudHeader.getAttribute2() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(cloudHeader.getAttribute3()))
					sqlBuilder.append(" p_xxr_cld_temp_hdrs(" + i + ").attribute3\\:=" + "\'"
							+ cloudHeader.getAttribute3() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(cloudHeader.getAttribute4()))
					sqlBuilder.append(" p_xxr_cld_temp_hdrs(" + i + ").attribute4\\:=" + "\'"
							+ cloudHeader.getAttribute4() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(cloudHeader.getAttribute5()))
					sqlBuilder.append(" p_xxr_cld_temp_hdrs(" + i + ").attribute5\\:=" + "\'"
							+ cloudHeader.getAttribute5() + "\';");

				i++;
			}

			sqlBuilder.append(" xxr_cld_temp_save_pkg.xxr_cld_temp_hdrs_prc( p_xxr_cld_temp_hdrs,p_user_id,p_msg);");
			sqlBuilder.append(" end;");

			Query query = entityManager.createNativeQuery(sqlBuilder.toString());

			int count = query.executeUpdate();

			log.info("count" + count);

			pmsg = "Cloud Template successfully saved";
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
		} catch (ValidationException e) {
			pmsg = "Exception while saving data";
			throw new ValidationException(e.getMessage());
		} catch (Exception e) {
			pmsg = "Exception while saving data";
			throw new Exception(e.getMessage());
		}
		return pmsg;

	}
}
