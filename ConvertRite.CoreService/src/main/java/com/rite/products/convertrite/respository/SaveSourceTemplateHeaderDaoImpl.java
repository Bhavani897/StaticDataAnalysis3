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
import com.rite.products.convertrite.po.SaveSourceTemplateHeadersPo;
import com.rite.products.convertrite.utils.Utils;

@Repository
public class SaveSourceTemplateHeaderDaoImpl {

	@PersistenceContext
	private EntityManager entityManager;

	private static final Logger log = LoggerFactory.getLogger(SaveSourceTemplateHeaderDaoImpl.class);

	@Autowired
	SourceTemplateHeadersRepository sourceTemplateHeadersRepository;

	@Transactional
	public String saveSourceTemplateHeaders(List<SaveSourceTemplateHeadersPo> sourceHeaders, HttpServletRequest request)
			throws ValidationException, Exception {
		log.info("Start of saveSourceTemplateHeaders Method in DaoImpl### ");
		String pmsg = "";
		try {
			StringBuilder sqlBuilder = new StringBuilder()
					.append("declare" + "  p_xxr_src_temp_hdrs xxr_src_temp_save_pkg.r_xxr_src_temp_hdrs;"
							+ "p_user_id VARCHAR2(2000);" + " p_msg VARCHAR2(2000);" + " begin ");
			sqlBuilder.append(" p_user_id\\:=" + "\'" + Utils.cleanEmail(request.getHeader("userId")) + "\';");
			int i = 1;
			for (SaveSourceTemplateHeadersPo sourceHeader : sourceHeaders) {

				/*
				 * if (sourceHeader.getPodId() == null || sourceHeader.getObjectId() == null ||
				 * sourceHeader.getParentObjectId() == null ||
				 * Validations.isNullOrEmpty(sourceHeader.getTemplateName()) ||
				 * sourceHeader.getProjectId() == null) throw new Exception(
				 * "pod_id,objectId,parentObjectId,templateName and projectId are Mandatory fields"
				 * );
				 */
				if (sourceHeader.getTemplateId() != null && sourceHeader.getTemplateId() != 0)
					sqlBuilder.append(
							" p_xxr_src_temp_hdrs(" + i + ").template_id\\:=" + sourceHeader.getTemplateId() + ";");
				else {

					Long templateId = sourceTemplateHeadersRepository.getTemplateId(sourceHeader.getTemplateName());
					if (templateId != null)
						throw new ValidationException("This source TemplateName already exists ");
					sqlBuilder.append(" p_xxr_src_temp_hdrs(" + i + ").template_id\\:=null;");
				}
				if (!Validations.isNullOrEmpty(sourceHeader.getTemplateName()))
					sqlBuilder.append(" p_xxr_src_temp_hdrs(" + i + ").template_name\\:=" + "\'"
							+ sourceHeader.getTemplateName() + "\';");
				/*
				 * sqlBuilder.append("  p_xxr_src_temp_hdrs(" + i + ").pod_id\\:=" +
				 * sourceHeader.getPodId() + ";"); if
				 * (!Validations.isNullOrEmpty(sourceHeader.getBu()))
				 * sqlBuilder.append(" p_xxr_src_temp_hdrs(" + i + ").bu\\:=" + "\'" +
				 * sourceHeader.getBu() + "\';"); if
				 * (!Validations.isNullOrEmpty(sourceHeader.getBuSpecific()))
				 * sqlBuilder.append(" p_xxr_src_temp_hdrs(" + i + ").bu_specific\\:=" + "\'" +
				 * sourceHeader.getBuSpecific() + "\';");
				 */
				if (!Validations.isNullOrEmpty(sourceHeader.getNormalizeDataFlag()))
					sqlBuilder.append(" p_xxr_src_temp_hdrs(" + i + ").NORMALIZE_DATA_FLAG\\:=" + "\'"
							+ sourceHeader.getNormalizeDataFlag() + "\';");
				if (!Validations.isNullOrEmpty(sourceHeader.getTemplateCode()))
					sqlBuilder.append(" p_xxr_src_temp_hdrs(" + i + ").TEMPLATE_CODE\\:=" + "\'"
							+ sourceHeader.getTemplateCode() + "\';");
				sqlBuilder
						.append("  p_xxr_src_temp_hdrs(" + i + ").project_id\\:=" + sourceHeader.getProjectId() + ";");
				sqlBuilder.append("  p_xxr_src_temp_hdrs(" + i + ").parent_object_id\\:="
						+ sourceHeader.getParentObjectId() + ";");
				sqlBuilder.append("  p_xxr_src_temp_hdrs(" + i + ").object_id\\:=" + sourceHeader.getObjectId() + ";");
				if (sourceHeader.getMetaDataTableId() != null)
					sqlBuilder.append("  p_xxr_src_temp_hdrs(" + i + ").metadata_table_id\\:="
							+ sourceHeader.getMetaDataTableId() + ";");
				if (!Validations.isNullOrEmptyorWhiteSpace(sourceHeader.getStagingTableName()))
					sqlBuilder.append(" p_xxr_src_temp_hdrs(" + i + ").staging_table_name\\:=" + "\'"
							+ sourceHeader.getStagingTableName() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(sourceHeader.getViewName()))
					sqlBuilder.append(" p_xxr_src_temp_hdrs(" + i + ").view_name\\:=" + "\'"
							+ sourceHeader.getViewName() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(sourceHeader.getAttribute1()))
					sqlBuilder.append(" p_xxr_src_temp_hdrs(" + i + ").attribute1\\:=" + "\'"
							+ sourceHeader.getAttribute1() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(sourceHeader.getAttribute2()))
					sqlBuilder.append(" p_xxr_src_temp_hdrs(" + i + ").attribute2\\:=" + "\'"
							+ sourceHeader.getAttribute2() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(sourceHeader.getAttribute3()))
					sqlBuilder.append(" p_xxr_src_temp_hdrs(" + i + ").attribute3\\:=" + "\'"
							+ sourceHeader.getAttribute3() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(sourceHeader.getAttribute4()))
					sqlBuilder.append(" p_xxr_src_temp_hdrs(" + i + ").attribute4\\:=" + "\'"
							+ sourceHeader.getAttribute4() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(sourceHeader.getAttribute5()))
					sqlBuilder.append(" p_xxr_src_temp_hdrs(" + i + ").attribute5\\:=" + "\'"
							+ sourceHeader.getAttribute5() + "\';");

				i++;
			}

			sqlBuilder.append(" xxr_src_temp_save_pkg.xxr_src_temp_hdrs_prc(p_xxr_src_temp_hdrs,p_user_id,p_msg);");
			sqlBuilder.append(" end;");

			Query query = entityManager.createNativeQuery(sqlBuilder.toString());
			int count = query.executeUpdate();
			log.info("count" + count);
			pmsg = "Source Template successfully saved";
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
