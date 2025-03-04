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
import com.rite.products.convertrite.po.SaveFbdiTemplateHeaderPo;
import com.rite.products.convertrite.utils.Utils;

@Repository
public class SaveFbdiTemplateHeaderDaoImpl {
	
	@PersistenceContext
	private EntityManager entityManager;

	private static final Logger log = LoggerFactory.getLogger(SaveFbdiTemplateHeaderDaoImpl.class);

	@Autowired
	XxrFbdiTempHdrsRepository xxrFbdiTempHdrsRepository;

	@Transactional
	public String saveFbdiTemplateHdrs(List<SaveFbdiTemplateHeaderPo> fbdiTemplateHeaderPo,HttpServletRequest request)
			throws Exception, ValidationException {
		log.info("Start of saveFbdiTemplateHdrs Method in DaoImpl### ");
		String pmsg = "";
		try {
			StringBuilder sqlBuilder = new StringBuilder()
					.append("declare" + "   p_xxr_fbdi_temp_hdrs xxr_fbdi_temp_save_pkg.r_xxr_fbdi_temp_hdrs;"
							+ "p_user_id VARCHAR2(2000);"
							+ " p_msg VARCHAR2(2000);" + " begin ");
			sqlBuilder.append(
					" p_user_id\\:="+ "\'" + Utils.cleanEmail(request.getHeader("userId")) + "\';");	
			int i = 1;
			for (SaveFbdiTemplateHeaderPo fbdiHeader : fbdiTemplateHeaderPo) {

				
				if (fbdiHeader.getFbdiTemplateId() != null && fbdiHeader.getFbdiTemplateId() != 0)
					sqlBuilder.append(
							" p_xxr_fbdi_temp_hdrs(" + i + ").fbdi_template_id\\:=" + fbdiHeader.getFbdiTemplateId() + ";");
				else {
					Long  templateId = xxrFbdiTempHdrsRepository.getFbdiTemplateId(fbdiHeader.getFbdiTemplateName(),fbdiHeader.getVersion(),fbdiHeader.getSheetName(),fbdiHeader.getObjectId());
					if (templateId != null)
						throw new ValidationException("combination of FbdiTemplateName,objectCode and version already exists ");
					sqlBuilder.append(" p_xxr_fbdi_temp_hdrs(" + i + ").fbdi_template_id\\:=null;");
				}
				if (!Validations.isNullOrEmpty(fbdiHeader.getFbdiTemplateName()))
					sqlBuilder.append(" p_xxr_fbdi_temp_hdrs(" + i + ").fbdi_template_name\\:=" + "\'"
							+ fbdiHeader.getFbdiTemplateName() + "\';");
				sqlBuilder.append("  p_xxr_fbdi_temp_hdrs(" + i + ").pod_id\\:=" + fbdiHeader.getPodId() + ";");
				sqlBuilder.append("  p_xxr_fbdi_temp_hdrs(" + i + ").project_id\\:=" + fbdiHeader.getProjectId() + ";");
				sqlBuilder.append("  p_xxr_fbdi_temp_hdrs(" + i + ").parent_object_id\\:="
						+ fbdiHeader.getParentObjectId() + ";");
				sqlBuilder.append("  p_xxr_fbdi_temp_hdrs(" + i + ").object_id\\:=" + fbdiHeader.getObjectId() + ";");
				if (!Validations.isNullOrEmpty(fbdiHeader.getVersion()))
					sqlBuilder.append(" p_xxr_fbdi_temp_hdrs(" + i + ").version\\:=" + "\'"
							+ fbdiHeader.getVersion() + "\';");
				if (!Validations.isNullOrEmpty(fbdiHeader.getSheetName()))
					sqlBuilder.append(
							" p_xxr_fbdi_temp_hdrs(" + i + ").sheet_name\\:=" + "\'" + fbdiHeader.getSheetName() + "\';");
				if (!Validations.isNullOrEmpty(fbdiHeader.getSheetName()))
					sqlBuilder.append(
							" p_xxr_fbdi_temp_hdrs(" + i + ").sheet_name\\:=" + "\'" + fbdiHeader.getSheetName() + "\';");
				if (!Validations.isNullOrEmpty(fbdiHeader.getApi()))
					sqlBuilder.append(" p_xxr_fbdi_temp_hdrs(" + i + ").api\\:=" + "\'" + fbdiHeader.getApi() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(fbdiHeader.getAttribute1()))
					sqlBuilder.append(" p_xxr_fbdi_temp_hdrs(" + i + ").attribute1\\:=" + "\'"
							+ fbdiHeader.getAttribute1() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(fbdiHeader.getAttribute2()))
					sqlBuilder.append(" p_xxr_fbdi_temp_hdrs(" + i + ").attribute2\\:=" + "\'"
							+ fbdiHeader.getAttribute2() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(fbdiHeader.getAttribute3()))
					sqlBuilder.append(" p_xxr_fbdi_temp_hdrs(" + i + ").attribute3\\:=" + "\'"
							+ fbdiHeader.getAttribute3() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(fbdiHeader.getAttribute4()))
					sqlBuilder.append(" p_xxr_fbdi_temp_hdrs(" + i + ").attribute4\\:=" + "\'"
							+ fbdiHeader.getAttribute4() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(fbdiHeader.getAttribute5()))
					sqlBuilder.append(" p_xxr_fbdi_temp_hdrs(" + i + ").attribute5\\:=" + "\'"
							+ fbdiHeader.getAttribute5() + "\';");

				i++;
			}

			sqlBuilder.append(" xxr_fbdi_temp_save_pkg.xxr_fbdi_temp_hdrs_prc( p_xxr_fbdi_temp_hdrs,p_user_id,p_msg);");
			sqlBuilder.append(" end;");

			Query query = entityManager.createNativeQuery(sqlBuilder.toString());

			int count = query.executeUpdate();

			log.info("count" + count);

			pmsg = "Fbdi Template Header successfully saved";
			

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
