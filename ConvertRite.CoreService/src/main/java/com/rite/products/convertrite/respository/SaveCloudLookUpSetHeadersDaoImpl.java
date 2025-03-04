package com.rite.products.convertrite.respository;

import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.persistence.Query;
import javax.transaction.Transactional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import com.rite.products.convertrite.Validations.Validations;
import com.rite.products.convertrite.po.SaveCloudLookUpSetHeadersPo;

@Repository
public class SaveCloudLookUpSetHeadersDaoImpl {
	
	@PersistenceContext
	private EntityManager entityManager;

	private static final Logger log = LoggerFactory.getLogger(SaveCloudLookUpSetHeadersDaoImpl.class);

	@Transactional
	public String saveCloudLookUpSetHeaders(List<SaveCloudLookUpSetHeadersPo> cloudLookUpSetHeadersPo) throws Exception {
		log.info("Start of saveCloudLookUpSetHeaders Method in DaoImpl### ");
		String pmsg = "";
		try {
			StringBuilder sqlBuilder = new StringBuilder()
					.append("declare" + "    p_xxr_cld_lookup_hdrs xxr_cld_lookup_save_pkg.r_xxr_hdrs_tabtype;"
							+ " p_msg VARCHAR2(2000);" + " begin ");
			int i = 1;
			for (SaveCloudLookUpSetHeadersPo cloudLookUpSetHeader : cloudLookUpSetHeadersPo) {

				if ( Validations.isNullOrEmpty(cloudLookUpSetHeader.getColumnName()))
					throw new Exception(
							"columnName is Mandatory fields");
				
				if(cloudLookUpSetHeader.getLookUpSetId()!=null && cloudLookUpSetHeader.getLookUpSetId()!= 0 )
				sqlBuilder
						.append(" p_xxr_cld_lookup_hdrs(" + i + ").lookup_set_id\\:=" + cloudLookUpSetHeader.getLookUpSetId() + ";");
				else {
					sqlBuilder
					.append(" p_xxr_cld_lookup_hdrs(" + i + ").lookup_set_id\\:= null;");
				}	
				if(cloudLookUpSetHeader.getBu()!=null)
				sqlBuilder.append(" p_xxr_cld_lookup_hdrs(" + i + ").bu\\:="+ cloudLookUpSetHeader.getBu() +";");
				if (!Validations.isNullOrEmptyorWhiteSpace(cloudLookUpSetHeader.getBuSpecific()))
				sqlBuilder.append(
						" p_xxr_cld_lookup_hdrs(" + i + ").bu_specific\\:=" + "\'" + cloudLookUpSetHeader.getBuSpecific() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(cloudLookUpSetHeader.getObjectCode()))
				sqlBuilder.append(" p_xxr_cld_lookup_hdrs(" + i + ").saas_object_code\\:=" + "\'"
						+ cloudLookUpSetHeader.getObjectCode() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(cloudLookUpSetHeader.getParentObjectCode()))
				sqlBuilder.append(" p_xxr_cld_lookup_hdrs(" + i + ").saas_parent_object_code\\:=" + "\'"
						+ cloudLookUpSetHeader.getParentObjectCode() + "\';");
				if(cloudLookUpSetHeader.getColumnId()!=null)
				sqlBuilder.append("  p_xxr_cld_lookup_hdrs(" + i + ").column_id\\:=" + cloudLookUpSetHeader.getColumnId() + ";");
				if (!Validations.isNullOrEmptyorWhiteSpace(cloudLookUpSetHeader.getColumnName()))
				sqlBuilder.append(" p_xxr_cld_lookup_hdrs(" + i + ").column_name\\:=" + "\'"
						+ cloudLookUpSetHeader.getColumnName() + "\';");
				if(cloudLookUpSetHeader.getPodId()!=null)
					sqlBuilder.append("  p_xxr_cld_lookup_hdrs(" + i + ").pod_id\\:=" + cloudLookUpSetHeader.getPodId() + ";");
				if (!Validations.isNullOrEmptyorWhiteSpace(cloudLookUpSetHeader.getPodName()))
					sqlBuilder.append(" p_xxr_cld_lookup_hdrs(" + i + ").pod_name\\:=" + "\'"
							+ cloudLookUpSetHeader.getPodName() + "\';");
				if(cloudLookUpSetHeader.getProjectId()!=null)
					sqlBuilder.append("  p_xxr_cld_lookup_hdrs(" + i + ").project_id\\:=" + cloudLookUpSetHeader.getProjectId() + ";");
				if (!Validations.isNullOrEmptyorWhiteSpace(cloudLookUpSetHeader.getProjectName()))
				sqlBuilder.append(" p_xxr_cld_lookup_hdrs(" + i + ").project_name\\:=" + "\'"
						+ cloudLookUpSetHeader.getProjectName() + "\';");
				
				i++;
			}

			sqlBuilder.append(" xxr_cld_lookup_save_pkg.xxr_cld_lookup_hdrs(p_xxr_cld_lookup_hdrs,p_msg);");
			sqlBuilder.append(" end;");

			Query query = entityManager.createNativeQuery(sqlBuilder.toString());

			int count = query.executeUpdate();

			log.info("count" + count);

			pmsg = "Cloud LookupSet Header saved successfully";
		
			entityManager.clear();
			entityManager.close();
		} catch (Exception e) {
			pmsg = "Exception while saving data";
			throw new Exception(e.getMessage());
		}
		return pmsg;

	}

}
