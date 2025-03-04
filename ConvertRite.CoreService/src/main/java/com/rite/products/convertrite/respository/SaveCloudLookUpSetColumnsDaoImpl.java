package com.rite.products.convertrite.respository;

import java.text.SimpleDateFormat;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.persistence.Query;
import javax.transaction.Transactional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import com.rite.products.convertrite.Validations.Validations;
import com.rite.products.convertrite.po.SaveCloudLookUpSetColumnsPo;

@Repository
public class SaveCloudLookUpSetColumnsDaoImpl {
	@PersistenceContext
	private EntityManager entityManager;

	private static final Logger log = LoggerFactory.getLogger(SaveCloudLookUpSetColumnsDaoImpl.class);

	@Transactional
	public String saveCloudLookUpSetValues(List<SaveCloudLookUpSetColumnsPo> cloudLookUpSetColumnsPo) throws Exception {
		log.info("Start of saveCloudLookUpSetValues Method in DaoImpl### ");

		String pmsg = "";
		try {
			StringBuilder sqlBuilder = new StringBuilder()
					.append("declare" + "    p_xxr_cld_lookup_cols xxr_cld_lookup_save_pkg.r_xxr_cols_tabtype;"
							+ " p_msg VARCHAR2(2000);" + " begin ");
			int i = 1;
			for (SaveCloudLookUpSetColumnsPo cloudLookUpSetColumn : cloudLookUpSetColumnsPo) {

				if (Validations.isNullOrEmpty(cloudLookUpSetColumn.getLookUpValue())
						|| cloudLookUpSetColumn.getLookUpSetId() == null)
					throw new Exception("lookUpSetId and lookUpValue are Mandatory fields");

				sqlBuilder.append(" p_xxr_cld_lookup_cols(" + i + ").lookup_set_id\\:="
						+ cloudLookUpSetColumn.getLookUpSetId() + ";");
				if (!Validations.isNullOrEmptyorWhiteSpace(cloudLookUpSetColumn.getLookUpValue()))
					sqlBuilder.append("  p_xxr_cld_lookup_cols(" + i + ").lookup_value\\:=" + "\'"
							+ cloudLookUpSetColumn.getLookUpValue() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(cloudLookUpSetColumn.getEnabledFlag()))
					sqlBuilder.append("  p_xxr_cld_lookup_cols(" + i + ").enabled\\:=" + "\'"
							+ cloudLookUpSetColumn.getEnabledFlag() + "\';");
				if (cloudLookUpSetColumn.getStartDate() != null)
					sqlBuilder.append("  p_xxr_cld_lookup_cols(" + i + ").start_date\\:=" + "\'"
							+ new SimpleDateFormat("dd-MMM-yyyy").format(cloudLookUpSetColumn.getStartDate()) + "\';");
				else
					sqlBuilder.append("  p_xxr_cld_lookup_cols(" + i + ").start_date\\:=null;");
				if (cloudLookUpSetColumn.getEndDate() != null)
					sqlBuilder.append(" p_xxr_cld_lookup_cols(" + i + ").end_date\\:=" + "\'"
							+ new SimpleDateFormat("dd-MMM-yyyy").format(cloudLookUpSetColumn.getEndDate()) + "\';");
				else
					sqlBuilder.append("  p_xxr_cld_lookup_cols(" + i + ").end_date\\:=null;");
				if (cloudLookUpSetColumn.getLookUpValueId() != null && cloudLookUpSetColumn.getLookUpValueId() != 0)
					sqlBuilder.append(" p_xxr_cld_lookup_cols(" + i + ").lookup_value_id\\:="
							+ cloudLookUpSetColumn.getLookUpValueId() + ";");
				if (!Validations.isNullOrEmptyorWhiteSpace(cloudLookUpSetColumn.getParentObjectValue()))
					sqlBuilder.append("  p_xxr_cld_lookup_cols(" + i + ").parent_object_value\\:=" + "\'"
							+ cloudLookUpSetColumn.getParentObjectValue() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(cloudLookUpSetColumn.getAttribute1()))
					sqlBuilder.append("  p_xxr_cld_lookup_cols(" + i + ").attribute1\\:=" + "\'"
							+ cloudLookUpSetColumn.getAttribute1() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(cloudLookUpSetColumn.getAttribute2()))
					sqlBuilder.append("  p_xxr_cld_lookup_cols(" + i + ").attribute2\\:=" + "\'"
							+ cloudLookUpSetColumn.getAttribute2() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(cloudLookUpSetColumn.getAttribute3()))
					sqlBuilder.append("  p_xxr_cld_lookup_cols(" + i + ").attribute3\\:=" + "\'"
							+ cloudLookUpSetColumn.getAttribute3() + "\';");

				i++;
			}

			sqlBuilder.append(" xxr_cld_lookup_save_pkg.xxr_cld_lookup_cols(p_xxr_cld_lookup_cols,p_msg);");
			sqlBuilder.append(" end;");

			Query query = entityManager.createNativeQuery(sqlBuilder.toString());

			int count = query.executeUpdate();

			log.info("count" + count);

			pmsg = "Cloud Lookup set values  saved successfully";
			entityManager.clear();
			entityManager.close();
		} catch (Exception e) {
			pmsg = "Exception while saving data";
			throw new Exception(e.getMessage());
		}
		return pmsg;

	}

}
