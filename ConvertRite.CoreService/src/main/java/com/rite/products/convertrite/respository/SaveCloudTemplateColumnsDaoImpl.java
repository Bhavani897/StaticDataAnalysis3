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
import com.rite.products.convertrite.po.SaveCloudTemplateColumnsPo;
import com.rite.products.convertrite.utils.Utils;

@Repository
public class SaveCloudTemplateColumnsDaoImpl {

	@PersistenceContext
	private EntityManager entityManager;

	private static final Logger log = LoggerFactory.getLogger(SaveCloudTemplateColumnsDaoImpl.class);

	@Transactional
	public String saveCloudTemplateColumns(List<SaveCloudTemplateColumnsPo> cloudTemplateColumnsPo,HttpServletRequest request) throws BadRequestException,Exception {
		log.info("Start of saveCloudTemplateColumns Method in DaoImpl### ");

		String pmsg = "";
		try {
			StringBuilder sqlBuilder = new StringBuilder()
					.append("declare" + "     p_xxr_cld_temp_cols xxr_cld_temp_save_pkg.r_xxr_cld_temp_cols;"
							+ "p_user_id VARCHAR2(2000);"
							+ " p_msg VARCHAR2(2000);" + " begin ");
			sqlBuilder.append(
					" p_user_id\\:="+ "\'" + Utils.cleanEmail(request.getHeader("userId")) + "\';");	
			int i = 1;
			for (SaveCloudTemplateColumnsPo cloudColumn : cloudTemplateColumnsPo) {

				if (Validations.isNullOrEmpty(cloudColumn.getColumnName())
						|| cloudColumn.getTemplateId() == null)
					throw new BadRequestException("templateId and columnName are Mandatory fields");

				sqlBuilder.append(" p_xxr_cld_temp_cols(" + i + ").template_id\\:=" + cloudColumn.getTemplateId() + ";");
				if (cloudColumn.getColumnId() != null && cloudColumn.getColumnId() != 0 )
					sqlBuilder
							.append("  p_xxr_cld_temp_cols(" + i + ").column_id\\:=" + cloudColumn.getColumnId() + ";");
				sqlBuilder.append(
						" p_xxr_cld_temp_cols(" + i + ").column_name\\:=" + "\'" + cloudColumn.getColumnName() + "\';");
				if(!Validations.isNullOrEmpty(cloudColumn.getMappingType()))
				sqlBuilder.append(" p_xxr_cld_temp_cols(" + i + ").mapping_type\\:=" + "\'"
						+ cloudColumn.getMappingType() + "\';");
				if (cloudColumn.getDisplaySeq() != null)
					sqlBuilder.append(
							" p_xxr_cld_temp_cols(" + i + ").display_seq\\:=" + cloudColumn.getDisplaySeq() + ";");
				if (cloudColumn.getMappingSetId() != null)
					sqlBuilder.append(
							" p_xxr_cld_temp_cols(" + i + ").mapping_set_id\\:=" + cloudColumn.getMappingSetId() + ";");
				if (cloudColumn.getSourceColumnId() != null)
					sqlBuilder.append(
							" p_xxr_cld_temp_cols(" + i + ").source_column_id\\:=" + cloudColumn.getSourceColumnId() + ";");
				/*
				 * if (!Validations.isNullOrEmptyorWhiteSpace(cloudColumn.getMappingSetName()))
				 * sqlBuilder.append(" p_xxr_cld_temp_cols(" + i + ").mapping_set_name\\:=" +
				 * "\'" + cloudColumn.getMappingSetName() + "\';");
				 */
				if (!Validations.isNullOrEmptyorWhiteSpace(cloudColumn.getMappingValue1()))
					sqlBuilder.append(" p_xxr_cld_temp_cols(" + i + ").mapping_value1\\:=" + "\'"
							+ cloudColumn.getMappingValue1() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(cloudColumn.getMappingValue2()))
					sqlBuilder.append(" p_xxr_cld_temp_cols(" + i + ").mapping_value2\\:=" + "\'"
							+ cloudColumn.getMappingValue2() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(cloudColumn.getMappingValue3()))
					sqlBuilder.append(" p_xxr_cld_temp_cols(" + i + ").mapping_value3\\:=" + "\'"
							+ cloudColumn.getMappingValue3() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(cloudColumn.getMappingValue4()))
					sqlBuilder.append(" p_xxr_cld_temp_cols(" + i + ").mapping_value4\\:=" + "\'"
							+ cloudColumn.getMappingValue4() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(cloudColumn.getMappingValue5()))
					sqlBuilder.append(" p_xxr_cld_temp_cols(" + i + ").mapping_value5\\:=" + "\'"
							+ cloudColumn.getMappingValue5() + "\';");
				/*
				 * if (cloudColumn.getStartDate() != null)
				 * sqlBuilder.append(" p_xxr_cld_temp_cols(" + i + ").start_date\\:=" + "\'" +
				 * new SimpleDateFormat("dd-MMM-yyyy").format(cloudColumn.getStartDate()) +
				 * "\';"); else sqlBuilder.append(" p_xxr_cld_temp_cols(" + i +
				 * ").start_date\\:=null;"); if (cloudColumn.getEndDate() != null)
				 * sqlBuilder.append(" p_xxr_cld_temp_cols(" + i + ").end_date\\:=" + "\'" + new
				 * SimpleDateFormat("dd-MMM-yyyy").format(cloudColumn.getEndDate()) + "\';");
				 * else sqlBuilder.append(" p_xxr_cld_temp_cols(" + i + ").end_date\\:=null;");
				 * if (!Validations.isNullOrEmptyorWhiteSpace(cloudColumn.getEnableFlag()))
				 * sqlBuilder.append(" p_xxr_cld_temp_cols(" + i + ").enabled_flag\\:=" + "\'" +
				 * cloudColumn.getEnableFlag() + "\';");
				 */
				if (!Validations.isNullOrEmptyorWhiteSpace(cloudColumn.getColumnType()))
					sqlBuilder.append(" p_xxr_cld_temp_cols(" + i + ").column_type\\:=" + "\'"
							+ cloudColumn.getColumnType() + "\';");	
				if (!Validations.isNullOrEmptyorWhiteSpace(cloudColumn.getNullAllowedFlag()))
					sqlBuilder.append(" p_xxr_cld_temp_cols(" + i + ").null_allowed_flag\\:=" + "\'"
							+ cloudColumn.getNullAllowedFlag() + "\';");	
				if (!Validations.isNullOrEmptyorWhiteSpace(cloudColumn.getDescription()))
					sqlBuilder.append(" p_xxr_cld_temp_cols(" + i + ").description\\:=" + "\'"
							+ cloudColumn.getDescription() + "\';");	
				if (cloudColumn.getWidth() != null)
					sqlBuilder
							.append("  p_xxr_cld_temp_cols(" + i + ").width\\:=" + cloudColumn.getWidth() + ";");
				if (!Validations.isNullOrEmptyorWhiteSpace(cloudColumn.getAttribute1()))
					sqlBuilder.append(" p_xxr_cld_temp_cols(" + i + ").attribute1\\:=" + "\'"
							+ cloudColumn.getAttribute1() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(cloudColumn.getAttribute2()))
					sqlBuilder.append(" p_xxr_cld_temp_cols(" + i + ").attribute2\\:=" + "\'"
							+ cloudColumn.getAttribute2() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(cloudColumn.getAttribute3()))
					sqlBuilder.append(" p_xxr_cld_temp_cols(" + i + ").attribute3\\:=" + "\'"
							+ cloudColumn.getAttribute3() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(cloudColumn.getAttribute4()))
					sqlBuilder.append(" p_xxr_cld_temp_cols(" + i + ").attribute4\\:=" + "\'"
							+ cloudColumn.getAttribute4() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(cloudColumn.getAttribute5()))
					sqlBuilder.append(" p_xxr_cld_temp_cols(" + i + ").attribute5\\:=" + "\'"
							+ cloudColumn.getAttribute5() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(cloudColumn.getSelected()))
					sqlBuilder.append(
							" p_xxr_cld_temp_cols(" + i + ").selected\\:=" + "\'" + cloudColumn.getSelected() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(cloudColumn.getOrigTransRef()))
					sqlBuilder.append(
							" p_xxr_cld_temp_cols(" + i + ").orig_trans_ref\\:=" + "\'" + cloudColumn.getOrigTransRef() + "\';");
				

				i++;
			}

			sqlBuilder.append(" xxr_cld_temp_save_pkg.xxr_cld_temp_cols_prc(p_xxr_cld_temp_cols,p_user_id,p_msg);");
			sqlBuilder.append(" end;");

			Query query = entityManager.createNativeQuery(sqlBuilder.toString());

			int count = query.executeUpdate();
			
			log.info("count" + count);

			pmsg = "Cloud Template columns saved successfully";
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
		} catch(BadRequestException e) {
			pmsg = "Exception while saving data";
			throw new BadRequestException(e.getMessage());
		}catch (Exception e) {
			pmsg = "Exception while saving data";
			throw new Exception(e.getMessage());
		}
		return pmsg;

	}
}
