package com.rite.products.convertrite.respository;

import java.text.SimpleDateFormat;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.ParameterMode;
import javax.persistence.PersistenceContext;
import javax.persistence.Query;
import javax.persistence.StoredProcedureQuery;
import javax.servlet.http.HttpServletRequest;
import javax.transaction.Transactional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import com.rite.products.convertrite.Validations.Validations;
import com.rite.products.convertrite.exception.BadRequestException;
import com.rite.products.convertrite.po.SaveSourceTemplatesColumnsPo;
import com.rite.products.convertrite.po.SrcTemplateColsUpdateReq;
import com.rite.products.convertrite.po.SrcTemplateColsUpdtRes;
import com.rite.products.convertrite.utils.Utils;

@Repository
public class SaveSourceTemplateColumnsDaoImpl {

	@PersistenceContext
	private EntityManager entityManager;

	private static final Logger log = LoggerFactory.getLogger(SaveSourceTemplateColumnsDaoImpl.class);

	@Transactional
	public String saveSourceTemplateColumns(List<SaveSourceTemplatesColumnsPo> sourceTemplateColumnsPo,
			HttpServletRequest request) throws BadRequestException, Exception {
		log.info("Start of saveSourceTemplateColumns Method in DaoImpl### ");

		String pmsg = "";
		try {
			StringBuilder sqlBuilder = new StringBuilder()
					.append("declare" + "     p_xxr_src_temp_cols  xxr_src_temp_save_pkg.r_xxr_src_temp_cols;"
							+ "p_user_id VARCHAR2(2000);" + " p_msg VARCHAR2(2000);" + " begin ");
			sqlBuilder.append(" p_user_id\\:=" + "\'" + Utils.cleanEmail(request.getHeader("userId")) + "\';");
			int i = 1;
			for (SaveSourceTemplatesColumnsPo sourceColumn : sourceTemplateColumnsPo) {

				if (Validations.isNullOrEmpty(sourceColumn.getColumnName()) || sourceColumn.getTemplateId() == null)
					throw new BadRequestException("templateId and columnName  are Mandatory fields");

				sqlBuilder
						.append(" p_xxr_src_temp_cols(" + i + ").template_id\\:=" + sourceColumn.getTemplateId() + ";");
				if (sourceColumn.getColumnId() != null && sourceColumn.getColumnId() != 0)
					sqlBuilder.append(
							"  p_xxr_src_temp_cols(" + i + ").column_id\\:=" + sourceColumn.getColumnId() + ";");
				sqlBuilder.append(" p_xxr_src_temp_cols(" + i + ").column_name\\:=" + "\'"
						+ sourceColumn.getColumnName() + "\';");
				/*
				 * if (!Validations.isNullOrEmpty(sourceColumn.getMappingType()))
				 * sqlBuilder.append(" p_xxr_src_temp_cols(" + i + ").mapping_type\\:=" + "\'" +
				 * sourceColumn.getMappingType() + "\';"); if (sourceColumn.getDisplaySeq() !=
				 * null) sqlBuilder.append( " p_xxr_src_temp_cols(" + i + ").display_seq\\:=" +
				 * sourceColumn.getDisplaySeq() + ";"); if (sourceColumn.getMappingSetId() !=
				 * null) sqlBuilder.append(" p_xxr_src_temp_cols(" + i + ").mapping_set_id\\:="
				 * + sourceColumn.getMappingSetId() + ";");
				 */
				/*
				 * if (!Validations.isNullOrEmptyorWhiteSpace(sourceColumn.getMappingSetName()))
				 * sqlBuilder.append(" p_xxr_src_temp_cols(" + i + ").mapping_set_name\\:=" +
				 * "\'" + sourceColumn.getMappingSetName() + "\';");
				 */
				/*
				 * if (!Validations.isNullOrEmptyorWhiteSpace(sourceColumn.getMappingValue()))
				 * sqlBuilder.append(" p_xxr_src_temp_cols(" + i + ").mapping_value\\:=" + "\'"
				 * + sourceColumn.getMappingValue() + "\';"); if (sourceColumn.getStartDate() !=
				 * null) sqlBuilder.append(" p_xxr_src_temp_cols(" + i + ").start_date\\:=" +
				 * "\'" + new
				 * SimpleDateFormat("dd-MMM-yyyy").format(sourceColumn.getStartDate()) + "\';");
				 * else sqlBuilder.append(" p_xxr_src_temp_cols(" + i +
				 * ").start_date\\:=null;"); if (sourceColumn.getEndDate() != null)
				 * sqlBuilder.append(" p_xxr_src_temp_cols(" + i + ").end_date\\:=" + "\'" + new
				 * SimpleDateFormat("dd-MMM-yyyy").format(sourceColumn.getEndDate()) + "\';");
				 * else sqlBuilder.append(" p_xxr_src_temp_cols(" + i + ").end_date\\:=null;");
				 * if (!Validations.isNullOrEmptyorWhiteSpace(sourceColumn.getEnableFlag()))
				 * sqlBuilder.append(" p_xxr_src_temp_cols(" + i + ").enabled_flag\\:=" + "\'" +
				 * sourceColumn.getEnableFlag() + "\';");
				 */
				if (!Validations.isNullOrEmptyorWhiteSpace(sourceColumn.getAttribute1()))
					sqlBuilder.append(" p_xxr_src_temp_cols(" + i + ").attribute1\\:=" + "\'"
							+ sourceColumn.getAttribute1() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(sourceColumn.getAttribute2()))
					sqlBuilder.append(" p_xxr_src_temp_cols(" + i + ").attribute2\\:=" + "\'"
							+ sourceColumn.getAttribute2() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(sourceColumn.getAttribute3()))
					sqlBuilder.append(" p_xxr_src_temp_cols(" + i + ").attribute3\\:=" + "\'"
							+ sourceColumn.getAttribute3() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(sourceColumn.getAttribute4()))
					sqlBuilder.append(" p_xxr_src_temp_cols(" + i + ").attribute4\\:=" + "\'"
							+ sourceColumn.getAttribute4() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(sourceColumn.getAttribute5()))
					sqlBuilder.append(" p_xxr_src_temp_cols(" + i + ").attribute5\\:=" + "\'"
							+ sourceColumn.getAttribute5() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(sourceColumn.getSelected()))
					sqlBuilder.append(
							" p_xxr_src_temp_cols(" + i + ").selected\\:=" + "\'" + sourceColumn.getSelected() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(sourceColumn.getOrigTransRef()))
					sqlBuilder.append(" p_xxr_src_temp_cols(" + i + ").orig_trans_ref\\:=" + "\'"
							+ sourceColumn.getOrigTransRef() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(sourceColumn.getColumnType()))
					sqlBuilder.append(" p_xxr_src_temp_cols(" + i + ").column_type\\:=" + "\'"
							+ sourceColumn.getColumnType() + "\';");
				/*
				 * if
				 * (!Validations.isNullOrEmptyorWhiteSpace(sourceColumn.getNullAllowedFlag()))
				 * sqlBuilder.append(" p_xxr_src_temp_cols(" + i + ").null_allowed_flag\\:=" +
				 * "\'" + sourceColumn.getNullAllowedFlag() + "\';");
				 */
				if (sourceColumn.getWidth() != null)
					sqlBuilder.append(" p_xxr_src_temp_cols(" + i + ").width\\:=" + sourceColumn.getWidth() + ";");

				i++;
			}

			sqlBuilder.append(" xxr_src_temp_save_pkg.xxr_src_temp_cols_prc(p_xxr_src_temp_cols,p_user_id,p_msg);");
			sqlBuilder.append(" end;");

			Query query = entityManager.createNativeQuery(sqlBuilder.toString());

			int count = query.executeUpdate();

			log.info("count" + count);

			pmsg = "Source Template columns saved successfully";

			entityManager.clear();
			entityManager.close();
		} catch (BadRequestException e) {
			pmsg = "Exception while saving data";
			throw new BadRequestException(e.getMessage());
		} catch (Exception e) {
			pmsg = "Exception while saving data";
			throw new Exception(e.getMessage());
		}
		return pmsg;

	}

	public SrcTemplateColsUpdtRes srcTemplateColumnsUpdate(SrcTemplateColsUpdateReq srcTemplateColsUpdateReq,String userId) {
		log.info("Start of srcTemplateColumnsUpdate Method in Dao ##");
		SrcTemplateColsUpdtRes srcTemplateColsUpdtRes = new SrcTemplateColsUpdtRes();
		StoredProcedureQuery srcTemplateColumnsUpdtePrc = entityManager
				.createStoredProcedureQuery("CR_SRC_COLS_MODIFY_PROC")
				.registerStoredProcedureParameter("P_TEMPLATE_ID", Long.class, ParameterMode.IN)
				.registerStoredProcedureParameter("P_COLUMN_NAME", String.class, ParameterMode.IN)
				.registerStoredProcedureParameter("P_COLUMN_TYPE", String.class, ParameterMode.IN)
				.registerStoredProcedureParameter("P_OPERATION_TYPE", String.class, ParameterMode.IN)
				.registerStoredProcedureParameter("P_DISPLAY_SEQ", Long.class, ParameterMode.IN)
				.registerStoredProcedureParameter("p_user_id", String.class, ParameterMode.IN)

				.registerStoredProcedureParameter("P_RET_MSG", String.class, ParameterMode.OUT)
				.registerStoredProcedureParameter("P_RET_CODE", String.class, ParameterMode.OUT)
				.setParameter("P_TEMPLATE_ID", srcTemplateColsUpdateReq.getTemplateId())
				.setParameter("P_COLUMN_NAME", srcTemplateColsUpdateReq.getColumnName())
				.setParameter("P_COLUMN_TYPE", srcTemplateColsUpdateReq.getColumnType())
				.setParameter("P_OPERATION_TYPE", srcTemplateColsUpdateReq.getOperationType())
				.setParameter("P_DISPLAY_SEQ", srcTemplateColsUpdateReq.getDisplaySeq())
				.setParameter("p_user_id", userId);

		srcTemplateColumnsUpdtePrc.execute();

		srcTemplateColsUpdtRes.setResCode((String) srcTemplateColumnsUpdtePrc.getOutputParameterValue("P_RET_CODE"));
		srcTemplateColsUpdtRes.setMessage((String) srcTemplateColumnsUpdtePrc.getOutputParameterValue("P_RET_MSG"));

//		String pResult = (String) srcTemplateColumnsUpdtePrc.getOutputParameterValue("P_RET_CODE");
//		if ("Y".equalsIgnoreCase(pResult))
//			srcTemplateColsUpdtRes.setMessage((String) srcTemplateColumnsUpdtePrc.getOutputParameterValue("P_MSG"));
//		else
//			srcTemplateColsUpdtRes.setError((String) srcTemplateColumnsUpdtePrc.getOutputParameterValue("P_MSG"));
		entityManager.clear();
		entityManager.close();

		return srcTemplateColsUpdtRes;
	}

}
