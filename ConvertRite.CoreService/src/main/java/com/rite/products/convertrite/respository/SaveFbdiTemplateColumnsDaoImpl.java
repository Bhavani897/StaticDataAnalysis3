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
import com.rite.products.convertrite.po.SaveFbdiTemplateColumnsPo;
import com.rite.products.convertrite.utils.Utils;

@Repository
public class SaveFbdiTemplateColumnsDaoImpl {
	
	@PersistenceContext
	private EntityManager entityManager;

	private static final Logger log = LoggerFactory.getLogger(SaveFbdiTemplateColumnsDaoImpl.class);

	@Transactional
	public String saveFbdiTemplateColumns(List<SaveFbdiTemplateColumnsPo> fbdiTemplateColumnsPo,HttpServletRequest request) throws BadRequestException,Exception {
		log.info("Start of saveFbdiTemplateColumns Method in DaoImpl### ");

		String pmsg = "";
		try {
			StringBuilder sqlBuilder = new StringBuilder()
					.append("declare" + "     p_xxr_fbdi_temp_cols xxr_fbdi_temp_save_pkg.r_xxr_fbdi_temp_cols;"
							+ "p_user_id VARCHAR2(2000);"
							+ " p_msg VARCHAR2(2000);" + " begin ");
			sqlBuilder.append(
					" p_user_id\\:="+ "\'" + Utils.cleanEmail(request.getHeader("userId")) + "\';");	
			int i = 1;
			for (SaveFbdiTemplateColumnsPo fbdiColumn : fbdiTemplateColumnsPo) {

				if ( fbdiColumn.getFbdiTemplateId() == null || Validations.isNullOrEmpty(fbdiColumn.getDatabaseColumn()))
					throw new BadRequestException("templateId and columnName are Mandatory fields");

				sqlBuilder.append(" p_xxr_fbdi_temp_cols(" + i + ").fbdi_template_id\\:=" + fbdiColumn.getFbdiTemplateId() + ";");
				if (fbdiColumn.getFbdiColumnId() != null && fbdiColumn.getFbdiColumnId() != 0 )
					sqlBuilder
							.append("  p_xxr_fbdi_temp_cols(" + i + ").fbdi_column_id\\:=" + fbdiColumn.getFbdiColumnId() + ";");
				if(!Validations.isNullOrEmpty(fbdiColumn.getDatabaseColumn()))
				sqlBuilder.append(
						" p_xxr_fbdi_temp_cols(" + i + ").fbdi_column_name\\:=" + "\'" + fbdiColumn.getFbdiColumnName() + "\';");
				if (fbdiColumn.getSequence() != null )
					sqlBuilder
							.append("  p_xxr_fbdi_temp_cols(" + i + ").sequence\\:=" + fbdiColumn.getSequence() + ";");
				if(!Validations.isNullOrEmpty(fbdiColumn.getRequired()))
				sqlBuilder.append(" p_xxr_fbdi_temp_cols(" + i + ").required\\:=" + "\'"
						+ fbdiColumn.getRequired() + "\';");
				if(!Validations.isNullOrEmpty(fbdiColumn.getActiveFlag()))
					sqlBuilder.append(" p_xxr_fbdi_temp_cols(" + i + ").active_flag\\:=" + "\'"
							+ fbdiColumn.getActiveFlag() + "\';");
				if(!Validations.isNullOrEmpty(fbdiColumn.getDatabaseTable()))
					sqlBuilder.append(" p_xxr_fbdi_temp_cols(" + i + ").database_table\\:=" + "\'"
							+ fbdiColumn.getDatabaseTable() + "\';");
				if(!Validations.isNullOrEmpty(fbdiColumn.getDatabaseColumn()))
					sqlBuilder.append(" p_xxr_fbdi_temp_cols(" + i + ").database_column\\:=" + "\'"
							+ fbdiColumn.getDatabaseColumn() + "\';");
				if (fbdiColumn.getStartDate() != null)
					sqlBuilder.append(" p_xxr_fbdi_temp_cols(" + i + ").start_date\\:=" + "\'"
						+ new SimpleDateFormat("dd-MMM-yyyy").format(fbdiColumn.getStartDate()) + "\';");
				else
					sqlBuilder.append(" p_xxr_fbdi_temp_cols(" + i + ").start_date\\:=null;");
				if (fbdiColumn.getEndDate() != null)
					sqlBuilder.append(" p_xxr_fbdi_temp_cols(" + i + ").end_date\\:=" + "\'"
							+ new SimpleDateFormat("dd-MMM-yyyy").format(fbdiColumn.getEndDate()) + "\';");
				else
					sqlBuilder.append(" p_xxr_fbdi_temp_cols(" + i + ").end_date\\:=null;");
				if (!Validations.isNullOrEmptyorWhiteSpace(fbdiColumn.getAttribute1()))
					sqlBuilder.append(" p_xxr_fbdi_temp_cols(" + i + ").attribute1\\:=" + "\'"
							+ fbdiColumn.getAttribute1() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(fbdiColumn.getAttribute2()))
					sqlBuilder.append(" p_xxr_fbdi_temp_cols(" + i + ").attribute2\\:=" + "\'"
							+ fbdiColumn.getAttribute2() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(fbdiColumn.getAttribute3()))
					sqlBuilder.append(" p_xxr_fbdi_temp_cols(" + i + ").attribute3\\:=" + "\'"
							+ fbdiColumn.getAttribute3() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(fbdiColumn.getAttribute4()))
					sqlBuilder.append(" p_xxr_fbdi_temp_cols(" + i + ").attribute4\\:=" + "\'"
							+ fbdiColumn.getAttribute4() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(fbdiColumn.getAttribute5()))
					sqlBuilder.append(" p_xxr_fbdi_temp_cols(" + i + ").attribute5\\:=" + "\'"
							+ fbdiColumn.getAttribute5() + "\';");
				
				i++;
			}

			sqlBuilder.append(" xxr_fbdi_temp_save_pkg.xxr_fbdi_temp_cols_prc(p_xxr_fbdi_temp_cols,p_user_id,p_msg);");
			sqlBuilder.append(" end;");

			Query query = entityManager.createNativeQuery(sqlBuilder.toString());

			int count = query.executeUpdate();
			
			log.info("count" + count);

			pmsg = "Fbdi Template columns saved successfully";
			
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
