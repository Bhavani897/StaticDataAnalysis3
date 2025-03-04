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
import com.rite.products.convertrite.exception.BadRequestException;
import com.rite.products.convertrite.exception.ValidationException;
import com.rite.products.convertrite.po.SaveFormulaSetHeaderPo;
import com.rite.products.convertrite.utils.Utils;

@Repository
public class SaveFormulaSetHeaderDaoImpl {

	@PersistenceContext
	private EntityManager entityManager;
	@Autowired
	XxrFormulaSetsRepository xxrFormulaSetsRepository;
	
	private static final Logger log = LoggerFactory.getLogger(SaveFormulaSetHeaderDaoImpl.class);

	@Transactional
	public String saveFormulaSetHeaders(List<SaveFormulaSetHeaderPo> saveFormulaSetHeaderPo,HttpServletRequest request) throws BadRequestException,ValidationException,Exception {
		log.info("Start of saveFormulaSetHeaders Method in DaoImpl### ");
		String pmsg = "";
		try {
			StringBuilder sqlBuilder = new StringBuilder()
					.append("declare" + "     p_xxr_formula_sets xxr_formula_save_pkg.r_xxr_formula_sets; "
							+ "p_user_id VARCHAR2(2000);" 
							+ " p_msg VARCHAR2(2000);" + " begin ");
			sqlBuilder.append(" p_user_id\\:=" + "\'" + Utils.cleanEmail(request.getHeader("userId")) + "\';");
			int i = 1;
			for (SaveFormulaSetHeaderPo formulaSetHeader : saveFormulaSetHeaderPo) {

				if (Validations.isNullOrEmpty(formulaSetHeader.getFormulaSetName())
						|| Validations.isNullOrEmpty(formulaSetHeader.getCloudColumn())
						|| formulaSetHeader.getObjectId() == null || formulaSetHeader.getParentObjectId() == null
						|| formulaSetHeader.getProjectId() == null || formulaSetHeader.getPodId() == null)
					throw new BadRequestException("formulaSetName,objectId,parentObjectId,projectId and podId are Mandatory fields");
				
				

				if (formulaSetHeader.getFormulaSetId() != null && formulaSetHeader.getFormulaSetId() != 0)
					sqlBuilder.append(" p_xxr_formula_sets(" + i + ").formula_set_id\\:="
							+ formulaSetHeader.getFormulaSetId() + ";");
				else {
					Long formulaSetId=xxrFormulaSetsRepository.getFormulaSetId(formulaSetHeader.getFormulaSetName());
					if(formulaSetId!=null)
						throw new ValidationException("This FormulaSetName already exists");
					
					sqlBuilder.append(" p_xxr_formula_sets(" + i + ").formula_set_id\\:= null;");
				}
				if (formulaSetHeader.getPodId() != null)
					sqlBuilder.append(" p_xxr_formula_sets(" + i + ").pod_id\\:=" + 
							+ formulaSetHeader.getPodId() + ";");
				if (formulaSetHeader.getProjectId() != null)
					sqlBuilder.append(" p_xxr_formula_sets(" + i + ").project_id\\:=" 
							+ formulaSetHeader.getProjectId() + ";");
				if (formulaSetHeader.getObjectId() != null)
					sqlBuilder.append(" p_xxr_formula_sets(" + i + ").object_id\\:=" 
							+ formulaSetHeader.getObjectId() + ";");
				if (formulaSetHeader.getParentObjectId() != null)
					sqlBuilder.append(" p_xxr_formula_sets(" + i + ").parent_object_id\\:="
							+ formulaSetHeader.getParentObjectId() + ";");
				if (!Validations.isNullOrEmptyorWhiteSpace(formulaSetHeader.getFormulaSetName()))
					sqlBuilder.append(" p_xxr_formula_sets(" + i + ").formula_set_name\\:=" + "\'"
							+ formulaSetHeader.getFormulaSetName() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(formulaSetHeader.getDescription()))
					sqlBuilder.append(" p_xxr_formula_sets(" + i + ").description\\:=" + "\'"
							+ formulaSetHeader.getDescription() + "\';");
				/*
				 * if (!Validations.isNullOrEmptyorWhiteSpace(formulaSetHeader.getBu()))
				 * sqlBuilder.append(" p_xxr_formula_sets(" + i + ").bu\\:=" + "\'" +
				 * formulaSetHeader.getBu() + "\';");
				 */
				if (!Validations.isNullOrEmptyorWhiteSpace(formulaSetHeader.getFormulaType()))
					sqlBuilder.append(" p_xxr_formula_sets(" + i + ").formula_type\\:=" + "\'"
							+ formulaSetHeader.getFormulaType() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(formulaSetHeader.getCloudColumn()))
					sqlBuilder.append(" p_xxr_formula_sets(" + i + ").cloud_column\\:=" + "\'"
							+ formulaSetHeader.getCloudColumn() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(formulaSetHeader.getFormulaValue()))
					sqlBuilder.append(" p_xxr_formula_sets(" + i + ").formula_value\\:=" + "\'"
							+ formulaSetHeader.getFormulaValue() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(formulaSetHeader.getSqlQuery()))
					sqlBuilder.append(" p_xxr_formula_sets(" + i + ").sqlquery\\:=q'[" 
							+ formulaSetHeader.getSqlQuery() + "]';");
				if (!Validations.isNullOrEmptyorWhiteSpace(formulaSetHeader.getAttribute1()))
					sqlBuilder.append(" p_xxr_formula_sets(" + i + ").attribute1\\:=" + "\'"
							+ formulaSetHeader.getAttribute1() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(formulaSetHeader.getAttribute2()))
					sqlBuilder.append(" p_xxr_formula_sets(" + i + ").attribute2\\:=" + "\'"
							+ formulaSetHeader.getAttribute2() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(formulaSetHeader.getAttribute3()))
					sqlBuilder.append(" p_xxr_formula_sets(" + i + ").attribute3\\:=" + "\'"
							+ formulaSetHeader.getAttribute3() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(formulaSetHeader.getAttribute4()))
					sqlBuilder.append(" p_xxr_formula_sets(" + i + ").attribute4\\:=" + "\'"
							+ formulaSetHeader.getAttribute4() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(formulaSetHeader.getAttribute5()))
					sqlBuilder.append(" p_xxr_formula_sets(" + i + ").attribute5\\:=" + "\'"
							+ formulaSetHeader.getAttribute5() + "\';");

				i++;
			}

			sqlBuilder.append("xxr_formula_save_pkg.xxr_formula_sets_prc(p_xxr_formula_sets,p_user_id,p_msg);");
			sqlBuilder.append(" end;");

			Query query = entityManager.createNativeQuery(sqlBuilder.toString());
			
		
			int count = query.executeUpdate();

			log.info("count" + count);

			pmsg = "FormulaSet Header saved successfully";

			entityManager.clear();
			entityManager.close();
		} catch (BadRequestException e) {
			pmsg = "Exception while saving data";
			throw new BadRequestException(e.getMessage());
		}catch (ValidationException e) {
			pmsg = "Exception while saving data";
			throw new ValidationException(e.getMessage());
		}catch (Exception e) {
			pmsg = "Exception while saving data";
			throw new Exception(e.getMessage());
		}
		return pmsg;

	}

}
