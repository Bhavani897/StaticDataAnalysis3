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
import com.rite.products.convertrite.po.SaveFormulaColumnsPo;
import com.rite.products.convertrite.utils.Utils;

@Repository
public class SaveFormulaColumnsDaoImpl {

	@PersistenceContext
	private EntityManager entityManager;

	private static final Logger log = LoggerFactory.getLogger(SaveFormulaColumnsDaoImpl.class);

	@Autowired
	XxrFormulaColumnsRepository xxrFormulaColumnsRepository;

	@Transactional
	public String saveFormulaColumns(List<SaveFormulaColumnsPo> saveFormulaColumnsPo,HttpServletRequest request)
			throws ValidationException, BadRequestException, Exception {
		log.info("Start of saveFormulaColumns Method in DaoImpl### ");
		String pmsg = "";
		try {
			StringBuilder sqlBuilder = new StringBuilder()
					.append("declare" + "     p_xxr_formula_columns xxr_formula_save_pkg.r_xxr_formula_columns; "
							+ "p_user_id VARCHAR2(2000);"
							+ " p_msg VARCHAR2(2000);" + " begin ");
			sqlBuilder.append(" p_user_id\\:=" + "\'" + Utils.cleanEmail(request.getHeader("userId")) + "\';");
			int i = 1;
			for (SaveFormulaColumnsPo formulaColumns : saveFormulaColumnsPo) {

				if (formulaColumns.getFormulaSetId() == null || formulaColumns.getFormulaTableId() == null
						|| formulaColumns.getSourceColumnId() == null)
					throw new BadRequestException(
							"formulaSetId,formulaTableId and sourceColumnId are Mandatory fields");

				if (formulaColumns.getFormulaSetId() != null)
					sqlBuilder.append(" p_xxr_formula_columns(" + i + ").formula_set_id\\:="
							+ formulaColumns.getFormulaSetId() + ";");
				if (formulaColumns.getFormulaTableId() != null)
					sqlBuilder.append(" p_xxr_formula_columns(" + i + ").formula_table_id\\:="
							+ formulaColumns.getFormulaTableId() + ";");
				if (formulaColumns.getFormulaColumnId() != null && formulaColumns.getFormulaColumnId() != 0) {
					sqlBuilder.append(" p_xxr_formula_columns(" + i + ").formula_column_id\\:="
							+ formulaColumns.getFormulaColumnId() + ";");
				} else {
					Long formulaColumnId = xxrFormulaColumnsRepository.getFormulaColumnId(
							formulaColumns.getFormulaSetId(), formulaColumns.getFormulaTableId(),
							formulaColumns.getSourceColumnId());
					if (formulaColumnId != null)
						throw new ValidationException("sourcecolumn already exists ");
					Long formColumnId = xxrFormulaColumnsRepository.getFormulaColumnIdByseq(
							formulaColumns.getFormulaSetId(), formulaColumns.getFormulaTableId(),
							formulaColumns.getSeq());
					if (formColumnId != null)
						throw new ValidationException("seq already exists ");

					sqlBuilder.append(" p_xxr_formula_columns(" + i + ").formula_column_id\\:=null;");
				}
				if (formulaColumns.getSeq() != null)
					sqlBuilder.append(" p_xxr_formula_columns(" + i + ").seq\\:=" + formulaColumns.getSeq() + ";");
				if (formulaColumns.getSourceColumnId() != null)
					sqlBuilder.append(" p_xxr_formula_columns(" + i + ").source_column_id\\:="
							+ formulaColumns.getSourceColumnId() + ";");
				if (!Validations.isNullOrEmptyorWhiteSpace(formulaColumns.getDescription()))
					sqlBuilder.append(" p_xxr_formula_columns(" + i + ").description\\:=" + "\'"
							+ formulaColumns.getDescription() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(formulaColumns.getAttribute1()))
					sqlBuilder.append(" p_xxr_formula_columns(" + i + ").attribute1\\:=" + "\'"
							+ formulaColumns.getAttribute1() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(formulaColumns.getAttribute2()))
					sqlBuilder.append(" p_xxr_formula_columns(" + i + ").attribute2\\:=" + "\'"
							+ formulaColumns.getAttribute2() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(formulaColumns.getAttribute3()))
					sqlBuilder.append(" p_xxr_formula_columns(" + i + ").attribute3\\:=" + "\'"
							+ formulaColumns.getAttribute3() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(formulaColumns.getAttribute4()))
					sqlBuilder.append(" p_xxr_formula_columns(" + i + ").attribute4\\:=" + "\'"
							+ formulaColumns.getAttribute4() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(formulaColumns.getAttribute5()))
					sqlBuilder.append(" p_xxr_formula_columns(" + i + ").attribute5\\:=" + "\'"
							+ formulaColumns.getAttribute5() + "\';");

				i++;
			}

			sqlBuilder.append("xxr_formula_save_pkg.xxr_formula_columns_prc(p_xxr_formula_columns,p_user_id,p_msg);");
			sqlBuilder.append(" end;");

			Query query = entityManager.createNativeQuery(sqlBuilder.toString());

			int count = query.executeUpdate();

			log.info("count" + count);

			pmsg = "Formula Columns saved successfully";

			entityManager.clear();
			entityManager.close();
		} catch (ValidationException e) {
			pmsg = "Exception while saving data";
			throw new ValidationException(e.getMessage());
		} catch (BadRequestException e) {
			pmsg = "Exception while saving data";
			throw new BadRequestException(e.getMessage());
		} catch (Exception e) {
			pmsg = "Exception while saving data";
			throw new Exception(e.getMessage());
		}
		return pmsg;

	}

}
