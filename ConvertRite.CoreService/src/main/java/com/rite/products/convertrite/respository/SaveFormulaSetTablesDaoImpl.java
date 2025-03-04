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
import com.rite.products.convertrite.po.SaveFormulaSetTablesPo;
import com.rite.products.convertrite.utils.Utils;

@Repository
public class SaveFormulaSetTablesDaoImpl {
	@PersistenceContext
	private EntityManager entityManager;

	@Autowired
	XxrFormulaTablesRepository xxrFormulaTablesRepository;

	private static final Logger log = LoggerFactory.getLogger(SaveFormulaSetTablesDaoImpl.class);

	@Transactional
	public String saveFormulaSetTables(List<SaveFormulaSetTablesPo> saveFormulaSetTablesPo, HttpServletRequest request)
			throws BadRequestException, ValidationException, Exception {
		log.info("Start of saveFormulaSetTables Method in DaoImpl### ");
		String pmsg = "";
		try {
			StringBuilder sqlBuilder = new StringBuilder()
					.append("declare" + "     p_xxr_formula_tables xxr_formula_save_pkg.r_xxr_formula_tables; "
							+ "p_user_id VARCHAR2(2000);" + " p_msg VARCHAR2(2000);" + " begin ");
			sqlBuilder.append(" p_user_id\\:=" + "\'" + Utils.cleanEmail(request.getHeader("userId")) + "\';");
			int i = 1;
			for (SaveFormulaSetTablesPo formulaSetTables : saveFormulaSetTablesPo) {

				if (formulaSetTables.getSourceTableId() == null)
					throw new BadRequestException("sourceTableId is Mandatory field");

				if (formulaSetTables.getFormulaSetId() != null)
					sqlBuilder.append(" p_xxr_formula_tables(" + i + ").formula_set_id\\:="
							+ formulaSetTables.getFormulaSetId() + ";");
				if (formulaSetTables.getFormulaTableId() != null && formulaSetTables.getFormulaTableId() != 0) {
					sqlBuilder.append(" p_xxr_formula_tables(" + i + ").formula_table_id\\:="
							+ formulaSetTables.getFormulaTableId() + ";");
				} else {
					Long formulaTableId = xxrFormulaTablesRepository
							.getFormulaTableId(formulaSetTables.getFormulaSetId(), formulaSetTables.getSourceTableId());
					if (formulaTableId != null)
						throw new ValidationException("This sourceTableName already exists ");
					Long formTableId = xxrFormulaTablesRepository
							.getFormulaTableIdBySeq(formulaSetTables.getFormulaSetId(), formulaSetTables.getSeq());
					if (formTableId != null)
						throw new ValidationException("This seq already exists ");

					sqlBuilder.append(" p_xxr_formula_tables(" + i + ").formula_table_id\\:=null;");
				}
				if (formulaSetTables.getSeq() != null)
					sqlBuilder.append(" p_xxr_formula_tables(" + i + ").seq\\:=" + formulaSetTables.getSeq() + ";");
				if (formulaSetTables.getSourceTableId() != null)
					sqlBuilder.append(" p_xxr_formula_tables(" + i + ").source_table_id\\:="
							+ formulaSetTables.getSourceTableId() + ";");
				if (!Validations.isNullOrEmptyorWhiteSpace(formulaSetTables.getDescription()))
					sqlBuilder.append(" p_xxr_formula_tables(" + i + ").description\\:=" + "\'"
							+ formulaSetTables.getDescription() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(formulaSetTables.getAttribute1()))
					sqlBuilder.append(" p_xxr_formula_tables(" + i + ").attribute1\\:=" + "\'"
							+ formulaSetTables.getAttribute1() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(formulaSetTables.getAttribute2()))
					sqlBuilder.append(" p_xxr_formula_tables(" + i + ").attribute2\\:=" + "\'"
							+ formulaSetTables.getAttribute2() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(formulaSetTables.getAttribute3()))
					sqlBuilder.append(" p_xxr_formula_tables(" + i + ").attribute3\\:=" + "\'"
							+ formulaSetTables.getAttribute3() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(formulaSetTables.getAttribute4()))
					sqlBuilder.append(" p_xxr_formula_tables(" + i + ").attribute4\\:=" + "\'"
							+ formulaSetTables.getAttribute4() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(formulaSetTables.getAttribute5()))
					sqlBuilder.append(" p_xxr_formula_tables(" + i + ").attribute5\\:=" + "\'"
							+ formulaSetTables.getAttribute5() + "\';");

				i++;
			}

			sqlBuilder.append("xxr_formula_save_pkg.xxr_formula_tables_prc(p_xxr_formula_tables,p_user_id,p_msg);");
			sqlBuilder.append(" end;");

			Query query = entityManager.createNativeQuery(sqlBuilder.toString());

			int count = query.executeUpdate();

			log.info("count" + count);

			pmsg = "FormulaSet Tables saved successfully";

			entityManager.clear();
			entityManager.close();
		} catch (BadRequestException e) {
			pmsg = "Exception while saving data";
			throw new BadRequestException(e.getMessage());
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
