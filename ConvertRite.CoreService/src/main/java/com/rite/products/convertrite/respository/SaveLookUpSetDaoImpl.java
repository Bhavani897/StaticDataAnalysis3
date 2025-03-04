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
import com.rite.products.convertrite.po.SaveLookUpSetPo;
import com.rite.products.convertrite.utils.Utils;

@Repository
public class SaveLookUpSetDaoImpl {

	@PersistenceContext
	private EntityManager entityManager;
	@Autowired
	XxrLookupSetsRepository xxrLookupSetsRepository;

	private static final Logger log = LoggerFactory.getLogger(SaveLookUpSetDaoImpl.class);

	@Transactional
	public String saveLookUpSet(List<SaveLookUpSetPo> lookUpSetHeadersPo,HttpServletRequest request) throws ValidationException,Exception {
		log.info("Start of saveLookUpSet Method in DaoImpl### ");
		String pmsg = "";
		try {
			StringBuilder sqlBuilder = new StringBuilder()
					.append("declare" + "     p_xxr_lookup_sets xxr_lookup_save_pkg.r_xxr_lookup_sets; "
							+ "p_user_id VARCHAR2(2000);"
							+ " p_msg VARCHAR2(2000);" + " begin ");
			int i = 1;
			
			sqlBuilder.append(
					" p_user_id\\:="+ "\'" + Utils.cleanEmail(request.getHeader("userId")) + "\';");	
			for (SaveLookUpSetPo lookUpSetHeader : lookUpSetHeadersPo) {

				/*
				 * if (Validations.isNullOrEmpty(lookUpSetHeader.getLookUpSetName())) throw new
				 * Exception("LookupSetName is Mandatory fields");
				 */

				if (lookUpSetHeader.getLookUpSetId() != null && lookUpSetHeader.getLookUpSetId() != 0) {
					sqlBuilder.append(
						" p_xxr_lookup_sets(" + i + ").lookup_set_id\\:=" + lookUpSetHeader.getLookUpSetId() + ";");	
				}
				else {
					Long lookUpSetId=xxrLookupSetsRepository.getLookupSetId(lookUpSetHeader.getLookUpSetName());
					if(lookUpSetId!=null)
						throw new ValidationException("LookupSetName Already Exists");
					sqlBuilder.append(" p_xxr_lookup_sets(" + i + ").lookup_set_id\\:= null;");
				}
				if (lookUpSetHeader.getLookUpSetName() != null)
					sqlBuilder.append(" p_xxr_lookup_sets(" + i + ").lookup_set_name\\:=" + "\'"
							+ lookUpSetHeader.getLookUpSetName() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(lookUpSetHeader.getDescription()))
					sqlBuilder.append(" p_xxr_lookup_sets(" + i + ").description\\:=" + "\'"
							+ lookUpSetHeader.getDescription() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(lookUpSetHeader.getRelatedTo()))
					sqlBuilder.append(" p_xxr_lookup_sets(" + i + ").related_to\\:=" + "\'"
							+ lookUpSetHeader.getRelatedTo() + "\';");
				else
					sqlBuilder.append(" p_xxr_lookup_sets(" + i + ").related_to\\:=" + "null"+";");
				if(!Validations.isNullOrEmptyorWhiteSpace(lookUpSetHeader.getLookupFlag()))
					sqlBuilder.append(" p_xxr_lookup_sets(" + i + ").lookup_flag\\:=" + "\'"
							+ lookUpSetHeader.getLookupFlag() + "\';");
				else
					sqlBuilder.append(" p_xxr_lookup_sets(" + i + ").lookup_flag\\:='N'");
				if (!Validations.isNullOrEmptyorWhiteSpace(lookUpSetHeader.getAttribute1()))
					sqlBuilder.append(" p_xxr_lookup_sets(" + i + ").attribute1\\:=" + "\'"
							+ lookUpSetHeader.getAttribute1() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(lookUpSetHeader.getAttribute2()))
					sqlBuilder.append(" p_xxr_lookup_sets(" + i + ").attribute2\\:=" + "\'"
							+ lookUpSetHeader.getAttribute2() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(lookUpSetHeader.getAttribute3()))
					sqlBuilder.append(" p_xxr_lookup_sets(" + i + ").attribute3\\:=" + "\'"
							+ lookUpSetHeader.getAttribute3() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(lookUpSetHeader.getAttribute4()))
					sqlBuilder.append(" p_xxr_lookup_sets(" + i + ").attribute4\\:=" + "\'"
							+ lookUpSetHeader.getAttribute4() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(lookUpSetHeader.getAttribute5()))
					sqlBuilder.append(" p_xxr_lookup_sets(" + i + ").attribute5\\:=" + "\'"
							+ lookUpSetHeader.getAttribute5() + "\';");
				
				/*
				 * sqlBuilder.append(" p_xxr_lookup_sets(" + i + ").attribute1\\:= null;");
				 * sqlBuilder.append(" p_xxr_lookup_sets(" + i + ").attribute2\\:= null;");
				 * sqlBuilder.append(" p_xxr_lookup_sets(" + i + ").attribute3\\:= null;");
				 * sqlBuilder.append(" p_xxr_lookup_sets(" + i + ").attribute4\\:= null;");
				 * sqlBuilder.append(" p_xxr_lookup_sets(" + i + ").attribute5\\:= null;");
				 */

				/*
				 * if
				 * (!Validations.isNullOrEmptyorWhiteSpace(lookUpSetHeader.getLastUpdatedBy()))
				 * sqlBuilder.append(" p_xxr_lookup_sets(" + i + ").last_updated_by\\:=" + "\'"
				 * + lookUpSetHeader.getLastUpdatedBy() + "\';"); if
				 * (!Validations.isNullOrEmptyorWhiteSpace(lookUpSetHeader.getCreatedBy()))
				 * sqlBuilder.append(" p_xxr_lookup_sets(" + i + ").created_by\\:=" + "\'" +
				 * lookUpSetHeader.getCreatedBy() + "\';");
				 */

				i++;
			}

			sqlBuilder.append("xxr_lookup_save_pkg.xxr_lookup_sets_prc(p_xxr_lookup_sets,p_user_id,p_msg);");
			sqlBuilder.append(" end;");

			Query query = entityManager.createNativeQuery(sqlBuilder.toString());

			int count = query.executeUpdate();

			log.info("count" + count);

			pmsg = "LookupSet Header saved successfully";

			entityManager.clear();
			entityManager.close();
		} catch (ValidationException e) {
			pmsg = "Exception while saving data";
			throw new ValidationException(e.getMessage());
		}catch (Exception e) {
			pmsg = "Exception while saving data";
			throw new Exception(e.getMessage());
		}
		return pmsg;

	}

}
