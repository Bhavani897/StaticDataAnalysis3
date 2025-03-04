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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.rite.products.convertrite.Validations.Validations;
import com.rite.products.convertrite.exception.BadRequestException;
import com.rite.products.convertrite.exception.ValidationException;
import com.rite.products.convertrite.po.SaveLookUpValuesPo;
import com.rite.products.convertrite.utils.Utils;

@Repository
public class SaveLookUpValuesDaoImpl {

	@PersistenceContext
	private EntityManager entityManager;

	private static final Logger log = LoggerFactory.getLogger(SaveLookUpValuesDaoImpl.class);
	
	@Autowired
	XxrLookUpValuesRepository xxrLookUpValuesRepository;
	
	@Transactional
	public String saveLookUpValues(List<SaveLookUpValuesPo> lookUpValuesPo,HttpServletRequest request) throws ValidationException,BadRequestException,Exception {
		log.info("Start of saveLookUpValues Method in DaoImpl### ");

		String pmsg = "";
		try {
			StringBuilder sqlBuilder = new StringBuilder()
					.append("declare" + "    p_xxr_lookup_values xxr_lookup_save_pkg.r_xxr_lookup_values;"
							+"p_user_id VARCHAR2(2000);"
							+ " p_msg VARCHAR2(2000);" + " begin ");
			sqlBuilder.append(
					" p_user_id\\:=" +"\'"  +Utils.cleanEmail(request.getHeader("userId")) + "\';");	
			int i = 1;
			for (SaveLookUpValuesPo lookupValue : lookUpValuesPo) {

				if (Validations.isNullOrEmpty(lookupValue.getLookUpValue()) || lookupValue.getLookUpSetId() == null)
					throw new BadRequestException("lookUpSetId and lookUpValue are Mandatory fields");

				sqlBuilder.append(
						" p_xxr_lookup_values(" + i + ").lookup_set_id\\:=" + lookupValue.getLookUpSetId() + ";");
				if (!Validations.isNullOrEmptyorWhiteSpace(lookupValue.getLookUpValue()))
					sqlBuilder.append("  p_xxr_lookup_values(" + i + ").lookup_value\\:=" + "\'"
							+ lookupValue.getLookUpValue() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(lookupValue.getEnabledFlag()))
					sqlBuilder.append("  p_xxr_lookup_values(" + i + ").enabled_flag\\:=" + "\'"
							+ lookupValue.getEnabledFlag() + "\';");
				if (lookupValue.getStartDate() != null)
					sqlBuilder.append("  p_xxr_lookup_values(" + i + ").start_date\\:=" + "\'"
							+ new SimpleDateFormat("dd-MMM-yyyy").format(lookupValue.getStartDate()) + "\';");
				else
					sqlBuilder.append("  p_xxr_lookup_values(" + i + ").start_date\\:=null;");
				if (lookupValue.getEndDate() != null)
					sqlBuilder.append(" p_xxr_lookup_values(" + i + ").end_date\\:=" + "\'"
							+ new SimpleDateFormat("dd-MMM-yyyy").format(lookupValue.getEndDate()) + "\';");
				else
					sqlBuilder.append("  p_xxr_lookup_values(" + i + ").end_date\\:=null;");
				if (lookupValue.getLookUpValueId() != null && lookupValue.getLookUpValueId() != 0) {
					sqlBuilder.append(" p_xxr_lookup_values(" + i + ").lookup_value_id\\:="
							+ lookupValue.getLookUpValueId() + ";");
				}
				else {
					Long lookValueId=xxrLookUpValuesRepository.getIdByValuesetId(lookupValue.getLookUpValue(),lookupValue.getLookUpSetId());
					if(lookValueId!=null)
						throw new ValidationException("LookValue Already Exists :::" +lookupValue.getLookUpValue());
					sqlBuilder.append(" p_xxr_lookup_values(" + i + ").lookup_value_id\\:= null;");
				}
				
				if (!Validations.isNullOrEmptyorWhiteSpace(lookupValue.getActualValue()))
					sqlBuilder.append("  p_xxr_lookup_values(" + i + ").actual_value\\:=" + "\'"
							+ lookupValue.getActualValue() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(lookupValue.getAttribute1()))
					sqlBuilder.append("  p_xxr_lookup_values(" + i + ").attribute1\\:=" + "\'"
							+ lookupValue.getAttribute1() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(lookupValue.getAttribute2()))
					sqlBuilder.append("  p_xxr_lookup_values(" + i + ").attribute2\\:=" + "\'"
							+ lookupValue.getAttribute2() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(lookupValue.getAttribute3()))
					sqlBuilder.append("  p_xxr_lookup_values(" + i + ").attribute3\\:=" + "\'"
							+ lookupValue.getAttribute3() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(lookupValue.getAttribute4()))
					sqlBuilder.append("  p_xxr_lookup_values(" + i + ").attribute4\\:=" + "\'"
							+ lookupValue.getAttribute4() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(lookupValue.getAttribute5()))
					sqlBuilder.append("  p_xxr_lookup_values(" + i + ").attribute5\\:=" + "\'"
							+ lookupValue.getAttribute5() + "\';");
				/*
				 * if (!Validations.isNullOrEmptyorWhiteSpace(lookupValue.getCreatedBy()))
				 * sqlBuilder.append("  p_xxr_lookup_values(" + i + ").created_by\\:=" + "\'" +
				 * lookupValue.getCreatedBy() + "\';"); if
				 * (!Validations.isNullOrEmptyorWhiteSpace(lookupValue.getLastUpdateBy()))
				 * sqlBuilder.append("  p_xxr_lookup_values(" + i + ").last_updated_by\\:=" +
				 * "\'" + lookupValue.getLastUpdateBy() + "\';");
				 */

				i++;
			}

			sqlBuilder.append(" xxr_lookup_save_pkg.xxr_lookup_values_prc(p_xxr_lookup_values,p_user_id,p_msg);");
			sqlBuilder.append(" end;");

			Query query = entityManager.createNativeQuery(sqlBuilder.toString());

			int count = query.executeUpdate();

			log.info("count" + count);

			pmsg = "Lookup values  saved successfully";
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

	/*
	 * public List getParentObjectLov(@Param("projectId") Long projectId) throws
	 * Exception { log.info("Start of getParentObjectLov Method in DaoImpl### ");
	 * List parentObjectList =new ArrayList<>(); try {
	 * 
	 * Query q = entityManager.createNativeQuery(
	 * "select distinct(a.Parent_Object_Id),b.lookup_Value from xxr_proj_activities a, XXR_LookUP_Values b where a.Parent_Object_Id= b.LookUp_Value_Id and a.project_Id=:projectId"
	 * ); parentObjectList= q.getResultList(); } catch (Exception e) {
	 * e.printStackTrace(); throw new Exception(e.getMessage()); } return
	 * parentObjectList; }
	 */

}
