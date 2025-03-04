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
import com.rite.products.convertrite.po.SaveCloudMappingSetHeadersPo;
import com.rite.products.convertrite.utils.Utils;

@Repository
public class SaveCloudMappingSetHeadersDaoImpl {

	@PersistenceContext
	private EntityManager entityManager;

	private static final Logger log = LoggerFactory.getLogger(SaveCloudMappingSetHeadersDaoImpl.class);

	@Autowired
	XxrMappingSetsRepository xxrMappingSetsRepository;

	@Transactional
	public String saveCloudMappingSetHeaders(List<SaveCloudMappingSetHeadersPo> mappingHeaders,
			HttpServletRequest request) throws ValidationException, Exception {
		log.info("Start of saveCloudMappingSetHeaders Method in DaoImpl### ");
		String pmsg = "";
		try {
			StringBuilder sqlBuilder = new StringBuilder()
					.append("declare" + "  p_xxr_mapping_sets xxr_mapping_save_pkg.r_xxr_mapping_sets;"
							+ "p_user_id VARCHAR2(2000);" + " p_msg VARCHAR2(2000);" + " begin ");
			sqlBuilder.append(" p_user_id\\:=" + "\'" + Utils.cleanEmail(request.getHeader("userId")) + "\';");
			int i = 1;

			for (SaveCloudMappingSetHeadersPo mappingHeader : mappingHeaders) {

				/*
				 * if ( Validations.isNullOrEmpty(mappingHeader.getMapSetName()) ||
				 * Validations.isNullOrEmpty(mappingHeader.getMapSetType()) ||
				 * Validations.isNullOrEmpty(mappingHeader.getSourceObject1()) ||
				 * Validations.isNullOrEmpty(mappingHeader.getCloudColumn()) ||
				 * Validations.isNullOrEmpty(mappingHeader.getSourceColumn1()) ||
				 * mappingHeader.getPodId()==null || mappingHeader.getProjectId()==null ||
				 * mappingHeader.getObjectId()==null || mappingHeader.getParentObjectId()==null
				 * ) throw new Exception(
				 * "mapSetName,mapSetType,sourceObject1,sourceColumn1,podId,projectId,parentObjectId,objectId and cloudColumn are Mandatory fields"
				 * );
				 */
				if (mappingHeader.getMapSetId() != null && mappingHeader.getMapSetId() != 0)
					sqlBuilder.append(
							"  p_xxr_mapping_sets(" + i + ").map_set_id\\:=" + mappingHeader.getMapSetId() + ";");
				else {
					Long mappingSetId = xxrMappingSetsRepository.getMapId(mappingHeader.getMapSetName());
					if (mappingSetId != null)
						throw new ValidationException(" This MappingSetName already Exists");
					sqlBuilder.append("  p_xxr_mapping_sets(" + i + ").map_set_id\\:=null;");
				}
				sqlBuilder.append("  p_xxr_mapping_sets(" + i + ").map_set_name\\:=" + "\'"
						+ mappingHeader.getMapSetName() + "\';");
				sqlBuilder.append("  p_xxr_mapping_sets(" + i + ").map_set_type\\:=" + "\'"
						+ mappingHeader.getMapSetType() + "\';");
				if (mappingHeader.getPodId() != null)
					sqlBuilder.append("  p_xxr_mapping_sets(" + i + ").pod_id\\:=" + mappingHeader.getPodId() + ";");
				if (mappingHeader.getProjectId() != null)
					sqlBuilder.append(
							"  p_xxr_mapping_sets(" + i + ").project_id\\:=" + mappingHeader.getProjectId() + ";");
				if (mappingHeader.getParentObjectId() != null)
					sqlBuilder.append("  p_xxr_mapping_sets(" + i + ").parent_object_id\\:="
							+ mappingHeader.getParentObjectId() + ";");
				if (mappingHeader.getObjectId() != null)
					sqlBuilder.append(
							"  p_xxr_mapping_sets(" + i + ").object_id\\:=" + mappingHeader.getObjectId() + ";");
				sqlBuilder.append("  p_xxr_mapping_sets(" + i + ").cloud_column\\:=" + "\'"
						+ mappingHeader.getCloudColumn() + "\';");

				sqlBuilder.append("  p_xxr_mapping_sets(" + i + ").lookup_set_name\\:=" + "\'"
						+ mappingHeader.getLookupSetName() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(mappingHeader.getSourceObject1()))
					sqlBuilder.append("  p_xxr_mapping_sets(" + i + ").source_object1\\:=" + "\'"
							+ mappingHeader.getSourceObject1() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(mappingHeader.getSourceObject2()))
					sqlBuilder.append("  p_xxr_mapping_sets(" + i + ").source_object2\\:=" + "\'"
							+ mappingHeader.getSourceObject2() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(mappingHeader.getSourceObject3()))
					sqlBuilder.append("  p_xxr_mapping_sets(" + i + ").source_object3\\:=" + "\'"
							+ mappingHeader.getSourceObject3() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(mappingHeader.getSourceColumn1()))
					sqlBuilder.append("  p_xxr_mapping_sets(" + i + ").source_column1\\:=" + "\'"
							+ mappingHeader.getSourceColumn1() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(mappingHeader.getSourceColumn2()))
					sqlBuilder.append("  p_xxr_mapping_sets(" + i + ").source_column2\\:=" + "\'"
							+ mappingHeader.getSourceColumn2() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(mappingHeader.getSourceColumn3()))
					sqlBuilder.append("  p_xxr_mapping_sets(" + i + ").source_column3\\:=" + "\'"
							+ mappingHeader.getSourceColumn3() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(mappingHeader.getAttribute1()))
					sqlBuilder.append("  p_xxr_mapping_sets(" + i + ").attribute1\\:=" + "\'"
							+ mappingHeader.getAttribute1() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(mappingHeader.getAttribute2()))
					sqlBuilder.append("  p_xxr_mapping_sets(" + i + ").attribute2\\:=" + "\'"
							+ mappingHeader.getAttribute2() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(mappingHeader.getAttribute3()))
					sqlBuilder.append("  p_xxr_mapping_sets(" + i + ").attribute3\\:=" + "\'"
							+ mappingHeader.getAttribute3() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(mappingHeader.getAttribute4()))
					sqlBuilder.append("  p_xxr_mapping_sets(" + i + ").attribute4\\:=" + "\'"
							+ mappingHeader.getAttribute4() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(mappingHeader.getAttribute5()))
					sqlBuilder.append("  p_xxr_mapping_sets(" + i + ").attribute5\\:=" + "\'"
							+ mappingHeader.getAttribute5() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(mappingHeader.getWhereClause()))
					sqlBuilder.append("  p_xxr_mapping_sets(" + i + ").where_clause\\:=" + "\'"
							+ mappingHeader.getWhereClause() + "\';");

				i++;
			}

			sqlBuilder.append(" xxr_mapping_save_pkg.xxr_mapping_sets_prc(p_xxr_mapping_sets,p_user_id,p_msg);");
			sqlBuilder.append(" end;");

			Query query = entityManager.createNativeQuery(sqlBuilder.toString());

			int count = query.executeUpdate();

			log.info("count" + count);

			pmsg = "CloudMappingSet Header is successfully saved";

			entityManager.clear();
			entityManager.close();
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
