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
import com.rite.products.convertrite.po.SaveCloudMappingSetColumnsPo;
import com.rite.products.convertrite.utils.Utils;

@Repository
public class SaveCloudMappingSetColumnsDaoImpl {

	@Autowired
	XxrMappingSetsRepository xxrColumnMapHdrRepository;

	@PersistenceContext
	private EntityManager entityManager;

	private static final Logger log = LoggerFactory.getLogger(SaveCloudMappingSetColumnsDaoImpl.class);

	@Transactional
	public String saveCloudMappingSetColumns(List<SaveCloudMappingSetColumnsPo> cloudMappingSetColumnsPo,
			HttpServletRequest request) throws BadRequestException, Exception {
		log.info("Start of saveCloudMappingSetColumns Method in DaoImpl### ");

		String pmsg = "";
		String mappingType = "";
		try {
			StringBuilder sqlBuilder = new StringBuilder()
					.append("declare" + "       p_xxr_mapping_values xxr_mapping_save_pkg.r_xxr_mapping_values ;"
							+ "p_user_id VARCHAR2(2000);" + " p_msg VARCHAR2(2000);" + " begin ");
			sqlBuilder.append(" p_user_id\\:=" + "\'" + Utils.cleanEmail(request.getHeader("userId")) + "\';");
			int i = 1;
			for (SaveCloudMappingSetColumnsPo mappingColumn : cloudMappingSetColumnsPo) {

				if (mappingColumn.getMapSetId() == null)
					throw new BadRequestException("mapSetId is Mandatory field");

				mappingType = xxrColumnMapHdrRepository.getMappingType(mappingColumn.getMapSetId());

				if (Validations.isNullOrEmptyorWhiteSpace(mappingType))
					throw new BadRequestException("mappingType is not defined for this MapSetId");

				/*
				 * if (("One to One").equalsIgnoreCase(mappingType)) { if
				 * (Validations.isNullOrEmptyorWhiteSpace(mappingColumn.getSourceField1()) ||
				 * Validations.isNullOrEmptyorWhiteSpace(mappingColumn.getCloudValue())) throw
				 * new BadRequestException("sourceField1 and cloudValue are Mandatory fields");
				 * } else if (("Two to One").equalsIgnoreCase(mappingType)) { if
				 * (Validations.isNullOrEmptyorWhiteSpace(mappingColumn.getSourceField1()) ||
				 * Validations.isNullOrEmptyorWhiteSpace(mappingColumn.getSourceField2()) ||
				 * Validations.isNullOrEmptyorWhiteSpace(mappingColumn.getCloudValue())) throw
				 * new
				 * BadRequestException("sourceField1,sourceField2 and cloudValue are Mandatory fields"
				 * ); } else if (("Three to One").equalsIgnoreCase(mappingType)) { if
				 * (Validations.isNullOrEmptyorWhiteSpace(mappingColumn.getSourceField1()) ||
				 * Validations.isNullOrEmptyorWhiteSpace(mappingColumn.getSourceField2()) ||
				 * Validations.isNullOrEmptyorWhiteSpace(mappingColumn.getSourceField3()) ||
				 * Validations.isNullOrEmptyorWhiteSpace(mappingColumn.getCloudValue())) throw
				 * new BadRequestException(
				 * "sourceField1,sourceField2,sourceField3 and cloudValue are Mandatory fields"
				 * ); }
				 */

				sqlBuilder
						.append(" p_xxr_mapping_values(" + i + ").map_set_id\\:=" + mappingColumn.getMapSetId() + ";");
				if (mappingColumn.getMapLineId() != null && mappingColumn.getMapLineId() != 0)
					sqlBuilder.append(
							" p_xxr_mapping_values(" + i + ").map_line_id\\:=" + mappingColumn.getMapLineId() + ";");
				else {
					sqlBuilder.append(" p_xxr_mapping_values(" + i + ").map_line_id\\:= null;");
				}
				if (!Validations.isNullOrEmptyorWhiteSpace(mappingColumn.getSourceField1()))
					sqlBuilder.append(" p_xxr_mapping_values(" + i + ").source_field1\\:=" + "\'"
							+ mappingColumn.getSourceField1() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(mappingColumn.getSourceField2()))
					sqlBuilder.append(" p_xxr_mapping_values(" + i + ").source_field2\\:=" + "\'"
							+ mappingColumn.getSourceField2() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(mappingColumn.getSourceField3()))
					sqlBuilder.append(" p_xxr_mapping_values(" + i + ").source_field3\\:=" + "\'"
							+ mappingColumn.getSourceField3() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(mappingColumn.getEnabled()))
					sqlBuilder.append(" p_xxr_mapping_values(" + i + ").enabled_flag\\:=" + "\'"
							+ mappingColumn.getEnabled() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(mappingColumn.getCloudValue()))
					sqlBuilder.append(" p_xxr_mapping_values(" + i + ").cloud_value\\:=" + "\'"
							+ mappingColumn.getCloudValue() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(mappingColumn.getAttribute1()))
					sqlBuilder.append(" p_xxr_mapping_values(" + i + ").attribute1\\:=" + "\'"
							+ mappingColumn.getAttribute1() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(mappingColumn.getAttribute2()))
					sqlBuilder.append(" p_xxr_mapping_values(" + i + ").attribute2\\:=" + "\'"
							+ mappingColumn.getAttribute2() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(mappingColumn.getAttribute3()))
					sqlBuilder.append(" p_xxr_mapping_values(" + i + ").attribute3\\:=" + "\'"
							+ mappingColumn.getAttribute3() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(mappingColumn.getAttribute4()))
					sqlBuilder.append(" p_xxr_mapping_values(" + i + ").attribute4\\:=" + "\'"
							+ mappingColumn.getAttribute4() + "\';");
				if (!Validations.isNullOrEmptyorWhiteSpace(mappingColumn.getAttribute5()))
					sqlBuilder.append(" p_xxr_mapping_values(" + i + ").attribute5\\:=" + "\'"
							+ mappingColumn.getAttribute5() + "\';");

				i++;
			}

			sqlBuilder.append(" xxr_mapping_save_pkg.xxr_mapping_values_prc(p_xxr_mapping_values,p_user_id,p_msg);");
			sqlBuilder.append(" end;");

			Query query = entityManager.createNativeQuery(sqlBuilder.toString());

			int count = query.executeUpdate();

			log.info("count" + count);

			pmsg = "MappingSet Columns saved successfully";

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

}
