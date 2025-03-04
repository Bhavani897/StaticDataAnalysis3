package com.rite.products.convertrite.service;

import java.util.List;

import javax.servlet.http.HttpServletRequest;

import com.rite.products.convertrite.exception.BadRequestException;
import com.rite.products.convertrite.exception.ValidationException;
import com.rite.products.convertrite.model.XxrMappingSetsView;
import com.rite.products.convertrite.model.XxrMappingValue;
import com.rite.products.convertrite.model.XxrMappingValues;
import com.rite.products.convertrite.po.CloudColumnsPo;
import com.rite.products.convertrite.po.MappingSetsResPo;
import com.rite.products.convertrite.po.SaveCloudMappingSetColumnsPo;
import com.rite.products.convertrite.po.SaveCloudMappingSetColumnsResPo;
import com.rite.products.convertrite.po.SaveCloudMappingSetHdrPo;
import com.rite.products.convertrite.po.SaveCloudMappingSetHeaderJpaRes;
import com.rite.products.convertrite.po.SaveCloudMappingSetHeaderResPo;
import com.rite.products.convertrite.po.SaveCloudMappingSetHeadersPo;
import com.rite.products.convertrite.po.SaveCloudMappingSetValuesResPo;
import com.rite.products.convertrite.po.SourceColumnsPo;
import com.rite.products.convertrite.po.XxrMappingSetsResPo;

public interface CloudMappingSetService {

	String[] getSourceObjects(Long podId, Long projectId, Long parentObjectId, Long objectCodeId) throws Exception;

	List<SourceColumnsPo> getSourceColumns(String viewName) throws Exception;

	List<Object> getSourceFields(String viewName, String columnName) throws Exception;

	List<String> getCloudValues(Long objectId, String cloudColumn) throws Exception;

	List<XxrMappingSetsResPo> getCloudMappingSets(Long podId, Long projectId, Long objectId) throws Exception;

	List<XxrMappingSetsView> getAllCloudMappingSets(Long roleId) throws Exception;

	List<XxrMappingValues> getCloudMappingSetValues(Long mappingSetId) throws Exception;

	List<CloudColumnsPo> getCloudColumns(Long objectId) throws Exception;

	SaveCloudMappingSetHeaderResPo saveCloudMappingSetHeaders(
			List<SaveCloudMappingSetHeadersPo> saveCloudMappingSetHeadersPo, HttpServletRequest request)
			throws Exception;

	SaveCloudMappingSetColumnsResPo saveCloudMappingSetColumns(
			List<SaveCloudMappingSetColumnsPo> cloudMappingSetColumnsPo, HttpServletRequest request) throws Exception;

	SaveCloudMappingSetColumnsResPo saveCloudMappingSetValuesWithJpa(
			List<SaveCloudMappingSetColumnsPo> saveCloudMappingSetColumnsPos)
			throws BadRequestException, ValidationException;

	SaveCloudMappingSetHeaderJpaRes saveCloudMappingSetHdrWithJpa(
			List<SaveCloudMappingSetHeadersPo> saveCloudMappingSetHeadersPos)
			throws BadRequestException, ValidationException;

	String[] getAllSourceObjects() throws Exception;

	SaveCloudMappingSetHeaderResPo copyCloudMappingSet(String mapSetName, Long mapSetId, Long podId,
			HttpServletRequest request) throws Exception;

	List<MappingSetsResPo> getAllMappingSets() throws Exception;

	List<XxrMappingValue> getCloudMappingSetValue(Long mappingSetId) throws Exception;

	SaveCloudMappingSetHeaderResPo saveCloudMappingSetHdr(SaveCloudMappingSetHdrPo saveCloudMappingSetHeadersPo,
			HttpServletRequest request) throws ValidationException, Exception;

	SaveCloudMappingSetValuesResPo saveCloudMappingSetValues(
			List<SaveCloudMappingSetColumnsPo> cloudMappingSetColumnsPo, HttpServletRequest request)
			throws ValidationException, Exception;

	SaveCloudMappingSetHeaderResPo copyMappingSet(String newMapSetName, Long mapSetId, Long podId,
			HttpServletRequest request) throws Exception;

}
