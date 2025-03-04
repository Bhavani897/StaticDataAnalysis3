package com.rite.products.convertrite.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.rite.products.convertrite.Validations.Validations;
import com.rite.products.convertrite.exception.BadRequestException;
import com.rite.products.convertrite.exception.ValidationException;
import com.rite.products.convertrite.model.XxrMappingSet;
import com.rite.products.convertrite.model.XxrMappingSets;
import com.rite.products.convertrite.model.XxrMappingSetsView;
import com.rite.products.convertrite.model.XxrMappingValue;
import com.rite.products.convertrite.model.XxrMappingValues;
import com.rite.products.convertrite.po.CloudColumnsPo;
import com.rite.products.convertrite.po.LovValuesPo;
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
import com.rite.products.convertrite.respository.CloudMetaDataRepository;
import com.rite.products.convertrite.respository.SaveCloudMappingSetColumnsDaoImpl;
import com.rite.products.convertrite.respository.SaveCloudMappingSetHeadersDaoImpl;
import com.rite.products.convertrite.respository.SourceTemplateColumnsRepository;
import com.rite.products.convertrite.respository.SourceTemplateHeadersDaoImpl;
import com.rite.products.convertrite.respository.SourceTemplateHeadersRepository;
import com.rite.products.convertrite.respository.XxrCloudColumnsRepository;
import com.rite.products.convertrite.respository.XxrCloudMasterLookupTabRepository;
import com.rite.products.convertrite.respository.XxrCloudMasterLookupValueTabRepository;
import com.rite.products.convertrite.respository.XxrLookUpValuesRepository;
import com.rite.products.convertrite.respository.XxrLookupSetsRepository;
import com.rite.products.convertrite.respository.XxrMappingSetRepository;
import com.rite.products.convertrite.respository.XxrMappingSetsRepository;
import com.rite.products.convertrite.respository.XxrMappingSetsViewRepository;
import com.rite.products.convertrite.respository.XxrMappingValueRepository;
import com.rite.products.convertrite.respository.XxrMappingValuesRepository;
import com.rite.products.convertrite.respository.XxrSrcLookupSetRepository;
import com.rite.products.convertrite.utils.Utils;

@Service
public class CloudMappingSetServiceImpl implements CloudMappingSetService {

	private static final Logger log = LoggerFactory.getLogger(CloudMappingSetServiceImpl.class);

	@Autowired
	CloudMetaDataRepository cloudMetaDataRepository;
	@Autowired
	SourceTemplateHeadersRepository sourceTemplateHeadersRepository;
	@Autowired
	SourceTemplateColumnsRepository sourceTemplateColumnsRepository;
	@Autowired
	SourceTemplateHeadersDaoImpl sourceTemplateHeadersDaoImpl;
	@Autowired
	XxrCloudMasterLookupValueTabRepository xxrCloudMasterLookupValueTabRepository;
	@Autowired
	XxrCloudMasterLookupTabRepository xxrCloudMasterLookupTabRepository;
	@Autowired
	XxrMappingSetsRepository xxrColumnMapHdrRepository;
	@Autowired
	XxrMappingValuesRepository xxrColumnMapLineRepository;
	@Autowired
	XxrCloudColumnsRepository xxrCloudColumnsRepository;
	@Autowired
	SaveCloudMappingSetHeadersDaoImpl saveCloudMappingSetHeadersDaoImpl;
	@Autowired
	SaveCloudMappingSetColumnsDaoImpl saveCloudMappingSetColumnsDaoImpl;
	@Autowired
	XxrLookUpValuesRepository xxrLookUpValuesRepository;
	@Autowired
	XxrLookupSetsRepository xxrLookupSetsRepository;
	@Autowired
	XxrMappingSetRepository xxrMappingSetRepository;
	@Autowired
	XxrSrcLookupSetRepository xxrSrcLookupSetRepository;
	@Autowired
	XxrMappingValueRepository xxrMappingValueRepository;
	@Autowired
	XxrMappingSetsViewRepository xxrMappingSetsViewRepository;

	@Override
	public String[] getSourceObjects(Long podId, Long projectId, Long parentObjectId, Long objectCodeId)
			throws Exception {
		log.info("Start Of getSourceObjects Method in Service Layer ####");
		String[] sourceObjects = {};
		try {
			/*
			 * String podValue = cloudMetaDataRepository.getValueById(podId); String
			 * projectName = cloudMetaDataRepository.getValueById(projectId); String
			 * parentObjectCode = cloudMetaDataRepository.getValueById(parentObjectId);
			 * String objectCode = cloudMetaDataRepository.getValueById(objectCodeId);
			 * log.info("podValue:::::" + podValue + "projectName:::" + projectName +
			 * "parentObjectCode::::" + parentObjectCode + "objectCode:::" + objectCode); if
			 * (!Validations.isNullOrEmpty(podValue) &&
			 * !Validations.isNullOrEmpty(projectName) &&
			 * !Validations.isNullOrEmpty(parentObjectCode) &&
			 * !Validations.isNullOrEmpty(objectCode))
			 */

			sourceObjects = sourceTemplateHeadersRepository.getSourceObjects( projectId, parentObjectId,
					objectCodeId);

		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return sourceObjects;
	}

	@Override
	public List<SourceColumnsPo> getSourceColumns(String viewName) throws Exception {
		log.info("Start Of getSourceColumns Method in Service Layer ####");
		List<SourceColumnsPo> sourceTemplateColumns = new ArrayList<>();
		try {
			Long templateId = sourceTemplateHeadersRepository.getTemplateIdByViewName(viewName);
			if (templateId != null)
				sourceTemplateColumns = sourceTemplateColumnsRepository.getSourceColumns(templateId);
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}

		return sourceTemplateColumns;

	}

	@Override
	public List<Object> getSourceFields(String viewName, String columnName) throws Exception {

		log.info("Start Of getSourceFields Method in Service Layer ####");
		List<Object> sourceFields = new ArrayList<>();
		try {

			sourceFields = sourceTemplateHeadersDaoImpl.getSourceFields(columnName, viewName);

		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}

		return sourceFields;
	}

	@Override
	public List<String> getCloudValues(Long objectId, String cloudColumn) throws Exception {
		log.info("Start Of getCloudValues Method in Service Layer ####");
		// String[] cloudValues = new String[100];
		List<String> cloudValues = new ArrayList<String>();
		try {
//			Long lookupSetId = xxrCloudMasterLookupTabRepository.getLookUpSetId(objectCode, cloudColumn);
//			if (lookupSetId != null)
//				cloudValues = xxrClLovValuesPooudMasterLookupValueTabRepository.getlookupValue(lookupSetId);
			List<LovValuesPo> lovValues = cloudMetaDataRepository.getValues(cloudColumn);

			for (LovValuesPo lovVal : lovValues) {
				cloudValues.add(lovVal.getValue());
			}

		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return cloudValues;
	}

	@Override
	public List<XxrMappingSetsResPo> getCloudMappingSets(Long podId, Long projectId, Long objectId) throws Exception {
		log.info("Start Of getCloudMappingSets Method in Service Layer ####");
		List<XxrMappingSets> cloudMappingSetList = new ArrayList<>();
		List<XxrMappingSetsResPo> mappingSetResPoList = new ArrayList<>();
		try {
			/*
			 * String podValue = cloudMetaDataRepository.getValueById(podId); String
			 * projectName = cloudMetaDataRepository.getValueById(projectId);
			 * log.info("podValue:::::" + podValue + "projectName:::" + projectName); if
			 * (!Validations.isNullOrEmpty(podValue) &&
			 * !Validations.isNullOrEmpty(projectName) &&objectId!=null)
			 */
			cloudMappingSetList = xxrColumnMapHdrRepository.getCloudMappingSets(podId, projectId, objectId);

			cloudMappingSetList.stream().forEach(x -> {

				XxrMappingSetsResPo xxrMappingSetsResPo = new XxrMappingSetsResPo();

				String podName = xxrLookUpValuesRepository.getValueById(x.getPodId());
				String projectName = xxrLookUpValuesRepository.getValueById(x.getProjectId());
				String objectCode = xxrLookUpValuesRepository.getValueById(x.getObjectId());
				String parentObjectCode = xxrLookUpValuesRepository.getValueById(x.getParentObjectId());

				if (x.getMapSetId() != null)
					xxrMappingSetsResPo.setMapSetId(x.getMapSetId());
				if (!Validations.isNullOrEmpty(x.getMapSetName()))
					xxrMappingSetsResPo.setMapSetName(x.getMapSetName());
				if (!Validations.isNullOrEmpty(x.getMapSetType()))
					xxrMappingSetsResPo.setMapSetType(x.getMapSetType());
				if (!Validations.isNullOrEmpty(x.getCloudColumn()))
					xxrMappingSetsResPo.setCloudColumn(x.getCloudColumn());
				xxrMappingSetsResPo.setPodId(x.getPodId());
				if (!Validations.isNullOrEmpty(podName))
					xxrMappingSetsResPo.setPodName(podName);
				if (!Validations.isNullOrEmpty(objectCode))
					xxrMappingSetsResPo.setObjectCode(objectCode);
				xxrMappingSetsResPo.setObjectId(x.getObjectId());
				if (!Validations.isNullOrEmpty(parentObjectCode))
					xxrMappingSetsResPo.setParentObjectCode(parentObjectCode);
				xxrMappingSetsResPo.setParentObjectId(x.getParentObjectId());
				xxrMappingSetsResPo.setProjectId(x.getProjectId());
				if (!Validations.isNullOrEmpty(projectName))
					xxrMappingSetsResPo.setProjectName(projectName);
				if (!Validations.isNullOrEmpty(x.getSourceColumn1()))
					xxrMappingSetsResPo.setSourceColumn1(x.getSourceColumn1());
				if (!Validations.isNullOrEmpty(x.getSourceColumn2()))
					xxrMappingSetsResPo.setSourceColumn2(x.getSourceColumn2());
				if (!Validations.isNullOrEmpty(x.getSourceColumn3()))
					xxrMappingSetsResPo.setSourceColumn3(x.getSourceColumn3());
				if (!Validations.isNullOrEmpty(x.getSourceObject1()))
					xxrMappingSetsResPo.setSourceObject1(x.getSourceObject1());
				if (!Validations.isNullOrEmpty(x.getSourceObject2()))
					xxrMappingSetsResPo.setSourceObject2(x.getSourceObject2());
				if (!Validations.isNullOrEmpty(x.getSourceObject3()))
					xxrMappingSetsResPo.setSourceObject3(x.getSourceObject3());
				if (!Validations.isNullOrEmpty(x.getAttribute1()))
					xxrMappingSetsResPo.setAttribute1(x.getAttribute1());
				if (!Validations.isNullOrEmpty(x.getAttribute2()))
					xxrMappingSetsResPo.setAttribute2(x.getAttribute2());
				if (!Validations.isNullOrEmpty(x.getAttribute3()))
					xxrMappingSetsResPo.setAttribute3(x.getAttribute3());
				if (!Validations.isNullOrEmpty(x.getAttribute4()))
					xxrMappingSetsResPo.setAttribute4(x.getAttribute4());
				if (!Validations.isNullOrEmpty(x.getAttribute5()))
					xxrMappingSetsResPo.setAttribute5(x.getAttribute5());
				if (!Validations.isNullOrEmpty(x.getWhereClause()))
					xxrMappingSetsResPo.setWhereClause(x.getWhereClause());

				mappingSetResPoList.add(xxrMappingSetsResPo);
			});

		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return mappingSetResPoList;
	}

	/*
	 * @Override public List<XxrMappingSetsResPo> getAllCloudMappingSets() throws
	 * Exception {
	 * log.info("Start Of getAllCloudMappingSets Method in Service Layer ####");
	 * List<XxrMappingSets> cloudMappingSetList = new ArrayList<>();
	 * List<XxrMappingSetsResPo> mappingSetResPoList = new ArrayList<>(); try {
	 * 
	 * cloudMappingSetList = xxrColumnMapHdrRepository.findAll();
	 * 
	 * cloudMappingSetList.stream().forEach(x -> {
	 * 
	 * XxrMappingSetsResPo xxrMappingSetsResPo = new XxrMappingSetsResPo();
	 * 
	 * String podName = xxrLookUpValuesRepository.getValueById(x.getPodId()); String
	 * projectName = xxrLookUpValuesRepository.getValueById(x.getProjectId());
	 * String objectCode = xxrLookUpValuesRepository.getValueById(x.getObjectId());
	 * String parentObjectCode =
	 * xxrLookUpValuesRepository.getValueById(x.getParentObjectId()); Long
	 * lookupSetId = xxrLookupSetsRepository.getLookupSetId(x.getLookupSetName());
	 * 
	 * if (x.getMapSetId() != null)
	 * xxrMappingSetsResPo.setMapSetId(x.getMapSetId()); if
	 * (!Validations.isNullOrEmpty(x.getMapSetName()))
	 * xxrMappingSetsResPo.setMapSetName(x.getMapSetName()); if
	 * (!Validations.isNullOrEmpty(x.getMapSetType()))
	 * xxrMappingSetsResPo.setMapSetType(x.getMapSetType()); if
	 * (!Validations.isNullOrEmpty(x.getCloudColumn()))
	 * xxrMappingSetsResPo.setCloudColumn(x.getCloudColumn());
	 * xxrMappingSetsResPo.setPodId(x.getPodId()); if
	 * (!Validations.isNullOrEmpty(podName))
	 * xxrMappingSetsResPo.setPodName(podName); if
	 * (!Validations.isNullOrEmpty(objectCode))
	 * xxrMappingSetsResPo.setObjectCode(objectCode);
	 * xxrMappingSetsResPo.setObjectId(x.getObjectId()); if
	 * (!Validations.isNullOrEmpty(parentObjectCode))
	 * xxrMappingSetsResPo.setParentObjectCode(parentObjectCode);
	 * xxrMappingSetsResPo.setParentObjectId(x.getParentObjectId());
	 * xxrMappingSetsResPo.setProjectId(x.getProjectId()); if
	 * (!Validations.isNullOrEmpty(projectName))
	 * xxrMappingSetsResPo.setProjectName(projectName); if
	 * (!Validations.isNullOrEmpty(x.getSourceColumn1()))
	 * xxrMappingSetsResPo.setSourceColumn1(x.getSourceColumn1()); if
	 * (!Validations.isNullOrEmpty(x.getSourceColumn2()))
	 * xxrMappingSetsResPo.setSourceColumn2(x.getSourceColumn2()); if
	 * (!Validations.isNullOrEmpty(x.getSourceColumn3()))
	 * xxrMappingSetsResPo.setSourceColumn3(x.getSourceColumn3()); if
	 * (!Validations.isNullOrEmpty(x.getSourceObject1()))
	 * xxrMappingSetsResPo.setSourceObject1(x.getSourceObject1()); if
	 * (!Validations.isNullOrEmpty(x.getSourceObject2()))
	 * xxrMappingSetsResPo.setSourceObject2(x.getSourceObject2()); if
	 * (!Validations.isNullOrEmpty(x.getSourceObject3()))
	 * xxrMappingSetsResPo.setSourceObject3(x.getSourceObject3()); if
	 * (!Validations.isNullOrEmpty(x.getAttribute1()))
	 * xxrMappingSetsResPo.setAttribute1(x.getAttribute1()); if
	 * (!Validations.isNullOrEmpty(x.getAttribute2()))
	 * xxrMappingSetsResPo.setAttribute2(x.getAttribute2()); if
	 * (!Validations.isNullOrEmpty(x.getAttribute3()))
	 * xxrMappingSetsResPo.setAttribute3(x.getAttribute3()); if
	 * (!Validations.isNullOrEmpty(x.getAttribute4()))
	 * xxrMappingSetsResPo.setAttribute4(x.getAttribute4()); if
	 * (!Validations.isNullOrEmpty(x.getAttribute5()))
	 * xxrMappingSetsResPo.setAttribute5(x.getAttribute5()); if
	 * (!Validations.isNullOrEmpty(x.getWhereClause()))
	 * xxrMappingSetsResPo.setWhereClause(x.getWhereClause());
	 * 
	 * xxrMappingSetsResPo.setLookupSetId(lookupSetId);
	 * xxrMappingSetsResPo.setLookupSetName(x.getLookupSetName());
	 * mappingSetResPoList.add(xxrMappingSetsResPo); });
	 * 
	 * } catch (Exception e) { throw new Exception(e.getMessage()); }
	 * 
	 * return mappingSetResPoList; }
	 */
	@Override
	public List<XxrMappingValues> getCloudMappingSetValues(Long mappingSetId) throws Exception {
		log.info("Start Of getCloudMappingSetValues Method in Service Layer ####");
		List<XxrMappingValues> cloudMappingSetValues = new ArrayList<>();
		try {
			cloudMappingSetValues = xxrColumnMapLineRepository.getMappingSetValues(mappingSetId);
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}

		return cloudMappingSetValues;
	}

	@Override
	public List<CloudColumnsPo> getCloudColumns(Long objectId) throws Exception {
		log.info("Start Of getCloudColumns Method in Service Layer ####");
		List<CloudColumnsPo> cloudColumnsList = new ArrayList<>();
		//List<Object[]> cloudColumnsObjectLi = new ArrayList<>();
		try {
			
			  cloudColumnsList = xxrCloudColumnsRepository.getCloudColumns(objectId);
			  
			  cloudColumnsList =
			  cloudColumnsList.stream().filter(Utils.distinctByKey(CloudColumnsPo::
			  getColumnName)) .collect(Collectors.toList());
			 

			/*
			 * cloudColumnsObjectLi = xxrCloudColumnsRepository.getCloudColumns(objectId);
			 * 
			 * 
			 * cloudColumnsObjectLi.stream().forEach(x -> { CloudColumnsPo cloudColumnsPo =
			 * new CloudColumnsPo(Long.parseLong(x.toString()), x.toString());
			 * cloudColumnsList.add(cloudColumnsPo); });
			 */
			

		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}

		return cloudColumnsList;
	}

	@Override
	public SaveCloudMappingSetHeaderResPo saveCloudMappingSetHeaders(
			List<SaveCloudMappingSetHeadersPo> saveCloudMappingSetHeadersPo, HttpServletRequest request)
			throws ValidationException, Exception {
		log.info("Start Of saveCloudMappingSetHeaders Method in Service Layer ####");
		String msg = "";
		long mapSetId = 0;
		SaveCloudMappingSetHeaderResPo saveCloudMappingSetHeaderResPo = new SaveCloudMappingSetHeaderResPo();
		try {
			msg = saveCloudMappingSetHeadersDaoImpl.saveCloudMappingSetHeaders(saveCloudMappingSetHeadersPo, request);
			saveCloudMappingSetHeaderResPo.setMessage(msg);
			SaveCloudMappingSetHeadersPo saveSourceTemplateHeadersPo = saveCloudMappingSetHeadersPo.get(0);
			String mapSetName = saveSourceTemplateHeadersPo.getMapSetName();
			if (!Validations.isNullOrEmpty(mapSetName)) {
				mapSetId = xxrColumnMapHdrRepository.getMapId(mapSetName);
				saveCloudMappingSetHeaderResPo.setMapSetId(mapSetId);
				saveCloudMappingSetHeaderResPo.setMapSetName(mapSetName);
			}
			log.info(msg + "msg###########");
		} catch (ValidationException e) {
			throw new ValidationException(e.getMessage());
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}

		return saveCloudMappingSetHeaderResPo;

	}

	@Override
	public SaveCloudMappingSetColumnsResPo saveCloudMappingSetColumns(
			List<SaveCloudMappingSetColumnsPo> cloudMappingSetColumnsPo, HttpServletRequest request)
			throws BadRequestException, Exception {
		log.info("Start of saveCloudMappingSetColumns in Service Layer ###");
		String msg = "";
		List<XxrMappingValues> cloudMappingSetColumns = new ArrayList<>();
		SaveCloudMappingSetColumnsResPo cloudMappingSetColumnsResPo = new SaveCloudMappingSetColumnsResPo();
		try {
			msg = saveCloudMappingSetColumnsDaoImpl.saveCloudMappingSetColumns(cloudMappingSetColumnsPo, request);
			cloudMappingSetColumnsResPo.setMessage(msg);
			Long mapSetId = cloudMappingSetColumnsPo.get(0).getMapSetId();
			if (mapSetId != null) {
				cloudMappingSetColumns = xxrColumnMapLineRepository.getMappingSetValues(mapSetId);
				cloudMappingSetColumnsResPo.setCloudMappingSetColumns(cloudMappingSetColumns);
			}
			log.info(msg + "msg###########");
		} catch (BadRequestException e) {
			throw new BadRequestException(e.getMessage());
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return cloudMappingSetColumnsResPo;
	}

	@Override
	public String[] getAllSourceObjects() throws Exception {
		log.info("Start Of getAllSourceObjects Method in Service Layer ####");
		String[] sourceObjects = {};
		try {
			sourceObjects = sourceTemplateHeadersRepository.getAllSourceObjects();

		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return sourceObjects;
	}

	@Override
	public SaveCloudMappingSetHeaderResPo copyCloudMappingSet(String newMapSetName, Long mapSetId, Long podId,
			HttpServletRequest request) throws Exception {
		log.info("Start Of copyCloudMappingSet Method in Service Layer ####");
		List<XxrMappingSets> xxrMappingSetList = new ArrayList<>();
		List<SaveCloudMappingSetHeadersPo> cloudMappingSetHeadersPo = new ArrayList<>();
		String msg = "";
		Long newMapSetId = null;
		List<XxrMappingValues> xxrMappingValuesList = new ArrayList<>();
		List<SaveCloudMappingSetColumnsPo> cloudMappingSetColumnsPo = new ArrayList<>();
		SaveCloudMappingSetHeaderResPo saveCloudMappingSetHeaderResPo = new SaveCloudMappingSetHeaderResPo();
		try {
			xxrMappingSetList = xxrColumnMapHdrRepository.getCloudMappingSetsById(mapSetId);

			xxrMappingSetList.stream().forEach(x -> {
				SaveCloudMappingSetHeadersPo saveCloudMappingSetHeadersPo = new SaveCloudMappingSetHeadersPo();

				saveCloudMappingSetHeadersPo.setMapSetName(newMapSetName);
				saveCloudMappingSetHeadersPo.setPodId(podId);
				saveCloudMappingSetHeadersPo.setMapSetType(x.getMapSetType());
				saveCloudMappingSetHeadersPo.setCloudColumn(x.getCloudColumn());
				saveCloudMappingSetHeadersPo.setObjectId(x.getObjectId());
				saveCloudMappingSetHeadersPo.setParentObjectId(x.getParentObjectId());
				saveCloudMappingSetHeadersPo.setProjectId(x.getProjectId());
				saveCloudMappingSetHeadersPo.setSourceColumn1(x.getSourceColumn1());
				saveCloudMappingSetHeadersPo.setSourceColumn2(x.getSourceColumn2());
				saveCloudMappingSetHeadersPo.setSourceColumn3(x.getSourceColumn3());
				saveCloudMappingSetHeadersPo.setSourceObject1(x.getSourceObject1());
				saveCloudMappingSetHeadersPo.setSourceObject2(x.getSourceObject2());
				saveCloudMappingSetHeadersPo.setSourceObject3(x.getSourceObject3());
				saveCloudMappingSetHeadersPo.setAttribute1(x.getAttribute1());
				saveCloudMappingSetHeadersPo.setAttribute2(x.getAttribute2());
				saveCloudMappingSetHeadersPo.setAttribute3(x.getAttribute3());
				saveCloudMappingSetHeadersPo.setAttribute4(x.getAttribute4());
				saveCloudMappingSetHeadersPo.setAttribute5(x.getAttribute5());
				saveCloudMappingSetHeadersPo.setWhereClause(x.getWhereClause());
				saveCloudMappingSetHeadersPo.setLookupSetName(x.getLookupSetName());
				cloudMappingSetHeadersPo.add(saveCloudMappingSetHeadersPo);
			});
			msg = saveCloudMappingSetHeadersDaoImpl.saveCloudMappingSetHeaders(cloudMappingSetHeadersPo, request);
			log.info("MappingsetHeader :: " + msg);

			if (!Validations.isNullOrEmpty(newMapSetName))
				newMapSetId = xxrColumnMapHdrRepository.getMapId(newMapSetName);

			xxrMappingValuesList = xxrColumnMapLineRepository.getMappingSetValues(mapSetId);

			for (XxrMappingValues xxrMappingValue : xxrMappingValuesList) {
				SaveCloudMappingSetColumnsPo saveCloudMappingSetColumnsPo = new SaveCloudMappingSetColumnsPo();

				saveCloudMappingSetColumnsPo.setMapSetId(newMapSetId);
				saveCloudMappingSetColumnsPo.setCloudValue(xxrMappingValue.getCloudValue());
				saveCloudMappingSetColumnsPo.setEnabled(xxrMappingValue.getEnabled());
				saveCloudMappingSetColumnsPo.setSourceField1(xxrMappingValue.getSourceField1());
				saveCloudMappingSetColumnsPo.setSourceField2(xxrMappingValue.getSourceField2());
				saveCloudMappingSetColumnsPo.setSourceField3(xxrMappingValue.getSourceField3());
				saveCloudMappingSetColumnsPo.setAttribute1(xxrMappingValue.getAttribute1());
				saveCloudMappingSetColumnsPo.setAttribute2(xxrMappingValue.getAttribute2());
				saveCloudMappingSetColumnsPo.setAttribute3(xxrMappingValue.getAttribute3());
				saveCloudMappingSetColumnsPo.setAttribute4(xxrMappingValue.getAttribute4());
				saveCloudMappingSetColumnsPo.setAttribute5(xxrMappingValue.getAttribute5());

				cloudMappingSetColumnsPo.add(saveCloudMappingSetColumnsPo);
			}
			;

			msg = saveCloudMappingSetColumnsDaoImpl.saveCloudMappingSetColumns(cloudMappingSetColumnsPo, request);
			log.info("MappingsetColumn :: " + msg);

			saveCloudMappingSetHeaderResPo.setMapSetId(newMapSetId);
			saveCloudMappingSetHeaderResPo.setMapSetName(newMapSetName);
			saveCloudMappingSetHeaderResPo.setMessage("MappingSet is Successfully copied");

		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return saveCloudMappingSetHeaderResPo;
	}

	@Override
	public List<MappingSetsResPo> getAllMappingSets() throws Exception {
		log.info("Start of getAllMappingSets#######");
		List<XxrMappingSet> xxrMappingSetLi = new ArrayList<>();
		List<MappingSetsResPo> mappingSetResLi = new ArrayList<>();
		try {
			xxrMappingSetLi = xxrMappingSetRepository.findAll();

			xxrMappingSetLi.stream().forEach(x -> {
				MappingSetsResPo mappingSetsResPo = new MappingSetsResPo();
				String sourceLookupSetName1 = "";
				String sourceLookupSetName2 = "";
				String sourceLookupSetName3 = "";

				String podName = xxrLookUpValuesRepository.getValueById(x.getPodId());
				String projectName = xxrLookUpValuesRepository.getValueById(x.getProjectId());
				String objectCode = xxrLookUpValuesRepository.getValueById(x.getObjectId());
				String parentObjectCode = xxrLookUpValuesRepository.getValueById(x.getParentObjectId());
				String cloudLookupSetName = xxrLookupSetsRepository.getLookupSetNamebyId(x.getCloudLookupSetId());
				if (x.getSourceLookupSetId1() != null)
					sourceLookupSetName1 = xxrSrcLookupSetRepository.getSrcLookupSetNameById(x.getSourceLookupSetId1());
				if (x.getSourceLookupSetId2() != null)
					sourceLookupSetName2 = xxrSrcLookupSetRepository.getSrcLookupSetNameById(x.getSourceLookupSetId2());
				if (x.getSourceLookupSetId3() != null)
					sourceLookupSetName3 = xxrSrcLookupSetRepository.getSrcLookupSetNameById(x.getSourceLookupSetId3());

				mappingSetsResPo.setMapSetId(x.getMapSetId());
				mappingSetsResPo.setMapSetName(x.getMapSetName());
				mappingSetsResPo.setMapSetType(x.getMapSetType());
				mappingSetsResPo.setPodId(x.getPodId());
				mappingSetsResPo.setPodName(podName);
				mappingSetsResPo.setProjectId(x.getProjectId());
				mappingSetsResPo.setProjectName(projectName);
				mappingSetsResPo.setParentObjectId(x.getParentObjectId());
				mappingSetsResPo.setParentObjectCode(parentObjectCode);
				mappingSetsResPo.setObjectId(x.getObjectId());
				mappingSetsResPo.setObjectCode(objectCode);
				mappingSetsResPo.setCloudLookupSetId(x.getCloudLookupSetId());
				mappingSetsResPo.setCloudLookupSetName(cloudLookupSetName);
				mappingSetsResPo.setSourcelookupSetId1(x.getSourceLookupSetId1());
				mappingSetsResPo.setSourcelookupSetId2(x.getSourceLookupSetId2());
				mappingSetsResPo.setSourcelookupSetId3(x.getSourceLookupSetId3());
				mappingSetsResPo.setSourceLookupSetName1(sourceLookupSetName1);
				mappingSetsResPo.setSourceLookupSetName2(sourceLookupSetName2);
				mappingSetsResPo.setSourceLookupSetName3(sourceLookupSetName3);
				mappingSetsResPo.setWhereClause(x.getWhereClause());

				mappingSetResLi.add(mappingSetsResPo);

			});
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return mappingSetResLi;
	}

	@Override
	public List<XxrMappingValue> getCloudMappingSetValue(Long mappingSetId) throws Exception {
		log.info("Start of getCloudMappingSetValue######");
		List<XxrMappingValue> mappingValuesLi = new ArrayList<>();
		try {
			mappingValuesLi = xxrMappingValueRepository.findByMapSetId(mappingSetId);
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return mappingValuesLi;
	}

	@Override
	public SaveCloudMappingSetHeaderResPo saveCloudMappingSetHdr(SaveCloudMappingSetHdrPo mappingHeader,
			HttpServletRequest request) throws ValidationException, Exception {
		log.info("Start of saveCloudMappingSetHdr ###########");
		SaveCloudMappingSetHeaderResPo saveCloudMappingSetHeaderResPo = new SaveCloudMappingSetHeaderResPo();
		XxrMappingSet xxrMappingSet = new XxrMappingSet();
		try {
			if (mappingHeader.getMapSetId() == null || mappingHeader.getMapSetId() == 0) {
				Long mappingSetId = xxrMappingSetRepository.getMapId(mappingHeader.getMapSetName());
				if (mappingSetId != null)
					throw new ValidationException("This MappingSetName already Exists");
			} else {
				xxrMappingSet = xxrMappingSetRepository.findByMapSetId(mappingHeader.getMapSetId());
				if (xxrMappingSet == null)
					throw new ValidationException("No MappingSet Exists with provided mappingsetId");
				// xxrMappingSet.setMapSetId(mappingHeader.getMapSetId());
			}
			xxrMappingSet.setMapSetName(mappingHeader.getMapSetName());
			xxrMappingSet.setMapSetType(mappingHeader.getMapSetType());
			xxrMappingSet.setPodId(mappingHeader.getPodId());
			xxrMappingSet.setProjectId(mappingHeader.getProjectId());
			xxrMappingSet.setParentObjectId(mappingHeader.getParentObjectId());
			xxrMappingSet.setObjectId(mappingHeader.getObjectId());
			xxrMappingSet.setSourceLookupSetId1(mappingHeader.getSourceLookupSetId1());
			xxrMappingSet.setSourceLookupSetId2(mappingHeader.getSourceLookupSetId2());
			xxrMappingSet.setSourceLookupSetId3(mappingHeader.getSourceLookupSetId3());
			xxrMappingSet.setCloudLookupSetId(mappingHeader.getCloudLookupSetId());
			xxrMappingSet.setWhereClause(mappingHeader.getWhereClause());
			xxrMappingSet.setAttribute1(mappingHeader.getAttribute1());
			xxrMappingSet.setAttribute2(mappingHeader.getAttribute2());
			xxrMappingSet.setAttribute3(mappingHeader.getAttribute3());
			xxrMappingSet.setAttribute4(mappingHeader.getAttribute4());
			xxrMappingSet.setAttribute5(mappingHeader.getAttribute5());
			xxrMappingSet.setCreationDate(new java.sql.Date(new java.util.Date().getTime()));
			xxrMappingSet.setCreatedBy("ConvertRite");
			xxrMappingSet.setLastUpdatedDate(new java.sql.Date(new java.util.Date().getTime()));
			xxrMappingSet.setLastUpdateBy("ConvertRite");

			XxrMappingSet xxrMappingSetRes = xxrMappingSetRepository.save(xxrMappingSet);

			saveCloudMappingSetHeaderResPo.setMapSetId(xxrMappingSetRes.getMapSetId());
			saveCloudMappingSetHeaderResPo.setMapSetName(xxrMappingSetRes.getMapSetName());
			saveCloudMappingSetHeaderResPo.setMessage("Successfully Saved MappingSet");
		} catch (ValidationException e) {
			throw new ValidationException(e.getMessage());
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return saveCloudMappingSetHeaderResPo;
	}

	@Override
	public SaveCloudMappingSetValuesResPo saveCloudMappingSetValues(
			List<SaveCloudMappingSetColumnsPo> cloudMappingSetColumnsPo, HttpServletRequest request)
			throws ValidationException, Exception {
		log.info("Start of saveCloudMappingSetValues########");
		List<XxrMappingValue> mappingValuesLi = new ArrayList<>();
		SaveCloudMappingSetValuesResPo saveCloudMappingSetColumnsResPo = new SaveCloudMappingSetValuesResPo();
		try {
			for (SaveCloudMappingSetColumnsPo x : cloudMappingSetColumnsPo) {
				XxrMappingValue mappingValue = new XxrMappingValue();

				if (x.getMapLineId() != null && x.getMapLineId() != 0) {
					mappingValue = xxrMappingValueRepository.findByMapLineId(x.getMapLineId());
					if (mappingValue == null)
						throw new ValidationException("No MappingSet Exists with provided mappingsetId");
					// mappingValue.setMapLineId(x.getMapLineId());
				}
				mappingValue.setMapSetId(x.getMapSetId());
				mappingValue.setCloudValue(x.getCloudValue());
				mappingValue.setEnabled(x.getEnabled());
				mappingValue.setSourceField1(x.getSourceField1());
				mappingValue.setSourceField2(x.getSourceField2());
				mappingValue.setSourceField3(x.getSourceField3());
				mappingValue.setAttribute1(x.getAttribute1());
				mappingValue.setAttribute2(x.getAttribute2());
				mappingValue.setAttribute3(x.getAttribute3());
				mappingValue.setAttribute4(x.getAttribute4());
				mappingValue.setAttribute5(x.getAttribute5());
				mappingValue.setCreationDate(new java.sql.Date(new java.util.Date().getTime()));
				mappingValue.setCreatedBy("ConvertRite");
				mappingValue.setLastUpdatedDate(new java.sql.Date(new java.util.Date().getTime()));
				mappingValue.setLastUpdateBy("ConvertRite");
				mappingValuesLi.add(mappingValue);
			}
			List<XxrMappingValue> mappingValuesLiRes = xxrMappingValueRepository.saveAll(mappingValuesLi);

			saveCloudMappingSetColumnsResPo.setCloudMappingSetColumns(mappingValuesLiRes);
			saveCloudMappingSetColumnsResPo.setMessage("Successfully Saved Mapping Values");

		} catch (ValidationException e) {
			throw new ValidationException(e.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e.getMessage());
		}
		return saveCloudMappingSetColumnsResPo;
	}

	@Override
	public SaveCloudMappingSetHeaderResPo copyMappingSet(String newMapSetName, Long mapSetId, Long podId,
			HttpServletRequest request) throws Exception {
		log.info("Start of copyMappingSet Method#####");
		XxrMappingSet x = new XxrMappingSet();
		SaveCloudMappingSetHdrPo saveCloudMappingSetHeadersPo = new SaveCloudMappingSetHdrPo();
		List<XxrMappingValue> xxrMappingValuesList = new ArrayList<>();
		List<SaveCloudMappingSetColumnsPo> cloudMappingSetColumnsPo = new ArrayList<>();
		SaveCloudMappingSetHeaderResPo saveCloudMappingSetHeaderResPo = new SaveCloudMappingSetHeaderResPo();
		try {
			x = xxrMappingSetRepository.findByMapSetId(mapSetId);
			if (x == null)
				throw new Exception("No MappingSet Exists with provided mapSetId");

			saveCloudMappingSetHeadersPo.setMapSetName(newMapSetName);
			saveCloudMappingSetHeadersPo.setPodId(podId);
			saveCloudMappingSetHeadersPo.setMapSetType(x.getMapSetType());
			saveCloudMappingSetHeadersPo.setObjectId(x.getObjectId());
			saveCloudMappingSetHeadersPo.setParentObjectId(x.getParentObjectId());
			saveCloudMappingSetHeadersPo.setProjectId(x.getProjectId());
			saveCloudMappingSetHeadersPo.setSourceLookupSetId1(x.getSourceLookupSetId1());
			saveCloudMappingSetHeadersPo.setSourceLookupSetId2(x.getSourceLookupSetId2());
			saveCloudMappingSetHeadersPo.setSourceLookupSetId3(x.getSourceLookupSetId3());
			saveCloudMappingSetHeadersPo.setAttribute1(x.getAttribute1());
			saveCloudMappingSetHeadersPo.setAttribute2(x.getAttribute2());
			saveCloudMappingSetHeadersPo.setAttribute3(x.getAttribute3());
			saveCloudMappingSetHeadersPo.setAttribute4(x.getAttribute4());
			saveCloudMappingSetHeadersPo.setAttribute5(x.getAttribute5());
			saveCloudMappingSetHeadersPo.setWhereClause(x.getWhereClause());
			saveCloudMappingSetHeadersPo.setCloudLookupSetId(x.getCloudLookupSetId());

			SaveCloudMappingSetHeaderResPo saveCloudMappingSetHeaderRes = saveCloudMappingSetHdr(
					saveCloudMappingSetHeadersPo, request);
			log.info("MappingsetHeader :: " + saveCloudMappingSetHeaderRes.getMessage());

			xxrMappingValuesList = xxrMappingValueRepository.findByMapSetId(mapSetId);

			for (XxrMappingValue xxrMappingValue : xxrMappingValuesList) {
				SaveCloudMappingSetColumnsPo saveCloudMappingSetColumnsPo = new SaveCloudMappingSetColumnsPo();

				saveCloudMappingSetColumnsPo.setMapSetId(saveCloudMappingSetHeaderRes.getMapSetId());
				saveCloudMappingSetColumnsPo.setCloudValue(xxrMappingValue.getCloudValue());
				saveCloudMappingSetColumnsPo.setEnabled(xxrMappingValue.getEnabled());
				saveCloudMappingSetColumnsPo.setSourceField1(xxrMappingValue.getSourceField1());
				saveCloudMappingSetColumnsPo.setSourceField2(xxrMappingValue.getSourceField2());
				saveCloudMappingSetColumnsPo.setSourceField3(xxrMappingValue.getSourceField3());
				saveCloudMappingSetColumnsPo.setAttribute1(xxrMappingValue.getAttribute1());
				saveCloudMappingSetColumnsPo.setAttribute2(xxrMappingValue.getAttribute2());
				saveCloudMappingSetColumnsPo.setAttribute3(xxrMappingValue.getAttribute3());
				saveCloudMappingSetColumnsPo.setAttribute4(xxrMappingValue.getAttribute4());
				saveCloudMappingSetColumnsPo.setAttribute5(xxrMappingValue.getAttribute5());

				cloudMappingSetColumnsPo.add(saveCloudMappingSetColumnsPo);
			}

			SaveCloudMappingSetValuesResPo saveCloudMappingSetValuesResPo = saveCloudMappingSetValues(
					cloudMappingSetColumnsPo, request);
			log.info("MappingsetColumn :: " + saveCloudMappingSetValuesResPo.getMessage());

			saveCloudMappingSetHeaderResPo.setMapSetId(saveCloudMappingSetHeaderRes.getMapSetId());
			saveCloudMappingSetHeaderResPo.setMapSetName(newMapSetName);
			saveCloudMappingSetHeaderResPo.setMessage("MappingSet is Successfully copied");
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return saveCloudMappingSetHeaderResPo;
	}

	@Override
	public List<XxrMappingSetsView> getAllCloudMappingSets(Long roleId) throws Exception {
		log.info("Start of getAllCloudMappingSets in service ##");
		List<XxrMappingSetsView> mappingSetViewLi = new ArrayList<>();
		mappingSetViewLi = xxrMappingSetsViewRepository.findByRoleId(roleId);
		return mappingSetViewLi;
	}
	
	@Override
	public SaveCloudMappingSetColumnsResPo saveCloudMappingSetValuesWithJpa(
			List<SaveCloudMappingSetColumnsPo> saveCloudMappingSetColumnsPos)
			throws BadRequestException, ValidationException {
		log.info("Start of saveCloudMappingSetValuesWithJpa ######");
		SaveCloudMappingSetColumnsResPo saveCloudMappingSetColumnsResPo = new SaveCloudMappingSetColumnsResPo();
		List<XxrMappingValues> xxrMappingValuesList = new ArrayList<>();
		for (SaveCloudMappingSetColumnsPo saveCloudMappingSetColumnsPo : saveCloudMappingSetColumnsPos) {
			if (saveCloudMappingSetColumnsPo.getMapSetId() == null)
				throw new BadRequestException("MapSetId is Mandatory field");
			if (saveCloudMappingSetColumnsPo.getMapLineId() == null) {
				XxrMappingValues xxrMappingValues = new XxrMappingValues();
				xxrMappingValues.setMapSetId(saveCloudMappingSetColumnsPo.getMapSetId());
				xxrMappingValues.setSourceField1(saveCloudMappingSetColumnsPo.getSourceField1());
				xxrMappingValues.setSourceField2(saveCloudMappingSetColumnsPo.getSourceField2());
				xxrMappingValues.setSourceField3(saveCloudMappingSetColumnsPo.getSourceField3());
				xxrMappingValues.setCloudValue(saveCloudMappingSetColumnsPo.getCloudValue());
				xxrMappingValues.setEnabled(saveCloudMappingSetColumnsPo.getEnabled());
				xxrMappingValues.setAttribute1(saveCloudMappingSetColumnsPo.getAttribute1());
				xxrMappingValues.setAttribute2(saveCloudMappingSetColumnsPo.getAttribute2());
				xxrMappingValues.setAttribute3(saveCloudMappingSetColumnsPo.getAttribute3());
				xxrMappingValues.setAttribute4(saveCloudMappingSetColumnsPo.getAttribute4());
				xxrMappingValues.setAttribute5(saveCloudMappingSetColumnsPo.getAttribute5());
				xxrMappingValues.setLastUpdatedBy("ConvertRite");
				xxrMappingValues.setLastUpdateDate(new java.sql.Date(new java.util.Date().getTime()));
				xxrMappingValues.setCreationDate(new java.sql.Date(new java.util.Date().getTime()));
				xxrMappingValues.setCreatedBy("ConvertRite");
				xxrMappingValuesList.add(xxrMappingValues);
			} else {
				Optional<XxrMappingValues> srcTempColsOpt = xxrColumnMapLineRepository.findById(saveCloudMappingSetColumnsPo.getMapLineId());
				if (!srcTempColsOpt.isPresent())
					throw new ValidationException("There is no Column with provided ColumnId");
				XxrMappingValues xxrMappingValues = srcTempColsOpt.get();
				xxrMappingValues.setMapSetId(saveCloudMappingSetColumnsPo.getMapSetId());
				xxrMappingValues.setSourceField1(saveCloudMappingSetColumnsPo.getSourceField1());
				xxrMappingValues.setSourceField2(saveCloudMappingSetColumnsPo.getSourceField2());
				xxrMappingValues.setSourceField3(saveCloudMappingSetColumnsPo.getSourceField3());
				xxrMappingValues.setCloudValue(saveCloudMappingSetColumnsPo.getCloudValue());
				xxrMappingValues.setEnabled(saveCloudMappingSetColumnsPo.getEnabled());
				xxrMappingValues.setAttribute1(saveCloudMappingSetColumnsPo.getAttribute1());
				xxrMappingValues.setAttribute2(saveCloudMappingSetColumnsPo.getAttribute2());
				xxrMappingValues.setAttribute3(saveCloudMappingSetColumnsPo.getAttribute3());
				xxrMappingValues.setAttribute4(saveCloudMappingSetColumnsPo.getAttribute4());
				xxrMappingValues.setAttribute5(saveCloudMappingSetColumnsPo.getAttribute5());
				xxrMappingValues.setCreationDate(new java.sql.Date(new java.util.Date().getTime()));
				xxrMappingValues.setLastUpdatedBy("ConvertRite");
				xxrMappingValuesList.add(xxrMappingValues);
			}
			List<XxrMappingValues> xxrMappingValues = xxrColumnMapLineRepository
					.saveAll(xxrMappingValuesList);
			saveCloudMappingSetColumnsResPo.setCloudMappingSetColumns(xxrMappingValues);
			saveCloudMappingSetColumnsResPo.setMessage("Successfully Saved/Updated CloudMappingSetValues");
		}
		return saveCloudMappingSetColumnsResPo;
	}

	@Override
	public SaveCloudMappingSetHeaderJpaRes saveCloudMappingSetHdrWithJpa(List<SaveCloudMappingSetHeadersPo> saveCloudMappingSetHeadersPos) throws BadRequestException, ValidationException {
		log.info("Start of saveCloudMappingSetValuesWithJpa ######");
		SaveCloudMappingSetHeaderJpaRes saveCloudMappingSetHeaderJpaRes = new SaveCloudMappingSetHeaderJpaRes();
		List<XxrMappingSets> xxrMappingSetsList = new ArrayList<>();
		for (SaveCloudMappingSetHeadersPo saveCloudMappingSetHeadersPo : saveCloudMappingSetHeadersPos) {
			if (saveCloudMappingSetHeadersPo.getMapSetId() == null) {
				XxrMappingSets xxrMappingSets = new XxrMappingSets();
				xxrMappingSets.setMapSetName(saveCloudMappingSetHeadersPo.getMapSetName());
				xxrMappingSets.setMapSetType(saveCloudMappingSetHeadersPo.getMapSetType());
				xxrMappingSets.setPodId(saveCloudMappingSetHeadersPo.getPodId());
				xxrMappingSets.setProjectId(saveCloudMappingSetHeadersPo.getProjectId());
				xxrMappingSets.setParentObjectId(saveCloudMappingSetHeadersPo.getParentObjectId());
				xxrMappingSets.setObjectId(saveCloudMappingSetHeadersPo.getObjectId());
				xxrMappingSets.setCloudColumn(saveCloudMappingSetHeadersPo.getCloudColumn());
				xxrMappingSets.setSourceObject1(saveCloudMappingSetHeadersPo.getSourceObject1());
				xxrMappingSets.setSourceObject2(saveCloudMappingSetHeadersPo.getSourceObject2());
				xxrMappingSets.setSourceObject3(saveCloudMappingSetHeadersPo.getSourceObject3());
				xxrMappingSets.setSourceColumn1(saveCloudMappingSetHeadersPo.getSourceColumn1());
				xxrMappingSets.setSourceColumn2(saveCloudMappingSetHeadersPo.getSourceColumn2());
				xxrMappingSets.setSourceColumn3(saveCloudMappingSetHeadersPo.getSourceColumn3());
				xxrMappingSets.setAttribute1(saveCloudMappingSetHeadersPo.getAttribute1());
				xxrMappingSets.setAttribute2(saveCloudMappingSetHeadersPo.getAttribute2());
				xxrMappingSets.setAttribute3(saveCloudMappingSetHeadersPo.getAttribute3());
				xxrMappingSets.setAttribute4(saveCloudMappingSetHeadersPo.getAttribute4());
				xxrMappingSets.setAttribute5(saveCloudMappingSetHeadersPo.getAttribute5());
				xxrMappingSets.setWhereClause(saveCloudMappingSetHeadersPo.getWhereClause());
				xxrMappingSets.setLookupSetName(saveCloudMappingSetHeadersPo.getLookupSetName());
				xxrMappingSets.setLastUpdateBy("ConvertRite");
				xxrMappingSets.setLastUpdatedDate(new java.sql.Date(new java.util.Date().getTime()));
				xxrMappingSets.setCreationDate(new java.sql.Date(new java.util.Date().getTime()));
				xxrMappingSets.setCreatedBy("ConvertRite");
				xxrMappingSetsList.add(xxrMappingSets);
			} else {
				Optional<XxrMappingSets> srcTempHdrOpt = xxrColumnMapHdrRepository.findById(saveCloudMappingSetHeadersPo.getMapSetId());
				if (!srcTempHdrOpt.isPresent())
					throw new ValidationException("There is no Hdr with provided ColumnId");
				XxrMappingSets xxrMappingSets = srcTempHdrOpt.get();
				xxrMappingSets.setMapSetName(saveCloudMappingSetHeadersPo.getMapSetName());
				xxrMappingSets.setMapSetType(saveCloudMappingSetHeadersPo.getMapSetType());
				xxrMappingSets.setPodId(saveCloudMappingSetHeadersPo.getPodId());
				xxrMappingSets.setProjectId(saveCloudMappingSetHeadersPo.getProjectId());
				xxrMappingSets.setParentObjectId(saveCloudMappingSetHeadersPo.getParentObjectId());
				xxrMappingSets.setObjectId(saveCloudMappingSetHeadersPo.getObjectId());
				xxrMappingSets.setCloudColumn(saveCloudMappingSetHeadersPo.getCloudColumn());
				xxrMappingSets.setSourceObject1(saveCloudMappingSetHeadersPo.getSourceObject1());
				xxrMappingSets.setSourceObject2(saveCloudMappingSetHeadersPo.getSourceObject2());
				xxrMappingSets.setSourceObject3(saveCloudMappingSetHeadersPo.getSourceObject3());
				xxrMappingSets.setSourceColumn1(saveCloudMappingSetHeadersPo.getSourceColumn1());
				xxrMappingSets.setSourceColumn2(saveCloudMappingSetHeadersPo.getSourceColumn2());
				xxrMappingSets.setSourceColumn3(saveCloudMappingSetHeadersPo.getSourceColumn3());
				xxrMappingSets.setAttribute1(saveCloudMappingSetHeadersPo.getAttribute1());
				xxrMappingSets.setAttribute2(saveCloudMappingSetHeadersPo.getAttribute2());
				xxrMappingSets.setAttribute3(saveCloudMappingSetHeadersPo.getAttribute3());
				xxrMappingSets.setAttribute4(saveCloudMappingSetHeadersPo.getAttribute4());
				xxrMappingSets.setAttribute5(saveCloudMappingSetHeadersPo.getAttribute5());
				xxrMappingSets.setWhereClause(saveCloudMappingSetHeadersPo.getWhereClause());
				xxrMappingSets.setLookupSetName(saveCloudMappingSetHeadersPo.getLookupSetName());
				xxrMappingSets.setCreationDate(new java.sql.Date(new java.util.Date().getTime()));
				xxrMappingSets.setCreatedBy("ConvertRite");
				xxrMappingSetsList.add(xxrMappingSets);
			}
			List<XxrMappingSets> xxrMappingSets = xxrColumnMapHdrRepository
					.saveAll(xxrMappingSetsList);
			saveCloudMappingSetHeaderJpaRes.setXxrMappingSetsList(xxrMappingSets);
			saveCloudMappingSetHeaderJpaRes.setMessage("Successfully Saved/Updated CloudMappingSetHdrs");
		}
		return saveCloudMappingSetHeaderJpaRes;
	}
}
