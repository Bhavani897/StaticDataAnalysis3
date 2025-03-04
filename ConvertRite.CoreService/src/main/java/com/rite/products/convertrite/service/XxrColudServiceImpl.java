package com.rite.products.convertrite.service;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import javax.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.rite.products.convertrite.Validations.Validations;
import com.rite.products.convertrite.exception.BadRequestException;
import com.rite.products.convertrite.exception.ValidationException;
import com.rite.products.convertrite.model.XxrCldTempHdrsView;
import com.rite.products.convertrite.model.XxrCloudTable;
import com.rite.products.convertrite.model.XxrCloudTemplateColumns;
import com.rite.products.convertrite.model.XxrCloudTemplateHeader;
import com.rite.products.convertrite.po.CldSrcTemplateIdsResPo;
import com.rite.products.convertrite.po.CloudMappingSetPo;
import com.rite.products.convertrite.po.CloudSourceColumnsPo;
import com.rite.products.convertrite.po.CloudStagingTablePo;
import com.rite.products.convertrite.po.CloudTablesPo;
import com.rite.products.convertrite.po.CloudTablesTemplatesPo;
import com.rite.products.convertrite.po.CloudTemplatePo;
import com.rite.products.convertrite.po.ColumnPo;
import com.rite.products.convertrite.po.CopyCloudReqPo;
import com.rite.products.convertrite.po.CreateDynamicViewPo;
import com.rite.products.convertrite.po.LovPo;
import com.rite.products.convertrite.po.LovValuesPo;
import com.rite.products.convertrite.po.PodsPo;
import com.rite.products.convertrite.po.SaveCloudTemplateColumnsPo;
import com.rite.products.convertrite.po.SaveCloudTemplateColumnsResPo;
import com.rite.products.convertrite.po.SaveCloudTemplateHeadersPo;
import com.rite.products.convertrite.po.SaveTemplateHeaderResponsePo;
import com.rite.products.convertrite.po.SourceColumnsPo;
import com.rite.products.convertrite.po.TemplatesPo;
import com.rite.products.convertrite.po.XxrCloudTemplateColumnsResPo;
import com.rite.products.convertrite.po.XxrCloudTemplatePo;
import com.rite.products.convertrite.respository.CloudMetaDataRepository;
import com.rite.products.convertrite.respository.CloudTemplateHeaderDaoImpl;
import com.rite.products.convertrite.respository.CreateCloudStagTableDaoImpl;
import com.rite.products.convertrite.respository.CreateDynamicViewDaoImpl;
import com.rite.products.convertrite.respository.SaveCloudTemplateColumnsDaoImpl;
import com.rite.products.convertrite.respository.SaveCloudTemplateHeadersDaoImpl;
import com.rite.products.convertrite.respository.SourceTemplateColumnsRepository;
import com.rite.products.convertrite.respository.SourceTemplateHeadersRepository;
import com.rite.products.convertrite.respository.XxrCldTempHdrsViewRepository;
import com.rite.products.convertrite.respository.XxrCloudColumnsRepository;
import com.rite.products.convertrite.respository.XxrCloudMasterLookUpSetRepository;
import com.rite.products.convertrite.respository.XxrCloudTableRepository;
import com.rite.products.convertrite.respository.XxrCloudTemplateColumnsRepository;
import com.rite.products.convertrite.respository.XxrCloudTemplateHeadersRepository;
import com.rite.products.convertrite.respository.XxrFbdiTempColsRepository;
import com.rite.products.convertrite.respository.XxrFormulaSetsRepository;
import com.rite.products.convertrite.respository.XxrLookUpValuesRepository;
import com.rite.products.convertrite.respository.XxrMappingSetsRepository;
import com.rite.products.convertrite.respository.XxrXlsmTempColsRepository;
import com.rite.products.convertrite.utils.DataSourceUtil;

@Service
public class XxrColudServiceImpl implements XxrCloudService {

	private static final Logger log = LoggerFactory.getLogger(XxrColudServiceImpl.class);

	@Autowired
	XxrCloudTableRepository xxrCloudTableRepository;
	@Autowired
	SourceTemplateHeadersRepository sourceTemplateHeadersRepository;
	@Autowired
	XxrCloudColumnsRepository xxrCloudColumnsRepository;
	@Autowired
	SourceTemplateColumnsRepository sourceTemplateColumnsRepository;
	@Autowired
	CloudMetaDataRepository cloudMetaDataRepository;
	@Autowired
	CloudTemplateHeaderDaoImpl cloudTemplateHeaderDaoImpl;
	@Autowired
	XxrCloudTemplateColumnsRepository xxrCloudTemplateColumnsRepository;
	@Autowired
	XxrCloudMasterLookUpSetRepository xxrCloudMasterLookUpSetRepository;
	@Autowired
	CreateCloudStagTableDaoImpl createCloudStagTableDaoImpl;
	@Autowired
	CreateDynamicViewDaoImpl createDynamicViewDaoImpl;
	@Autowired
	SaveCloudTemplateHeadersDaoImpl cloudTemplateHeadersSaveDaoImpl;
	@Autowired
	SaveCloudTemplateColumnsDaoImpl cloudTemplateColumnsDaoImpl;
	@Autowired
	XxrCloudTemplateHeadersRepository xxrCloudTemplateHeadersRepository;
	@Autowired
	XxrMappingSetsRepository xxrColumnMapHdrRepository;
	@Autowired
	XxrLookUpValuesRepository xxrLookUpValuesRepository;
	@Autowired
	XxrFbdiTempColsRepository xxrFbdiTempColsRepository;
	@Autowired
	XxrXlsmTempColsRepository xxrXlsmTempColsRepository;
	@Autowired
	XxrFormulaSetsRepository xxrFormulaSetsRepository;
	@Autowired
	DataSourceUtil dataSourceUtil;
	@Autowired
	XxrCldTempHdrsViewRepository xxrCldTempHdrsViewRepository;

	public XxrCloudTemplatePo getAllCloudData() throws Exception {
		log.info("Start of getAllCloudData in Service Layer ###");
		List<XxrCloudTable> cloudDataList = new ArrayList<>();
		// String[] templateHeaders;
		XxrCloudTemplatePo cloudTemplatePo = new XxrCloudTemplatePo();
		try {
			cloudDataList = xxrCloudTableRepository.findAll();
			// templateHeaders = xxrCloudTableRepository.getSourceTemplateHeaders();
			/*
			 * String[] objectNames = cloudMetaDataRepository.getValues("OBJECT_NAME");
			 * String[] pod = cloudMetaDataRepository.getValues("POD"); String[] bu =
			 * cloudMetaDataRepository.getValues("BU"); templateHeaders =
			 * sourceTemplateHeadersRepository.getTemplateHeaders(objectNames);
			 */

			cloudTemplatePo.setCloudTableMetaData(cloudDataList);
			/*
			 * cloudTemplatePo.setBu(bu); cloudTemplatePo.setPod(pod);
			 * cloudTemplatePo.setObjectCodes(objectNames);
			 * cloudTemplatePo.setParentObjectCode(objectNames);
			 * cloudTemplatePo.setTemplateHeaders(templateHeaders);
			 */

		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return cloudTemplatePo;
	}

	public CloudSourceColumnsPo getCloudSourceColumns(String sourceTemplateName, String cloudTableName)
			throws Exception {
		log.info("Start Of getCloudSourceColumns Method in Service Layer ####");
		CloudSourceColumnsPo cloudSourceColumnsPo = new CloudSourceColumnsPo();
		Long templateId = null;
		List<SourceColumnsPo> sourceColumns = new ArrayList<>();
		List<ColumnPo> cloudColumns = new ArrayList<>();
		try {
			Long tableId = xxrCloudTableRepository.getTableId(cloudTableName);
			if (!Validations.isNullOrEmpty(sourceTemplateName))
				templateId = sourceTemplateHeadersRepository.getTemplateId(sourceTemplateName);
			log.info("tableId:::::: " + tableId + " templateId:::: " + templateId);
			if (tableId != null)
				cloudColumns = xxrCloudColumnsRepository.getCloudColumnsById(tableId);
			if (templateId != null)
				sourceColumns = sourceTemplateColumnsRepository.getColumnNames(templateId);
			cloudSourceColumnsPo.setCloudColumns(cloudColumns);
			cloudSourceColumnsPo.setSourceColumns(sourceColumns);

		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return cloudSourceColumnsPo;
	}

	public List<XxrCloudTemplateHeader> getCloudTemplate(CloudTemplatePo cloudTemplatePo) throws Exception {
		log.info("Start Of getCloudTemplate Method in Service Layer ####");
		List<XxrCloudTemplateHeader> list = new ArrayList<>();
		try {
			list = cloudTemplateHeaderDaoImpl.getCloudTemplate(cloudTemplatePo);
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return list;
	}

	public List<XxrCldTempHdrsView> getAllCloudTemplates(Long roleId) throws Exception {
		log.info("Start Of getAllCloudTemplates Method in Service Layer ####");
		List<XxrCldTempHdrsView> list = new ArrayList<>();
		list = xxrCldTempHdrsViewRepository.findByRoleId(roleId);
		return list;
	}

	/*
	 * public List<XxrCloudTemplateHeaderResPo> getAllCloudTemplates() throws
	 * Exception {
	 * log.info("Start Of getAllCloudTemplates Method in Service Layer ####");
	 * List<XxrCloudTemplateHeader> list = new ArrayList<>();
	 * List<XxrCloudTemplateHeaderResPo> CloudTemplateHeaderRes = new ArrayList<>();
	 * try {
	 * 
	 * list = xxrCloudTemplateHeadersRepository.findAll();
	 * 
	 * list.stream().forEach(x -> { XxrCloudTemplateHeaderResPo
	 * xxrCloudTemplateHeaderResPo = new XxrCloudTemplateHeaderResPo(); String
	 * sourceTemplateName = ""; String metaDataTableName = ""; //
	 * log.info("podId:::"+x.getPodId()+"projectId::::::"+x.getProjectId()+
	 * "objectId:::::"+x.getObjectId()+"parentObjectId::::::"+x.getParentObjectId())
	 * ; String podName = xxrLookUpValuesRepository.getValueById(x.getPodId());
	 * String projectName =
	 * xxrLookUpValuesRepository.getValueById(x.getProjectId()); String objectCode =
	 * xxrLookUpValuesRepository.getValueById(x.getObjectId()); String
	 * parentObjectCode =
	 * xxrLookUpValuesRepository.getValueById(x.getParentObjectId()); if
	 * (x.getMetaDataTableId() != null) { metaDataTableName =
	 * xxrCloudTableRepository.getMetaDataTableName(x.getMetaDataTableId());
	 * xxrCloudTemplateHeaderResPo.setMetaDataTableId(x.getMetaDataTableId()); } if
	 * (x.getSourceTemplateId() != null) { sourceTemplateName =
	 * sourceTemplateHeadersRepository.getTemplateName(x.getSourceTemplateId());
	 * xxrCloudTemplateHeaderResPo.setSourceTemplateId(x.getSourceTemplateId()); }
	 * xxrCloudTemplateHeaderResPo.setTemplateId(x.getTemplateId());
	 * xxrCloudTemplateHeaderResPo.setTemplateName(x.getTemplateName());
	 * xxrCloudTemplateHeaderResPo.setPodId(x.getPodId());
	 * xxrCloudTemplateHeaderResPo.setPodName(podName);
	 * xxrCloudTemplateHeaderResPo.setProjectId(x.getProjectId());
	 * xxrCloudTemplateHeaderResPo.setProjectName(projectName);
	 * xxrCloudTemplateHeaderResPo.setObjectId(x.getObjectId());
	 * xxrCloudTemplateHeaderResPo.setObjectCode(objectCode);
	 * xxrCloudTemplateHeaderResPo.setParentObjectId(x.getParentObjectId());
	 * xxrCloudTemplateHeaderResPo.setParentObjectCode(parentObjectCode); if
	 * (!Validations.isNullOrEmpty(sourceTemplateName))
	 * xxrCloudTemplateHeaderResPo.setSourceTemplateName(sourceTemplateName); if
	 * (!Validations.isNullOrEmpty(metaDataTableName))
	 * xxrCloudTemplateHeaderResPo.setMetaDataTableName(metaDataTableName); if
	 * (!Validations.isNullOrEmpty(x.getStagingTableName()))
	 * xxrCloudTemplateHeaderResPo.setStagingTableName(x.getStagingTableName()); if
	 * (!Validations.isNullOrEmpty(x.getViewName()))
	 * xxrCloudTemplateHeaderResPo.setViewName(x.getViewName()); if
	 * (!Validations.isNullOrEmpty(x.getBu()))
	 * xxrCloudTemplateHeaderResPo.setBu(x.getBu()); if
	 * (!Validations.isNullOrEmpty(x.getBuSpecific()))
	 * xxrCloudTemplateHeaderResPo.setBuSpecific(x.getBuSpecific()); if
	 * (!Validations.isNullOrEmpty(x.getVersion()))
	 * xxrCloudTemplateHeaderResPo.setVersion(x.getVersion());
	 * CloudTemplateHeaderRes.add(xxrCloudTemplateHeaderResPo); });
	 * 
	 * } catch (Exception e) { throw new Exception(e.getMessage()); } return
	 * CloudTemplateHeaderRes; }
	 */

	public List<XxrCloudTemplateColumnsResPo> getCloudTemplateColumns(long templateId) throws Exception {
		log.info("Start Of getCloudTemplateColumns Method in Service Layer ####");
		List<XxrCloudTemplateColumns> cloudTemplateColumnsList = new ArrayList<>();
		List<XxrCloudTemplateColumnsResPo> cloudTemplateColumnsRes = new ArrayList<>();
		try {
			cloudTemplateColumnsList = xxrCloudTemplateColumnsRepository.findByTemplateId(templateId);
			cloudTemplateColumnsList.stream().forEach(x -> {
				XxrCloudTemplateColumnsResPo xxrCloudTemplateColumnsResPo = new XxrCloudTemplateColumnsResPo();
				String mappingSetName = "";
				String sourceColumnName = "";

				if (x.getMappingSetId() != null) {
					if ("Formula".equalsIgnoreCase(x.getMappingType())) {
						mappingSetName = xxrFormulaSetsRepository.getFormulaSetNameNameById(x.getMappingSetId());
					} else {
						mappingSetName = xxrColumnMapHdrRepository.getMappingSetNameById(x.getMappingSetId());
					}
					xxrCloudTemplateColumnsResPo.setMappingSetId(x.getMappingSetId());
				}
				if (x.getSourceColumnId() != null) {
					sourceColumnName = sourceTemplateColumnsRepository.getSourceColumnName(x.getSourceColumnId());
					xxrCloudTemplateColumnsResPo.setSourceColumnId(x.getSourceColumnId());
				}

				xxrCloudTemplateColumnsResPo.setColumnId(x.getColumnId());
				xxrCloudTemplateColumnsResPo.setColumnName(x.getColumnName());
				xxrCloudTemplateColumnsResPo.setTemplateId(x.getTemplateId());
				if (!Validations.isNullOrEmpty(x.getSelected()))
					xxrCloudTemplateColumnsResPo.setSelected(x.getSelected());
				if (!Validations.isNullOrEmpty(x.getMappingType()))
					xxrCloudTemplateColumnsResPo.setMappingType(x.getMappingType());
				if (!Validations.isNullOrEmpty(x.getMappingValue1()))
					xxrCloudTemplateColumnsResPo.setMappingValue1(x.getMappingValue1());
				if (!Validations.isNullOrEmpty(x.getMappingValue2()))
					xxrCloudTemplateColumnsResPo.setMappingValue2(x.getMappingValue2());
				if (!Validations.isNullOrEmpty(x.getMappingValue3()))
					xxrCloudTemplateColumnsResPo.setMappingValue3(x.getMappingValue3());
				if (!Validations.isNullOrEmpty(x.getMappingValue4()))
					xxrCloudTemplateColumnsResPo.setMappingValue4(x.getMappingValue4());
				if (!Validations.isNullOrEmpty(x.getMappingValue5()))
					xxrCloudTemplateColumnsResPo.setMappingValue5(x.getMappingValue5());
				if (!Validations.isNullOrEmpty(mappingSetName))
					xxrCloudTemplateColumnsResPo.setMappingSetName(mappingSetName);
				/*
				 * if (x.getStartDate() != null)
				 * xxrCloudTemplateColumnsResPo.setStartDate(x.getStartDate()); if
				 * (x.getEndDate() != null)
				 * xxrCloudTemplateColumnsResPo.setEndDate(x.getEndDate());
				 */
				if (!Validations.isNullOrEmpty(sourceColumnName))
					xxrCloudTemplateColumnsResPo.setSourceColumnName(sourceColumnName);
				/*
				 * if (!Validations.isNullOrEmpty(x.getEnabledFlag()))
				 * xxrCloudTemplateColumnsResPo.setEnabledFlag(x.getEnabledFlag());
				 */
				if (!Validations.isNullOrEmpty(x.getColumnType()))
					xxrCloudTemplateColumnsResPo.setColumnType(x.getColumnType());
				if (!Validations.isNullOrEmpty(x.getNullAllowedFlag()))
					xxrCloudTemplateColumnsResPo.setNullAllowedFlag(x.getNullAllowedFlag());
				if (!Validations.isNullOrEmpty(x.getDescription()))
					xxrCloudTemplateColumnsResPo.setDescription(x.getDescription());
				if (x.getWidth() != null)
					xxrCloudTemplateColumnsResPo.setWidth(x.getWidth());
				if (x.getSeq() != null)
					xxrCloudTemplateColumnsResPo.setSeq(x.getSeq());
				if (x.getOrigTransRef() != null)
					xxrCloudTemplateColumnsResPo.setOrigTransRef(x.getOrigTransRef());

				cloudTemplateColumnsRes.add(xxrCloudTemplateColumnsResPo);

			});

		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return cloudTemplateColumnsRes;
	}

	public LovPo getCloudLovValues(String[] lovValues, long podId, long projectId) throws Exception {
		log.info("Start Of getCloudLovValues Method in Service Layer ####");

		LovPo lovPo = new LovPo();
		try {
			for (int i = 0; i < lovValues.length; i++) {

				if (lovValues[i].equalsIgnoreCase("BU")) {
					List<LovValuesPo> buList = cloudMetaDataRepository.getValues("BU");
					// List<LovValuesPo> buList = cloudMetaDataRepository.getValues("BU", podId,
					// projectId);
					lovPo.setBu(buList);
				} else if (lovValues[i].equalsIgnoreCase("OBJECTCODE")) {

					List<LovValuesPo> objectCodeList = cloudMetaDataRepository.getValues("OBJECT_NAME");
					/*
					 * List<LovValuesPo> objectCodeList =
					 * cloudMetaDataRepository.getValues("OBJECT_NAME", podId, projectId);
					 */
					lovPo.setObjectCodes(objectCodeList);
				} else if (lovValues[i].equalsIgnoreCase("PARENTOBJECTCODE")) {
					List<LovValuesPo> parentObjectCode = cloudMetaDataRepository.getValues("PARENT_OBJECT_NAME");
					/*
					 * List<LovValuesPo> parentObjectCode =
					 * cloudMetaDataRepository.getValues("OBJECT_NAME", podId, projectId);
					 */
					lovPo.setParentObjectCode(parentObjectCode);
				}

			}
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return lovPo;
	}

	public LovPo getAllLovValues(String[] lovValues) throws Exception {
		log.info("Start Of getCloudLovValues Method in Service Layer ####");

		LovPo lovPo = new LovPo();
		try {
			for (int i = 0; i < lovValues.length; i++) {

				if (lovValues[i].equalsIgnoreCase("BU")) {
					List<LovValuesPo> buList = cloudMetaDataRepository.getValues("BU");
					lovPo.setBu(buList);
				} else if (lovValues[i].equalsIgnoreCase("OBJECTCODE")) {
					List<LovValuesPo> objectCodeList = cloudMetaDataRepository.getValues("OBJECT_NAME");
					lovPo.setObjectCodes(objectCodeList);
				} else if (lovValues[i].equalsIgnoreCase("PARENTOBJECTCODE")) {
					List<LovValuesPo> parentObjectCode = cloudMetaDataRepository.getValues("PARENT_OBJECT_NAME");
					lovPo.setParentObjectCode(parentObjectCode);
				}

			}
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return lovPo;
	}

	/*public CloudSourceColumnsPo getCloudSourceColumnsByIds(Long templateId, Long tableId) throws Exception {
		log.info("Start Of getCloudSourceColumnsByIds Method in Service Layer ####");
		CloudSourceColumnsPo cloudSourceColumnsPo = new CloudSourceColumnsPo();
		List<ColumnPo> cloudColumns = new ArrayList<>();
		List<SourceColumnsPo> sourceColumns = new ArrayList<>();
		try {
			log.info("tableId:::::: " + tableId + " templateId:::: " + templateId);
			if (tableId != null)
				cloudColumns = xxrCloudColumnsRepository.getCloudColumnsById(tableId);
			if (templateId != null)
				sourceColumns = sourceTemplateColumnsRepository.getColumnNames(templateId);
			cloudSourceColumnsPo.setCloudColumns(cloudColumns);
			cloudSourceColumnsPo.setSourceColumns(sourceColumns);

		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return cloudSourceColumnsPo;
	}*/

	public List<PodsPo> getPods() throws Exception {
		log.info("Start Of getPods Method in Service Layer ####");
		List<PodsPo> podList = new ArrayList<>();
		try {
			// podList = cloudMetaDataRepository.getPods("POD");
			// cloudMasterLookUpSetList = xxrCloudMasterLookUpSetRepository.getPods();

		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}

		return podList;
	}

	public List<LovValuesPo> getProjectsByPod(long podId) throws Exception {
		log.info("Start Of getProjectsByPod Method in Service Layer ####");
		List<LovValuesPo> projectList = new ArrayList<>();
		try {
			// cloudMasterLookUpSetList =
			// xxrCloudMasterLookUpSetRepository.getProjectsByLov(podId);

			projectList = cloudMetaDataRepository.getValues("PROJECT_NAME");

		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}

		return projectList;

	}

	public List<LovValuesPo> getProjects() throws Exception {

		log.info("Start Of getProjects Method in Service Layer ####");
		List<LovValuesPo> projectList = new ArrayList<>();
		try {
			projectList = cloudMetaDataRepository.getValues("PROJECT_NAME");

		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}

		return projectList;
	}

	public CloudStagingTablePo createCloudStaggingTab(String tableName, long templateId,
			HttpServletRequest request) throws Exception {
		log.info("Start Of createCloudStaggingTab Method in Service Layer ####");
		CloudStagingTablePo stagingPo = new CloudStagingTablePo();
		try {
			stagingPo = createCloudStagTableDaoImpl.createStaggingTable(tableName, templateId, request);
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e.getMessage());
		}

		return stagingPo;
	}

	public CreateDynamicViewPo createDynamicView(long templateId, String stgTableName, HttpServletRequest request)
			throws Exception {
		log.info("Start Of createDynamicView Method in Service Layer ####");

		CreateDynamicViewPo createDynamicViewPo = new CreateDynamicViewPo();
		try {
			createDynamicViewPo = createDynamicViewDaoImpl.createDynamicView(templateId, stgTableName, request);
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}

		return createDynamicViewPo;
	}

	/*
	 * public CloudTablesTemplatesPo getCloudTablesTemplates(long projectId, long
	 * podId, String objectCode, String parentObjectCode) throws Exception{
	 * CloudTablesTemplatesPo cloudTablesTemplatesPo = new CloudTablesTemplatesPo();
	 * 
	 * try { CloudTemplatePo cloudTemplatePo = new CloudTemplatePo();
	 * List<ProjectsPo> projectPos = getProjectsByLov(podId); ProjectsPo
	 * selectedProject = projectPos.stream().filter(x->x.getProjectId() ==
	 * projectId).findFirst().get();
	 * cloudTemplatePo.setProjectName(selectedProject.getProjectName());
	 * cloudTemplatePo.setPodId(podId); cloudTemplatePo.setObjectCode(objectCode);
	 * cloudTemplatePo.setParentObjectCode(parentObjectCode);
	 * 
	 * List<XxrCloudTemplateHeader> xxrCloudTemplateHeaders =
	 * cloudTemplateHeaderDaoImpl.getCloudTemplate(cloudTemplatePo);
	 * 
	 * 
	 * List<String> tableNames = new ArrayList<String>(); List<String>
	 * sourceTemplateHeaderNames = new ArrayList<String>();
	 * xxrCloudTemplateHeaders.forEach(x->tableNames.add(x.getTableName()));
	 * xxrCloudTemplateHeaders.forEach(x->sourceTemplateHeaderNames.add(x.
	 * getSourceHeaderTemplate())); sourceTemplateHeaderNames.removeIf(x->x ==
	 * null); tableNames.removeIf(x->x == null); List<String> tableNamesDistinct =
	 * tableNames.stream().distinct().collect(Collectors.toList()); List<String>
	 * sourceTemplateHeaderNamesDistinct =
	 * sourceTemplateHeaderNames.stream().distinct().collect(Collectors.toList());
	 * cloudTablesTemplatesPo.setSourceHeaderTemplateNames(
	 * sourceTemplateHeaderNamesDistinct);
	 * cloudTablesTemplatesPo.setTableNames(tableNamesDistinct); } catch(Exception
	 * ex) { log.error(ex.getMessage()); } return cloudTablesTemplatesPo; }
	 */

	/*public CloudTablesTemplatesPo getCloudTablesTemplates(long projectId, long podId, long objectId,
			long parentObjectId) throws Exception {
		log.info("Start of getCloudTablesTemplates in Service Layer ###");
		CloudTablesTemplatesPo cloudTablesTemplatesPo = new CloudTablesTemplatesPo();
		List<CloudTablesPo> cloudDataList = new ArrayList<>();
		List<TemplatesPo> templateHeaders = new ArrayList<>();
		try {
			// to retireve cloud tables
			cloudDataList = xxrCloudTableRepository.getTableIdNames(objectId);

			templateHeaders = sourceTemplateHeadersRepository.getTemplateHeaders(objectId, parentObjectId);

			cloudTablesTemplatesPo.setCloudTables(cloudDataList);
			cloudTablesTemplatesPo.setSourceTemplateHeaders(templateHeaders);
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}

		return cloudTablesTemplatesPo;

	}*/

	public SaveTemplateHeaderResponsePo saveCloudHeaders(List<SaveCloudTemplateHeadersPo> cloudTemplateHeadersPo,
			HttpServletRequest request) throws Exception, ValidationException {
		log.info("Start of saveCloudHeaders in Service Layer ###");
		String msg = "";
		long templateId = 0;

		SaveTemplateHeaderResponsePo saveTemplateHeaderResponsePo = new SaveTemplateHeaderResponsePo();

		try {
			msg = cloudTemplateHeadersSaveDaoImpl.saveCloudHeaders(cloudTemplateHeadersPo, request);
			saveTemplateHeaderResponsePo.setMessage(msg);
			SaveCloudTemplateHeadersPo saveCloudTemplateHeadersPo = cloudTemplateHeadersPo.get(0);
			String templateName = saveCloudTemplateHeadersPo.getTemplateName();
			if (!Validations.isNullOrEmpty(templateName)) {
				templateId = xxrCloudTemplateHeadersRepository.getTemplateId(templateName);
				saveTemplateHeaderResponsePo.setTemplateId(templateId);
				saveTemplateHeaderResponsePo.setTemplateName(templateName);
			}
			log.info(msg + "msg###########");
		} catch (ValidationException e) {
			throw new ValidationException(e.getMessage());
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}

		return saveTemplateHeaderResponsePo;
	}

	public SaveCloudTemplateColumnsResPo saveCloudTemplateColumns(
			List<SaveCloudTemplateColumnsPo> cloudTemplateColumnsPo, HttpServletRequest request)
			throws BadRequestException, Exception {
		log.info("Start of saveCloudTemplateColumns in Service Layer ###");
		String msg = "";
		List<XxrCloudTemplateColumns> xxrCloudTemplateColumns = new ArrayList<>();
		SaveCloudTemplateColumnsResPo saveCloudTemplateColumnsResPo = new SaveCloudTemplateColumnsResPo();
		try {
			msg = cloudTemplateColumnsDaoImpl.saveCloudTemplateColumns(cloudTemplateColumnsPo, request);
			saveCloudTemplateColumnsResPo.setMessage(msg);
			Long templateId = cloudTemplateColumnsPo.get(0).getTemplateId();
			if (templateId != null) {
				xxrCloudTemplateColumns = xxrCloudTemplateColumnsRepository.findByTemplateId(templateId);
				saveCloudTemplateColumnsResPo.setCloudTemplateColumns(xxrCloudTemplateColumns);
			}
			log.info(msg + "msg###########");
		} catch (BadRequestException e) {
			throw new BadRequestException(e.getMessage());
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}

		return saveCloudTemplateColumnsResPo;

	}

	public List<CloudMappingSetPo> getCloudMappingSetNames()
			throws Exception {
		log.info("Start Of getCloudMappingSetNames Method in Service Layer ####");
		List<CloudMappingSetPo> cloudMappingSetPo = new ArrayList<>();
		try {

			cloudMappingSetPo = xxrColumnMapHdrRepository.getCloudMappingSetNames();

		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}

		return cloudMappingSetPo;

	}

	@Override
	public SaveTemplateHeaderResponsePo copyCloudTemplate(CopyCloudReqPo copyCloudReqPo, HttpServletRequest request)
			throws Exception {
		log.info("Start Of copyCloudTemplate Method in Service Layer ####");
		// List<XxrCloudTemplateHeader> cloudTemplateHdrList = new ArrayList<>();
		List<XxrCloudTemplateColumns> cloudTemplateColumnsList = new ArrayList<>();
		String msg = "";
		Long newTemplateId = null;
		Long objectId = null;
		SaveTemplateHeaderResponsePo saveTemplateHeaderResponsePo = new SaveTemplateHeaderResponsePo();
		CloudStagingTablePo stagingPo = new CloudStagingTablePo();
		CreateDynamicViewPo createDynamicViewPo = new CreateDynamicViewPo();
		String metaDataTableName = "";
		String indic = null;
		Long tableId = null;
		List<String> diffcolumnSequenceList = new ArrayList<>();
		List<ColumnPo> columnPoLi = new ArrayList<>();
		try {
			XxrCloudTemplateHeader xxrCloudTemplateHeader = xxrCloudTemplateHeadersRepository
					.getCloudTemplateById(copyCloudReqPo.getOldTemplateId());
			if (xxrCloudTemplateHeader != null)
				objectId = xxrCloudTemplateHeader.getObjectId();
			else
				throw new Exception("There is no Template existing with this templateId");
			log.info(objectId + ":::::::::::::objectId");
			if (!copyCloudReqPo.getOldVersion().equalsIgnoreCase(copyCloudReqPo.getNewVersion())) {

				tableId = xxrCloudTableRepository.getTableId(copyCloudReqPo.getCloudMetaDataTableName());

				List<String> oldcolumnSequenceList = xxrCloudTemplateColumnsRepository
						.getColudTemplateColumns(copyCloudReqPo.getOldTemplateId());
				List<String> newcolumnSequenceList = new ArrayList<String>(
						Arrays.asList(xxrCloudColumnsRepository.getColumnName(tableId)));
				if (oldcolumnSequenceList.size() < newcolumnSequenceList.size()) {
					newcolumnSequenceList.removeAll(oldcolumnSequenceList);
					diffcolumnSequenceList = newcolumnSequenceList;
					indic = "added";
				} else if (oldcolumnSequenceList.size() > newcolumnSequenceList.size()) {
					oldcolumnSequenceList.removeAll(newcolumnSequenceList);
					diffcolumnSequenceList = oldcolumnSequenceList;
					indic = "removed";
				} else {
					indic = "nodiff";
				}
			}
			Long metaDataTableId = tableId;
			// SaveCloudTemplateHeader for newtemplate
			msg = saveCloudTemplateHeader(xxrCloudTemplateHeader, metaDataTableId, copyCloudReqPo, request);
			log.info("Cloud:: " + msg);
			if (!Validations.isNullOrEmpty(copyCloudReqPo.getNewTemplateName()))
				newTemplateId = xxrCloudTemplateHeadersRepository.getTemplateId(copyCloudReqPo.getNewTemplateName());

			cloudTemplateColumnsList = xxrCloudTemplateColumnsRepository
					.findByTemplateId(copyCloudReqPo.getOldTemplateId());
			if ("added".equalsIgnoreCase(indic)) {
				columnPoLi = xxrCloudColumnsRepository.getCloudColumnData(tableId, diffcolumnSequenceList);
				for (ColumnPo columnPo : columnPoLi) {
					XxrCloudTemplateColumns xxrCloudTemplateColumns = new XxrCloudTemplateColumns();

					xxrCloudTemplateColumns.setColumnName(columnPo.getColumnName());
					xxrCloudTemplateColumns.setColumnType(columnPo.getColumnType());
					//xxrCloudTemplateColumns.setEnabledFlag("Y");
					xxrCloudTemplateColumns.setMappingType("As-Is");
					xxrCloudTemplateColumns.setOrigTransRef("N");
					xxrCloudTemplateColumns.setNullAllowedFlag(columnPo.getNullAllowedFlag());
					if (columnPo.getNullAllowedFlag().equalsIgnoreCase("N"))
						xxrCloudTemplateColumns.setSelected("M");
					else
						xxrCloudTemplateColumns.setSelected("Y");
					cloudTemplateColumnsList.add(xxrCloudTemplateColumns);
				}
			}
			// SaveCloudTemplateColumns for newtemplate
			msg = saveCloudTemplateColumns(cloudTemplateColumnsList, newTemplateId, request);
			log.info("cloud::: " + msg);
			if ("added".equalsIgnoreCase(indic) || "removed".equalsIgnoreCase(indic)) {
				log.info(copyCloudReqPo.getNewVersion() + ":::::::::version");
				generateSequence(newTemplateId, objectId, copyCloudReqPo.getNewVersion(), request);
			}
			if (newTemplateId != null) {
				final long nTemplateId = newTemplateId;
				XxrCloudTemplateHeader tempHeader = xxrCloudTemplateHeadersRepository.findAll().stream()
						.filter(x -> x.getTemplateId() == nTemplateId).findFirst().get();

				log.info("tableId:::::::" + tempHeader.getMetaDataTableId());

				if (tempHeader.getMetaDataTableId() != null)
					metaDataTableName = xxrCloudTableRepository.getMetaDataTableName(tempHeader.getMetaDataTableId());

				/*
				 * log.info("metaDataTableName::" + metaDataTableName + "newTemplateId::" +
				 * newTemplateId + "podId:::" + copyCloudReqPo.getPodId());
				 * 
				 * stagingPo =
				 * createCloudStagTableDaoImpl.createStaggingTable(metaDataTableName,
				 * newTemplateId, copyCloudReqPo.getPodId(), request);
				 */
				log.info("Cloud staging Table::::: " + stagingPo.getResult() + "stagingTableName :: "
						+ stagingPo.getTableName());

				if (stagingPo != null && !(stagingPo.getResult().isBlank())
						&& stagingPo.getResult().equalsIgnoreCase("Y")) {
					String stagingTableName = stagingPo.getTableName().split(":")[1];

					if ("added".equalsIgnoreCase(indic)) {
						alterStagingTable(columnPoLi, stagingTableName);
					}
					// tempHeader.setStagingTableName(stagingTableName);
					createDynamicViewPo = createDynamicViewDaoImpl.createDynamicView(newTemplateId, stagingTableName,
							request);
					log.info("Cloud Dynamic view ::::: " + createDynamicViewPo.getResult() + "DynamicView :: "
							+ createDynamicViewPo.getViewName());

					/*
					 * if (createDynamicViewPo != null &&
					 * !(createDynamicViewPo.getResult().isBlank()) &&
					 * createDynamicViewPo.getResult().equalsIgnoreCase("Y"))
					 */
					// tempHeader.setViewName(createDynamicViewPo.getViewName());
					// xxrCloudTemplateHeadersRepository.save(tempHeader);
				}

				saveTemplateHeaderResponsePo.setTemplateId(newTemplateId);
				saveTemplateHeaderResponsePo.setTemplateName(copyCloudReqPo.getNewTemplateName());
				saveTemplateHeaderResponsePo.setMessage("CloudTemplate is successfully copied");

			}

		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e.getMessage());
		}
		return saveTemplateHeaderResponsePo;
	}

	private String saveCloudTemplateHeader(XxrCloudTemplateHeader x, Long metaDataTableId,
			CopyCloudReqPo copyCloudReqPo, HttpServletRequest request) throws Exception {
		List<SaveCloudTemplateHeadersPo> cloudTemplateHeadersPo = new ArrayList<>();
		String msg = "";
		try {

			SaveCloudTemplateHeadersPo saveCloudTemplateHeadersPo = new SaveCloudTemplateHeadersPo();

			saveCloudTemplateHeadersPo.setTemplateName(copyCloudReqPo.getNewTemplateName());
			//saveCloudTemplateHeadersPo.setPodId(copyCloudReqPo.getPodId());
			saveCloudTemplateHeadersPo.setProjectId(x.getProjectId());
			saveCloudTemplateHeadersPo.setParentObjectId(x.getParentObjectId());
			saveCloudTemplateHeadersPo.setObjectId(x.getObjectId());
			//saveCloudTemplateHeadersPo.setBu(x.getBu());
			saveCloudTemplateHeadersPo.setTemplateCode(x.getTemplateCode());
			saveCloudTemplateHeadersPo.setPrimaryTemplateFlag(x.getPrimaryTemplateFlag());
			//saveCloudTemplateHeadersPo.setBuSpecific(x.getBuSpecific());
			if (metaDataTableId != null)
				saveCloudTemplateHeadersPo.setMetaDataTableId(metaDataTableId);
			else
				saveCloudTemplateHeadersPo.setMetaDataTableId(x.getMetaDataTableId());
			saveCloudTemplateHeadersPo.setSourceTemplateId(x.getSourceTemplateId());
			saveCloudTemplateHeadersPo.setVersion(copyCloudReqPo.getNewVersion());
			// saveCloudTemplateHeadersPo.setStagingTableName(x.getStagingTableName());
			// saveCloudTemplateHeadersPo.setViewName(x.getViewName());

			cloudTemplateHeadersPo.add(saveCloudTemplateHeadersPo);

			msg = cloudTemplateHeadersSaveDaoImpl.saveCloudHeaders(cloudTemplateHeadersPo, request);
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return msg;
	}

	private String saveCloudTemplateColumns(List<XxrCloudTemplateColumns> cloudTemplateColumnsList, Long newTemplateId,
			HttpServletRequest request) throws Exception {
		List<SaveCloudTemplateColumnsPo> cloudTemplateColumnsPo = new ArrayList<>();
		String msg = "";
		try {
			for (XxrCloudTemplateColumns xxrCloudTemplateColumns : cloudTemplateColumnsList) {

				SaveCloudTemplateColumnsPo saveCloudTemplateColumnsPo = new SaveCloudTemplateColumnsPo();

				saveCloudTemplateColumnsPo.setTemplateId(newTemplateId);
				// saveCloudTemplateColumnsPo.setColumnId(xxrCloudTemplateColumns.getColumnId());
				saveCloudTemplateColumnsPo.setColumnName(xxrCloudTemplateColumns.getColumnName());
				saveCloudTemplateColumnsPo.setDisplaySeq(xxrCloudTemplateColumns.getSeq());
				/*
				 * saveCloudTemplateColumnsPo.setEnableFlag(xxrCloudTemplateColumns.
				 * getEnabledFlag());
				 * saveCloudTemplateColumnsPo.setEndDate(xxrCloudTemplateColumns.getEndDate());
				 * saveCloudTemplateColumnsPo.setStartDate(xxrCloudTemplateColumns.getStartDate(
				 * ));
				 */
				saveCloudTemplateColumnsPo.setMappingSetId(xxrCloudTemplateColumns.getMappingSetId());
				saveCloudTemplateColumnsPo.setMappingType(xxrCloudTemplateColumns.getMappingType());
				saveCloudTemplateColumnsPo.setMappingValue1(xxrCloudTemplateColumns.getMappingValue1());
				saveCloudTemplateColumnsPo.setMappingValue2(xxrCloudTemplateColumns.getMappingValue2());
				saveCloudTemplateColumnsPo.setMappingValue3(xxrCloudTemplateColumns.getMappingValue3());
				saveCloudTemplateColumnsPo.setMappingValue4(xxrCloudTemplateColumns.getMappingValue4());
				saveCloudTemplateColumnsPo.setMappingValue5(xxrCloudTemplateColumns.getMappingValue5());
				saveCloudTemplateColumnsPo.setSelected(xxrCloudTemplateColumns.getSelected());
				saveCloudTemplateColumnsPo.setSourceColumnId(xxrCloudTemplateColumns.getSourceColumnId());
				saveCloudTemplateColumnsPo.setColumnType(xxrCloudTemplateColumns.getColumnType());
				saveCloudTemplateColumnsPo.setWidth(xxrCloudTemplateColumns.getWidth());
				saveCloudTemplateColumnsPo.setNullAllowedFlag(xxrCloudTemplateColumns.getNullAllowedFlag());
				saveCloudTemplateColumnsPo.setOrigTransRef(xxrCloudTemplateColumns.getOrigTransRef());

				cloudTemplateColumnsPo.add(saveCloudTemplateColumnsPo);

			}

			msg = cloudTemplateColumnsDaoImpl.saveCloudTemplateColumns(cloudTemplateColumnsPo, request);
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return msg;
	}

	private void alterStagingTable(List<ColumnPo> columnPoLi, String stagingTableName) {
//		try {
//			Connection con = dataSourceUtil.createConnection();
//			Statement stmnnt = con.createStatement();
			String dataType = "";
			String sql = "";
//			for (ColumnPo columnPo : columnPoLi) {
//				if (columnPo.getColumnType().equalsIgnoreCase("V") && !Validations.isNullOrEmpty(columnPo.getWidth()))
//					dataType = "VARCHAR2(" + columnPo.getWidth() + ")";
//				else if (columnPo.getColumnType().equalsIgnoreCase("V")
//						&& Validations.isNullOrEmpty(columnPo.getWidth()))
//					dataType = "VARCHAR2(1000)";
//				else if (columnPo.getColumnType().equalsIgnoreCase("N"))
//					dataType = "NUMBER";
//				else if (columnPo.getColumnType().equalsIgnoreCase("D"))
//					dataType = "DATE";
//				else if (columnPo.getColumnType().equalsIgnoreCase("T"))
//					dataType = "TIMESTAMP(6)";
//				sql = "ALTER TABLE " + stagingTableName + " ADD " + columnPo.getColumnName() + " " + dataType;
//				stmnnt.execute(sql);
//			}
//			stmnnt.close();
//			con.close();
//		} catch (Exception e) {
//			log.error(e.getMessage());
//		}

	}

	@Override
	public SaveCloudTemplateColumnsResPo generateSequence(Long templateId, Long objectId, String version,
			HttpServletRequest request) throws Exception {
		log.info("Start Of generateSequence Method in Service Layer ####");
		List<XxrCloudTemplateColumns> cloudTemplateColumnsList = new ArrayList<>();
		List<SaveCloudTemplateColumnsPo> cloudTemplateColumnsPo = new ArrayList<>();
		List<Object> columnSequenceList = new ArrayList<>();
		String msg = "";
		SaveCloudTemplateColumnsResPo saveCloudTemplateColumnsResPo = new SaveCloudTemplateColumnsResPo();
		try {
			if (templateId != null)
				cloudTemplateColumnsList = xxrCloudTemplateColumnsRepository.findByTemplateId(templateId);

			columnSequenceList = xxrFbdiTempColsRepository.getColumnNameandSequence(objectId, version);

			log.info(columnSequenceList.size() + "::::::::size");
			if (!columnSequenceList.isEmpty()) {
				cloudTemplateColumnsPo = getColumnSequence(columnSequenceList, cloudTemplateColumnsList);
				msg = cloudTemplateColumnsDaoImpl.saveCloudTemplateColumns(cloudTemplateColumnsPo, request);
				log.info("cloud::: " + msg);
				cloudTemplateColumnsList = xxrCloudTemplateColumnsRepository.findByTemplateId(templateId);

				saveCloudTemplateColumnsResPo.setCloudTemplateColumns(cloudTemplateColumnsList);
				saveCloudTemplateColumnsResPo.setMessage("Successfully generated Sequence");
			} else {
				throw new Exception("We dont have sequence in fbdi workbench for above combination");
			}
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return saveCloudTemplateColumnsResPo;
	}

	@Override
	public SaveCloudTemplateColumnsResPo xlsmGenerateSequence(Long templateId, Long objectId, String version,
			HttpServletRequest request) throws Exception,ValidationException {
		log.info("Start Of generateSequence Method in Service Layer ####");
		List<XxrCloudTemplateColumns> cloudTemplateColumnsList = new ArrayList<>();
		List<SaveCloudTemplateColumnsPo> cloudTemplateColumnsPo = new ArrayList<>();
		List<Object> columnSequenceList = new ArrayList<>();
		String msg = "";
		SaveCloudTemplateColumnsResPo saveCloudTemplateColumnsResPo = new SaveCloudTemplateColumnsResPo();
		try {
			if (templateId != null)
				cloudTemplateColumnsList = xxrCloudTemplateColumnsRepository.findByTemplateId(templateId);
			columnSequenceList = xxrXlsmTempColsRepository.getColumnNameandSequence(objectId, version);
			log.info(columnSequenceList.size() + "::::::::size");
			if (!columnSequenceList.isEmpty()) {
				cloudTemplateColumnsPo = getColumnSequence(columnSequenceList, cloudTemplateColumnsList);
				msg = cloudTemplateColumnsDaoImpl.saveCloudTemplateColumns(cloudTemplateColumnsPo, request);
				log.info("cloud::: " + msg);
				cloudTemplateColumnsList = xxrCloudTemplateColumnsRepository.findByTemplateId(templateId);

				saveCloudTemplateColumnsResPo.setCloudTemplateColumns(cloudTemplateColumnsList);
				saveCloudTemplateColumnsResPo.setMessage("Successfully generated Sequence");
			} else {
				throw new ValidationException("We dont have sequence in fbdi workbench for above combination");
			}
		}catch (ValidationException e) {
			throw new ValidationException(e.getMessage());
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return saveCloudTemplateColumnsResPo;
	}

	private List<SaveCloudTemplateColumnsPo> getColumnSequence(List<Object> columnSequenceList,
			List<XxrCloudTemplateColumns> cloudTemplateColumnsList) throws Exception {
		List<SaveCloudTemplateColumnsPo> cloudTemplateColumnsPo = new ArrayList<>();
		try {

			for (XxrCloudTemplateColumns xxrCloudTemplateColumns : cloudTemplateColumnsList) {
				Integer seq = null;
				for (Object columnSequence : columnSequenceList) {
					Object[] obj = (Object[]) columnSequence;
					String columnName = (String) obj[0];
					if (xxrCloudTemplateColumns.getColumnName().equalsIgnoreCase(columnName)) {
						seq = (int) obj[1];
						break;
					}
				}
				SaveCloudTemplateColumnsPo saveCloudTemplateColumnsPo = new SaveCloudTemplateColumnsPo();

				saveCloudTemplateColumnsPo.setTemplateId(xxrCloudTemplateColumns.getTemplateId());
				saveCloudTemplateColumnsPo.setColumnId(xxrCloudTemplateColumns.getColumnId());
				saveCloudTemplateColumnsPo.setColumnName(xxrCloudTemplateColumns.getColumnName());
				if (seq != null)
					saveCloudTemplateColumnsPo.setDisplaySeq(seq);
				//saveCloudTemplateColumnsPo.setEnableFlag(xxrCloudTemplateColumns.getEnabledFlag());
				//saveCloudTemplateColumnsPo.setEndDate(xxrCloudTemplateColumns.getEndDate());
				//saveCloudTemplateColumnsPo.setStartDate(xxrCloudTemplateColumns.getStartDate());
				saveCloudTemplateColumnsPo.setMappingSetId(xxrCloudTemplateColumns.getMappingSetId());
				saveCloudTemplateColumnsPo.setMappingType(xxrCloudTemplateColumns.getMappingType());
				saveCloudTemplateColumnsPo.setMappingValue1(xxrCloudTemplateColumns.getMappingValue1());
				saveCloudTemplateColumnsPo.setMappingValue2(xxrCloudTemplateColumns.getMappingValue2());
				saveCloudTemplateColumnsPo.setMappingValue3(xxrCloudTemplateColumns.getMappingValue3());
				saveCloudTemplateColumnsPo.setMappingValue4(xxrCloudTemplateColumns.getMappingValue4());
				saveCloudTemplateColumnsPo.setMappingValue5(xxrCloudTemplateColumns.getMappingValue5());
				saveCloudTemplateColumnsPo.setSelected(xxrCloudTemplateColumns.getSelected());
				saveCloudTemplateColumnsPo.setSourceColumnId(xxrCloudTemplateColumns.getSourceColumnId());
				saveCloudTemplateColumnsPo.setColumnType(xxrCloudTemplateColumns.getColumnType());
				saveCloudTemplateColumnsPo.setWidth(xxrCloudTemplateColumns.getWidth());
				saveCloudTemplateColumnsPo.setNullAllowedFlag(xxrCloudTemplateColumns.getNullAllowedFlag());
				saveCloudTemplateColumnsPo.setOrigTransRef(xxrCloudTemplateColumns.getOrigTransRef());

				cloudTemplateColumnsPo.add(saveCloudTemplateColumnsPo);

			}

		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}

		return cloudTemplateColumnsPo;
	}

	@Override
	public List<CldSrcTemplateIdsResPo> getCldSrcTemplateIds(String objectCode, String parentObjectCode,Long roleId) {
		log.info("Start of getCldSrcTemplateIds in service#######");
		List<CldSrcTemplateIdsResPo> cldSrcTemplateIdResPo=new ArrayList<>();
		
		if("All".equalsIgnoreCase(objectCode)) {
			cldSrcTemplateIdResPo=xxrCldTempHdrsViewRepository.getByParentObjectCode(parentObjectCode,roleId);
		}else {
			cldSrcTemplateIdResPo=xxrCldTempHdrsViewRepository.getByObjectCode(objectCode,roleId);
		}
		
		return cldSrcTemplateIdResPo;
	}
	
	
	@Override
	public SaveCloudTemplateColumnsResPo saveCloudTemplateColumnsWithJpa(
			List<SaveCloudTemplateColumnsPo> cloudTemplateColumnsPo) throws BadRequestException, ValidationException {
		SaveCloudTemplateColumnsResPo saveCloudTemplateColumnsResPo = new SaveCloudTemplateColumnsResPo();
		log.info("Start of saveCloudTemplateColumnsWithJpa #########");
		List<XxrCloudTemplateColumns> cloudTempColumnsLi = new ArrayList<>();
		for (SaveCloudTemplateColumnsPo saveCloudTemplateColumnsPo : cloudTemplateColumnsPo) {
			if (Validations.isNullOrEmpty(saveCloudTemplateColumnsPo.getColumnName())
					|| saveCloudTemplateColumnsPo.getTemplateId() == null)
				throw new BadRequestException("templateId and columnName are Mandatory fields");
			if (saveCloudTemplateColumnsPo.getColumnId() == null) {
				XxrCloudTemplateColumns xxrCloudTemplateColumns = new XxrCloudTemplateColumns();
				xxrCloudTemplateColumns.setColumnName(saveCloudTemplateColumnsPo.getColumnName());
				xxrCloudTemplateColumns.setColumnType(saveCloudTemplateColumnsPo.getColumnType());
				xxrCloudTemplateColumns.setDescription(saveCloudTemplateColumnsPo.getDescription());
				/*
				 * xxrCloudTemplateColumns.setEnabledFlag(saveCloudTemplateColumnsPo.
				 * getEnableFlag());
				 * xxrCloudTemplateColumns.setEndDate(saveCloudTemplateColumnsPo.getEndDate());
				 */
				xxrCloudTemplateColumns.setMappingSetId(saveCloudTemplateColumnsPo.getMappingSetId());
				xxrCloudTemplateColumns.setMappingType(saveCloudTemplateColumnsPo.getMappingType());
				xxrCloudTemplateColumns.setMappingValue1(saveCloudTemplateColumnsPo.getMappingValue1());
				xxrCloudTemplateColumns.setMappingValue2(saveCloudTemplateColumnsPo.getMappingValue2());
				xxrCloudTemplateColumns.setMappingValue3(saveCloudTemplateColumnsPo.getMappingValue3());
				xxrCloudTemplateColumns.setMappingValue4(saveCloudTemplateColumnsPo.getMappingValue4());
				xxrCloudTemplateColumns.setMappingValue5(saveCloudTemplateColumnsPo.getMappingValue5());
				xxrCloudTemplateColumns.setNullAllowedFlag(saveCloudTemplateColumnsPo.getNullAllowedFlag());
				xxrCloudTemplateColumns.setOrigTransRef(saveCloudTemplateColumnsPo.getOrigTransRef());
				xxrCloudTemplateColumns.setSelected(saveCloudTemplateColumnsPo.getSelected());
				xxrCloudTemplateColumns.setSeq(saveCloudTemplateColumnsPo.getDisplaySeq());
				xxrCloudTemplateColumns.setSourceColumnId(saveCloudTemplateColumnsPo.getSourceColumnId());
				//xxrCloudTemplateColumns.setStartDate(saveCloudTemplateColumnsPo.getStartDate());
				xxrCloudTemplateColumns.setTemplateId(saveCloudTemplateColumnsPo.getTemplateId());
				xxrCloudTemplateColumns.setWidth(saveCloudTemplateColumnsPo.getWidth());
				xxrCloudTemplateColumns.setLastUpdatedBy("ConvertRite");
				xxrCloudTemplateColumns.setLastUpdatedDate(new java.sql.Date(new java.util.Date().getTime()));
				xxrCloudTemplateColumns.setCreationDate(new java.sql.Date(new java.util.Date().getTime()));
				xxrCloudTemplateColumns.setLastUpdatedBy("ConvertRite");
				cloudTempColumnsLi.add(xxrCloudTemplateColumns);
			} else {
				Optional<XxrCloudTemplateColumns> cloudTempColsOpt = xxrCloudTemplateColumnsRepository
						.findById(saveCloudTemplateColumnsPo.getColumnId());
				if (!cloudTempColsOpt.isPresent())
					throw new ValidationException("There is not Column with provided ColumnId");
				XxrCloudTemplateColumns cldTempCols = cloudTempColsOpt.get();

				cldTempCols.setColumnName(saveCloudTemplateColumnsPo.getColumnName());
				cldTempCols.setColumnType(saveCloudTemplateColumnsPo.getColumnType());
				cldTempCols.setDescription(saveCloudTemplateColumnsPo.getDescription());
				//cldTempCols.setEnabledFlag(saveCloudTemplateColumnsPo.getEnableFlag());
				//cldTempCols.setEndDate(saveCloudTemplateColumnsPo.getEndDate());
				cldTempCols.setMappingSetId(saveCloudTemplateColumnsPo.getMappingSetId());
				cldTempCols.setMappingType(saveCloudTemplateColumnsPo.getMappingType());
				cldTempCols.setMappingValue1(saveCloudTemplateColumnsPo.getMappingValue1());
				cldTempCols.setMappingValue2(saveCloudTemplateColumnsPo.getMappingValue2());
				cldTempCols.setMappingValue3(saveCloudTemplateColumnsPo.getMappingValue3());
				cldTempCols.setMappingValue4(saveCloudTemplateColumnsPo.getMappingValue4());
				cldTempCols.setMappingValue5(saveCloudTemplateColumnsPo.getMappingValue5());
				cldTempCols.setNullAllowedFlag(saveCloudTemplateColumnsPo.getNullAllowedFlag());
				cldTempCols.setOrigTransRef(saveCloudTemplateColumnsPo.getOrigTransRef());
				cldTempCols.setSelected(saveCloudTemplateColumnsPo.getSelected());
				cldTempCols.setSeq(saveCloudTemplateColumnsPo.getDisplaySeq());
				cldTempCols.setSourceColumnId(saveCloudTemplateColumnsPo.getSourceColumnId());
				//cldTempCols.setStartDate(saveCloudTemplateColumnsPo.getStartDate());
				cldTempCols.setTemplateId(saveCloudTemplateColumnsPo.getTemplateId());
				cldTempCols.setWidth(saveCloudTemplateColumnsPo.getWidth());
				cldTempCols.setLastUpdatedBy("ConvertRite");
				cldTempCols.setLastUpdatedDate(new java.sql.Date(new java.util.Date().getTime()));
				cldTempCols.setCreationDate(new java.sql.Date(new java.util.Date().getTime()));
				cldTempCols.setLastUpdatedBy("ConvertRite");;
				cloudTempColumnsLi.add(cldTempCols);
			}		
			
		}
		
		List<XxrCloudTemplateColumns> cloudTemplateColumnsLiRes = xxrCloudTemplateColumnsRepository
				.saveAll(cloudTempColumnsLi);
		saveCloudTemplateColumnsResPo.setCloudTemplateColumns(cloudTemplateColumnsLiRes);
		saveCloudTemplateColumnsResPo.setMessage("Successfully Saved/Updated CloudTemplateColumns");
		return saveCloudTemplateColumnsResPo;
	}
	
	@Override
	public SaveTemplateHeaderResponsePo saveCloudTemplateHdrsWithJpa(
			SaveCloudTemplateHeadersPo saveCloudTemplateHeadersPo) throws ValidationException {
		log.info("Start of saveCloudTemplateHdrsWithJpa in service ######");
		SaveTemplateHeaderResponsePo saveTemplateHeaderResponsePo = new SaveTemplateHeaderResponsePo();
		XxrCloudTemplateHeader xxrCloudTemplateHeaderRes = new XxrCloudTemplateHeader();

	
		if (saveCloudTemplateHeadersPo.getTemplateId()!=null) {
			Optional<XxrCloudTemplateHeader> cloudTempHdr = xxrCloudTemplateHeadersRepository
					.findById(saveCloudTemplateHeadersPo.getTemplateId());
			if (!cloudTempHdr.isPresent())
				throw new ValidationException("There is no CloudTemplate with this TemplateId");
			XxrCloudTemplateHeader xxrCldTempHdr = cloudTempHdr.get();
			xxrCldTempHdr.setViewName(saveCloudTemplateHeadersPo.getViewName());
			xxrCldTempHdr.setVersion(saveCloudTemplateHeadersPo.getVersion());
			xxrCldTempHdr.setStagingTableName(saveCloudTemplateHeadersPo.getStagingTableName());
			xxrCldTempHdr.setSourceTemplateId(saveCloudTemplateHeadersPo.getSourceTemplateId());
			//xxrCldTempHdr.setPodId(saveCloudTemplateHeadersPo.getPodId());
			xxrCldTempHdr.setProjectId(saveCloudTemplateHeadersPo.getProjectId());
			xxrCldTempHdr.setParentObjectId(saveCloudTemplateHeadersPo.getParentObjectId());
			xxrCldTempHdr.setObjectId(saveCloudTemplateHeadersPo.getObjectId());
			xxrCldTempHdr.setMetaDataTableId(saveCloudTemplateHeadersPo.getMetaDataTableId());
			xxrCldTempHdr.setTemplateName(saveCloudTemplateHeadersPo.getTemplateName());
			//xxrCldTempHdr.setBu(saveCloudTemplateHeadersPo.getBu());
			//xxrCldTempHdr.setBuSpecific(saveCloudTemplateHeadersPo.getBuSpecific());
			xxrCldTempHdr.setTemplateCode(saveCloudTemplateHeadersPo.getTemplateCode());
			xxrCldTempHdr.setPrimaryTemplateFlag(saveCloudTemplateHeadersPo.getPrimaryTemplateFlag());
			//xxrCldTempHdr.setCreatedBy("ConvertRite");
			//xxrCldTempHdr.setCreationDate(new java.sql.Date(new java.util.Date().getTime()));
			xxrCldTempHdr.setLastUpdatedBy("ConvertRite");
			xxrCldTempHdr.setLastUpdatedDate(new java.sql.Date(new java.util.Date().getTime()));
			xxrCldTempHdr.setAttribute1(saveCloudTemplateHeadersPo.getAttribute1());
			xxrCldTempHdr.setAttribute2(saveCloudTemplateHeadersPo.getAttribute2());
			xxrCldTempHdr.setAttribute3(saveCloudTemplateHeadersPo.getAttribute3());
			xxrCldTempHdr.setAttribute4(saveCloudTemplateHeadersPo.getAttribute4());
			xxrCldTempHdr.setAttribute5(saveCloudTemplateHeadersPo.getAttribute5());
			xxrCloudTemplateHeaderRes = xxrCloudTemplateHeadersRepository.save(xxrCldTempHdr);

		} else {	
			Long templateId = xxrCloudTemplateHeadersRepository
					.getTemplateId(saveCloudTemplateHeadersPo.getTemplateName());
			if (templateId != null)
				throw new ValidationException("This Cloud TemplateName already exists ");

			XxrCloudTemplateHeader xxrCldTempHdrs = new XxrCloudTemplateHeader();
			xxrCldTempHdrs.setViewName(saveCloudTemplateHeadersPo.getViewName());
			xxrCldTempHdrs.setVersion(saveCloudTemplateHeadersPo.getVersion());
			xxrCldTempHdrs.setStagingTableName(saveCloudTemplateHeadersPo.getStagingTableName());
			xxrCldTempHdrs.setSourceTemplateId(saveCloudTemplateHeadersPo.getSourceTemplateId());
			//xxrCldTempHdrs.setPodId(saveCloudTemplateHeadersPo.getPodId());
			xxrCldTempHdrs.setTemplateCode(saveCloudTemplateHeadersPo.getTemplateCode());
			xxrCldTempHdrs.setPrimaryTemplateFlag(saveCloudTemplateHeadersPo.getPrimaryTemplateFlag());
			xxrCldTempHdrs.setProjectId(saveCloudTemplateHeadersPo.getProjectId());
			xxrCldTempHdrs.setParentObjectId(saveCloudTemplateHeadersPo.getParentObjectId());
			xxrCldTempHdrs.setObjectId(saveCloudTemplateHeadersPo.getObjectId());
			xxrCldTempHdrs.setMetaDataTableId(saveCloudTemplateHeadersPo.getMetaDataTableId());
			xxrCldTempHdrs.setTemplateName(saveCloudTemplateHeadersPo.getTemplateName());
			//xxrCldTempHdrs.setBu(saveCloudTemplateHeadersPo.getBu());
			//xxrCldTempHdrs.setBuSpecific(saveCloudTemplateHeadersPo.getBuSpecific());
			xxrCldTempHdrs.setAttribute1(saveCloudTemplateHeadersPo.getAttribute1());
			xxrCldTempHdrs.setAttribute2(saveCloudTemplateHeadersPo.getAttribute2());
			xxrCldTempHdrs.setAttribute3(saveCloudTemplateHeadersPo.getAttribute3());
			xxrCldTempHdrs.setAttribute4(saveCloudTemplateHeadersPo.getAttribute4());
			xxrCldTempHdrs.setAttribute5(saveCloudTemplateHeadersPo.getAttribute5());
			xxrCldTempHdrs.setCreatedBy("ConvertRite");
			xxrCldTempHdrs.setCreationDate(new java.sql.Date(new java.util.Date().getTime()));
			xxrCldTempHdrs.setLastUpdatedBy("ConvertRite");
			xxrCldTempHdrs.setLastUpdatedDate(new java.sql.Date(new java.util.Date().getTime()));
			xxrCloudTemplateHeaderRes = xxrCloudTemplateHeadersRepository.save(xxrCldTempHdrs);
		}

		saveTemplateHeaderResponsePo.setTemplateId(xxrCloudTemplateHeaderRes.getTemplateId());
		saveTemplateHeaderResponsePo.setTemplateName(xxrCloudTemplateHeaderRes.getTemplateName());
		saveTemplateHeaderResponsePo.setMessage("Successfully Saved/Updated CloudTemplateHdrs");

		return saveTemplateHeaderResponsePo;
	}

	@Override
	public List<CldSrcTemplateIdsResPo> getDefaultCldSrcTemplateIds(String objectCode, String parentObjectCode,
			Long roleId) {
		log.info("Start of getDefaultCldSrcTemplateIds in service#######");
		List<CldSrcTemplateIdsResPo> cldSrcTemplateIdResPo=new ArrayList<>();
		
		if("All".equalsIgnoreCase(objectCode)) {
			cldSrcTemplateIdResPo=xxrCldTempHdrsViewRepository.getDefaultTempByParentObjectCode(parentObjectCode,roleId);
		}else {
			cldSrcTemplateIdResPo=xxrCldTempHdrsViewRepository.getDefaultTempByObjectCode(objectCode,roleId);
		}
		
		return cldSrcTemplateIdResPo;
	}
}
