package com.rite.products.convertrite.service;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.rite.products.convertrite.Validations.Validations;
import com.rite.products.convertrite.exception.BadRequestException;
import com.rite.products.convertrite.exception.ValidationException;
import com.rite.products.convertrite.model.XxrFormulaSets;
import com.rite.products.convertrite.model.XxrFormulaSetsView;
import com.rite.products.convertrite.po.CloudColumnsPo;
import com.rite.products.convertrite.po.SaveFormulaColumnsPo;
import com.rite.products.convertrite.po.SaveFormulaColumnsResPo;
import com.rite.products.convertrite.po.SaveFormulaSetHeaderPo;
import com.rite.products.convertrite.po.SaveFormulaSetHeadersResPo;
import com.rite.products.convertrite.po.SaveFormulaSetTablesPo;
import com.rite.products.convertrite.po.SaveFormulaSetTablesResPo;
import com.rite.products.convertrite.po.SourceColumnsPo;
import com.rite.products.convertrite.po.SourceTablesPo;
import com.rite.products.convertrite.po.TestDataResPo;
import com.rite.products.convertrite.po.TestSqlSyntaxResPo;
import com.rite.products.convertrite.po.XxrFormulaColumnsResPo;
import com.rite.products.convertrite.po.XxrFormulaSetTablesResPo;
import com.rite.products.convertrite.po.XxrFormulaSetsResPo;
import com.rite.products.convertrite.respository.CloudMetaDataRepository;
import com.rite.products.convertrite.respository.FormulaSetDaoImpl;
import com.rite.products.convertrite.respository.SaveFormulaColumnsDaoImpl;
import com.rite.products.convertrite.respository.SaveFormulaSetHeaderDaoImpl;
import com.rite.products.convertrite.respository.SaveFormulaSetTablesDaoImpl;
import com.rite.products.convertrite.respository.SourceTemplateColumnsRepository;
import com.rite.products.convertrite.respository.SourceTemplateHeadersRepository;
import com.rite.products.convertrite.respository.XxrCloudColumnsRepository;
import com.rite.products.convertrite.respository.XxrFormulaColumnsRepository;
import com.rite.products.convertrite.respository.XxrFormulaSetsRepository;
import com.rite.products.convertrite.respository.XxrFormulaSetsViewRepository;
import com.rite.products.convertrite.respository.XxrFormulaTablesRepository;
import com.rite.products.convertrite.respository.XxrSourceColumnsRepository;
import com.rite.products.convertrite.respository.XxrSourceTablesRepository;
import com.rite.products.convertrite.utils.Utils;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;

@Service
public class FormulaSetServiceImpl implements FormulaSetService {

	private static final Logger log = LoggerFactory.getLogger(FormulaSetServiceImpl.class);

	@Autowired
	XxrFormulaSetsRepository xxrFormulaSetsRepository;
	@Autowired
	CloudMetaDataRepository cloudMetaDataRepository;
	@Autowired
	SaveFormulaSetHeaderDaoImpl saveFormulaSetHeaderDaoImpl;
	@Autowired
	XxrFormulaTablesRepository xxrFormulaTablesRepository;
	@Autowired
	SaveFormulaSetTablesDaoImpl saveFormulaSetTablesDaoImpl;
	@Autowired
	XxrFormulaColumnsRepository xxrFormulaColumnsRepository;
	@Autowired
	SaveFormulaColumnsDaoImpl saveFormulaColumnsDaoImpl;
	@Autowired
	XxrSourceTablesRepository xxrSourceTablesRepository;
	@Autowired
	XxrSourceColumnsRepository xxrSourceColumnsRepository;
	@Autowired
	XxrCloudColumnsRepository xxrCloudColumnsRepository;
	@Autowired
	SourceTemplateHeadersRepository sourceTemplateHeadersRepository;
	@Autowired
	SourceTemplateColumnsRepository sourceTemplateColumnsRepository;
	@Autowired
	FormulaSetDaoImpl formulaSetDaoImpl;
	@Autowired
	XxrFormulaSetsViewRepository xxrFormulaSetsViewRepository;

	@Override
	public List<XxrFormulaSetsResPo> getFormulaSetsById(Long formulaSetId) throws Exception {
		log.info("Start Of Method getFormulaSets ####");
		List<XxrFormulaSets> formulaSetsList = new ArrayList<>();
		List<XxrFormulaSetsResPo> formulaSetResList = new ArrayList<>();
		try {
			formulaSetsList = xxrFormulaSetsRepository.findByformulaSetId(formulaSetId);
			formulaSetsList.stream().forEach(x -> {
				XxrFormulaSetsResPo xxrFormulaSetsResPo = new XxrFormulaSetsResPo();

				String podName = cloudMetaDataRepository.getValueById(x.getPodId());
				String projectName = cloudMetaDataRepository.getValueById(x.getProjectId());
				String objectCode = cloudMetaDataRepository.getValueById(x.getObjectId());
				String parentObjectCode = cloudMetaDataRepository.getValueById(x.getParentObjectId());

				xxrFormulaSetsResPo.setFormulaSetId(x.getFormulaSetId());
				xxrFormulaSetsResPo.setFormulaSetName(x.getFormulaSetName());
				if (!Validations.isNullOrEmpty(x.getFormulaType()))
					xxrFormulaSetsResPo.setFormulaType(x.getFormulaType());
				if (!Validations.isNullOrEmpty(x.getFormulaValue()))
					xxrFormulaSetsResPo.setFormulaValue(x.getFormulaValue());
				xxrFormulaSetsResPo.setObjectCode(objectCode);
				xxrFormulaSetsResPo.setObjectId(x.getObjectId());
				xxrFormulaSetsResPo.setParentObjectId(x.getParentObjectId());
				xxrFormulaSetsResPo.setParentObjectCode(parentObjectCode);
				xxrFormulaSetsResPo.setProjectId(x.getProjectId());
				xxrFormulaSetsResPo.setProjectName(projectName);
				xxrFormulaSetsResPo.setPodId(x.getPodId());
				xxrFormulaSetsResPo.setPodName(podName);
				if (!Validations.isNullOrEmpty(x.getDescription()))
					xxrFormulaSetsResPo.setDescription(x.getDescription());
				/*
				 * if (!Validations.isNullOrEmpty(x.getBu()))
				 * xxrFormulaSetsResPo.setBu(x.getBu());
				 */
				if (!Validations.isNullOrEmpty(x.getCloudColumn()))
					xxrFormulaSetsResPo.setCloudColumn(x.getCloudColumn());

				formulaSetResList.add(xxrFormulaSetsResPo);
			});
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return formulaSetResList;
	}

	@Override
	public SaveFormulaSetHeadersResPo saveFormulaSetHeaders(List<SaveFormulaSetHeaderPo> saveFormulaSetHeaderPo,
			HttpServletRequest request) throws BadRequestException, ValidationException, Exception {
		log.info("Start Of Method saveFormulaSetHeaders ####");
		String msg = "";
		Long formulaSetId;
		SaveFormulaSetHeadersResPo saveFormulaSetHeadersResPo = new SaveFormulaSetHeadersResPo();
		try {
			msg = saveFormulaSetHeaderDaoImpl.saveFormulaSetHeaders(saveFormulaSetHeaderPo, request);
			saveFormulaSetHeadersResPo.setMessage(msg);

			SaveFormulaSetHeaderPo saveFormulaSetHeader = saveFormulaSetHeaderPo.get(0);
			String formulaSetName = saveFormulaSetHeader.getFormulaSetName();

			if (!Validations.isNullOrEmpty(formulaSetName)) {
				formulaSetId = xxrFormulaSetsRepository.getFormulaSetId(formulaSetName);
				saveFormulaSetHeadersResPo.setFormulaSetId(formulaSetId);

			}
		} catch (ValidationException e) {
			throw new ValidationException(e.getMessage());
		} catch (BadRequestException e) {
			throw new BadRequestException(e.getMessage());
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}

		return saveFormulaSetHeadersResPo;

	}

	/*
	 * @Override public List<XxrFormulaSetsResPo> getFormulaSets() throws Exception
	 * { 
	 * log.info("Start Of Method getFormulaSets ####"); List<XxrFormulaSets>
	 * formulaSetsList = new ArrayList<>(); List<XxrFormulaSetsResPo>
	 * formulaSetResList = new ArrayList<>(); try { formulaSetsList =
	 * xxrFormulaSetsRepository.findAll(); formulaSetsList.stream().forEach(x -> {
	 * XxrFormulaSetsResPo xxrFormulaSetsResPo = new XxrFormulaSetsResPo();
	 * 
	 * String podName = cloudMetaDataRepository.getValueById(x.getPodId()); String
	 * projectName = cloudMetaDataRepository.getValueById(x.getProjectId()); String
	 * objectCode = cloudMetaDataRepository.getValueById(x.getObjectId()); String
	 * parentObjectCode =
	 * cloudMetaDataRepository.getValueById(x.getParentObjectId());
	 * 
	 * xxrFormulaSetsResPo.setFormulaSetId(x.getFormulaSetId());
	 * xxrFormulaSetsResPo.setFormulaSetName(x.getFormulaSetName()); if
	 * (!Validations.isNullOrEmpty(x.getFormulaType()))
	 * xxrFormulaSetsResPo.setFormulaType(x.getFormulaType()); if
	 * (!Validations.isNullOrEmpty(x.getFormulaValue()))
	 * xxrFormulaSetsResPo.setFormulaValue(x.getFormulaValue());
	 * xxrFormulaSetsResPo.setObjectCode(objectCode);
	 * xxrFormulaSetsResPo.setObjectId(x.getObjectId());
	 * xxrFormulaSetsResPo.setParentObjectId(x.getParentObjectId());
	 * xxrFormulaSetsResPo.setParentObjectCode(parentObjectCode);
	 * xxrFormulaSetsResPo.setProjectId(x.getProjectId());
	 * xxrFormulaSetsResPo.setProjectName(projectName);
	 * xxrFormulaSetsResPo.setPodId(x.getPodId());
	 * xxrFormulaSetsResPo.setPodName(podName); if
	 * (!Validations.isNullOrEmpty(x.getDescription()))
	 * xxrFormulaSetsResPo.setDescription(x.getDescription()); if
	 * (!Validations.isNullOrEmpty(x.getSqlQuery()))
	 * xxrFormulaSetsResPo.setSqlQuery(x.getSqlQuery());
	 * 
	 * if (!Validations.isNullOrEmpty(x.getBu()))
	 * xxrFormulaSetsResPo.setBu(x.getBu());
	 * 
	 * if (!Validations.isNullOrEmpty(x.getCloudColumn()))
	 * xxrFormulaSetsResPo.setCloudColumn(x.getCloudColumn());
	 * 
	 * formulaSetResList.add(xxrFormulaSetsResPo); }); } catch (Exception e) { throw
	 * new Exception(e.getMessage()); } return formulaSetResList; }
	 */

	@Override
	public List<XxrFormulaSetTablesResPo> getFormulaSetTables(Long formulaSetId) throws Exception {
		log.info("Start Of Method getFormulaSetTables service ####");
		List<XxrFormulaSetTablesResPo> xxrFormulaSetTablesRes = new ArrayList<>();
		try {
			xxrFormulaSetTablesRes = xxrFormulaTablesRepository.findByformulaSetId(formulaSetId);
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return xxrFormulaSetTablesRes;
	}

	@Override
	public SaveFormulaSetTablesResPo saveFormulaSetTables(List<SaveFormulaSetTablesPo> saveFormulaSetTablesPo,
			HttpServletRequest request) throws ValidationException, BadRequestException, Exception {
		log.info("Start Of Method saveFormulaSetTables service ####");
		String msg = "";
		List<XxrFormulaSetTablesResPo> xxrFormulaSetTablesRes = new ArrayList<>();
		SaveFormulaSetTablesResPo saveFormulaSetTablesResPo = new SaveFormulaSetTablesResPo();
		try {
			msg = saveFormulaSetTablesDaoImpl.saveFormulaSetTables(saveFormulaSetTablesPo, request);
			saveFormulaSetTablesResPo.setMessage(msg);
			Long formulaSetId = saveFormulaSetTablesPo.get(0).getFormulaSetId();
			if (formulaSetId != null) {
				xxrFormulaSetTablesRes = xxrFormulaTablesRepository.findByformulaSetId(formulaSetId);
				saveFormulaSetTablesResPo.setFormulaSetTables(xxrFormulaSetTablesRes);
			}
			log.info(msg + "msg###########");
		} catch (BadRequestException e) {
			throw new BadRequestException(e.getMessage());
		} catch (ValidationException e) {
			throw new ValidationException(e.getMessage());
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}

		return saveFormulaSetTablesResPo;

	}

	@Override
	public List<XxrFormulaColumnsResPo> getFormulaColumns(Long formulaSetId, Long formulaTableId) throws Exception {
		log.info("Start Of Method getFormulaColumns service ####");
		List<XxrFormulaColumnsResPo> xxrFormulaColumnsRes = new ArrayList<>();
		try {
			xxrFormulaColumnsRes = xxrFormulaColumnsRepository.getFormulaColumns(formulaSetId, formulaTableId);
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return xxrFormulaColumnsRes;
	}

	@Override
	public SaveFormulaColumnsResPo saveFormulaColumns(List<SaveFormulaColumnsPo> saveFormulaColumnsPo,
			HttpServletRequest request) throws BadRequestException, ValidationException, Exception {
		log.info("Start Of Method saveFormulaColumns service ####");
		String msg = "";
		List<XxrFormulaColumnsResPo> xxrFormulaColumnsRes = new ArrayList<>();
		SaveFormulaColumnsResPo saveFormulaColumnsResPo = new SaveFormulaColumnsResPo();
		try {
			msg = saveFormulaColumnsDaoImpl.saveFormulaColumns(saveFormulaColumnsPo, request);
			saveFormulaColumnsResPo.setMessage(msg);
			Long formulaSetId = saveFormulaColumnsPo.get(0).getFormulaSetId();
			Long formulaTableId = saveFormulaColumnsPo.get(0).getFormulaTableId();
			if (formulaSetId != null && formulaTableId != null) {
				xxrFormulaColumnsRes = xxrFormulaColumnsRepository.getFormulaColumns(formulaSetId, formulaTableId);
				saveFormulaColumnsResPo.setFormulaColumns(xxrFormulaColumnsRes);
			}
			log.info(msg + "msg###########");
		} catch (ValidationException e) {
			throw new ValidationException(e.getMessage());
		} catch (BadRequestException e) {
			throw new BadRequestException(e.getMessage());
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}

		return saveFormulaColumnsResPo;
	}

	@Override
	public List<SourceTablesPo> getSourceTableNames(Long objectId) throws Exception {
		log.info("Start Of getSourceTableNames Method in Service Layer ####");
		List<SourceTablesPo> sourceTableNames = new ArrayList<>();
		try {

			sourceTableNames = xxrSourceTablesRepository.getTableIdNames(objectId);

		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}

		return sourceTableNames;

	}

	@Override
	public List<SourceColumnsPo> getSourceColumns(Long sourceTableId) throws Exception {
		log.info("Start Of getSourceColumns Method  in Service Layer ####");
		List<SourceColumnsPo> sourceTemplateColumns = new ArrayList<>();

		try {
			sourceTemplateColumns = xxrSourceColumnsRepository.getSourceColumns(sourceTableId);
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}

		return sourceTemplateColumns;
	}

	@Override
	public List<CloudColumnsPo> getCloudColumns(Long objectId) throws Exception {
		log.info("Start Of getCloudColumns Method in Service Layer ####");
		List<CloudColumnsPo> cloudColumnsList = new ArrayList<>();
		List<Object[]> cloudColumnsObjectLi = new ArrayList<>();
		try {

			cloudColumnsList = xxrCloudColumnsRepository.getCloudColumns(objectId);
			cloudColumnsList = cloudColumnsList.stream().filter(Utils.distinctByKey(CloudColumnsPo::getColumnName))
					.collect(Collectors.toList());

			/*
			 * cloudColumnsObjectLi = xxrCloudColumnsRepository.getCloudColumns(objectId);
			 * cloudColumnsObjectLi.stream().forEach(x -> { CloudColumnsPo cloudColumnsPo =
			 * new CloudColumnsPo(Long.parseLong(x[0].toString()), x[1].toString());
			 * cloudColumnsList.add(cloudColumnsPo); });
			 */

		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}

		return cloudColumnsList;
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
	public SaveFormulaSetHeadersResPo copyFormulaSet(String newFormulaSetName, Long formulaSetId, Long podId,
			HttpServletRequest request) throws Exception {
		log.info("Start Of copyFormulaSet Method in Service Layer ####");
		List<SaveFormulaSetHeaderPo> saveFormulaSetList = new ArrayList<>();
		String msg = "";
		Long newFormulaSetId = null;
		SaveFormulaSetHeadersResPo saveFormulaSetHeadersResPo = new SaveFormulaSetHeadersResPo();
		// List<SaveFormulaSetTablesPo> saveFormulaSetTablesPo = new ArrayList<>();
		try {
			List<XxrFormulaSets> formulaSetList = xxrFormulaSetsRepository.findByformulaSetId(formulaSetId);

			formulaSetList.stream().forEach(x -> {
				SaveFormulaSetHeaderPo saveFormulaSetHeaderPo = new SaveFormulaSetHeaderPo();

				saveFormulaSetHeaderPo.setFormulaSetName(newFormulaSetName);
				saveFormulaSetHeaderPo.setPodId(podId);
				saveFormulaSetHeaderPo.setFormulaType(x.getFormulaType());
				saveFormulaSetHeaderPo.setFormulaValue(x.getFormulaValue());
				saveFormulaSetHeaderPo.setObjectId(x.getObjectId());
				saveFormulaSetHeaderPo.setParentObjectId(x.getParentObjectId());
				saveFormulaSetHeaderPo.setProjectId(x.getProjectId());
				saveFormulaSetHeaderPo.setCloudColumn(x.getCloudColumn());
				saveFormulaSetHeaderPo.setDescription(x.getDescription());
				saveFormulaSetHeaderPo.setSqlQuery(x.getSqlQuery());
				saveFormulaSetList.add(saveFormulaSetHeaderPo);
			});

			msg = saveFormulaSetHeaderDaoImpl.saveFormulaSetHeaders(saveFormulaSetList, request);
			log.info("FormulasetHeader :: " + msg);

			if (!Validations.isNullOrEmpty(newFormulaSetName))
				newFormulaSetId = xxrFormulaSetsRepository.getFormulaSetId(newFormulaSetName);

			saveFormulaSetHeadersResPo.setFormulaSetId(newFormulaSetId);
			saveFormulaSetHeadersResPo.setMessage("FormulaSet is Successfully copied");

			/*
			 * List<XxrFormulaSetTablesResPo> xxrFormulaSetTable =
			 * xxrFormulaTablesRepository .findByformulaSetId(formulaSetId);
			 * 
			 * for (XxrFormulaSetTablesResPo formulaSetTablesRes : xxrFormulaSetTable) {
			 * SaveFormulaSetTablesPo formulaSetTablesPo =new SaveFormulaSetTablesPo();
			 * 
			 * formulaSetTablesPo.setFormulaSetId(newFormulaSetId);
			 * formulaSetTablesPo.setSourceTableId(formulaSetTablesRes.getSourceTableId());
			 * formulaSetTablesPo.setSeq(formulaSetTablesRes.getSeq());
			 * formulaSetTablesPo.setDescription(formulaSetTablesRes.getDescription());
			 * 
			 * saveFormulaSetTablesPo.add(formulaSetTablesPo); }
			 * 
			 * msg =
			 * saveFormulaSetTablesDaoImpl.saveFormulaSetTables(saveFormulaSetTablesPo);
			 * log.info("FormulasetTables :: " + msg);
			 */

		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return saveFormulaSetHeadersResPo;
	}

	@Override
	public TestSqlSyntaxResPo testSqlSyntax(String sqlQuery) throws Exception {
		log.info("Start of testSqlSyntax Method ##");
		TestSqlSyntaxResPo testSqlSyntaxResPo = new TestSqlSyntaxResPo();
		try {
			// msg=formulaSetDaoImpl.testSqlSyntax(sqlQuery);
			CCJSqlParserManager pm = new CCJSqlParserManager();
			pm.parse(new StringReader(sqlQuery));
			testSqlSyntaxResPo.setMessage("Valid Sql Syntax");
			testSqlSyntaxResPo.setValidSyntax(true);
		} catch (JSQLParserException e) {
			testSqlSyntaxResPo.setMessage("InValid Sql Syntax");
			testSqlSyntaxResPo.setValidSyntax(false);
			String msg = e.getCause().getMessage().replaceAll("\\n", "");
			testSqlSyntaxResPo.setError(msg);
			// testSqlSyntaxResPo.setError(e.getCause().getMessage());
			e.printStackTrace();
			// String [] errorMsg=e.getCause().getMessage().split("\n");
			// log.info("NOT VALID SQL: " +errorMsg[0]+errorMsg[1]);
			log.error("NOT VALID SQL: " + msg);
		} catch (Exception e) {
			// e.printStackTrace();
			throw new Exception(e.getMessage());
		}
		return testSqlSyntaxResPo;
	}

	@Override
	public TestDataResPo testSqlData(String sqlQuery) throws Exception {
		log.info("Start of testSqlData Method ##");
		TestDataResPo testDataResPo = new TestDataResPo();
		try {
			testDataResPo = formulaSetDaoImpl.testSqlData(sqlQuery);
		} catch (Exception e) {
			// e.printStackTrace();
			throw new Exception(e.getMessage());
		}
		return testDataResPo;
	}

	@Override
	public List<XxrFormulaSetsResPo> getFormulaSetsByobjectId(Long objectId) throws Exception {
		log.info("Start Of Method getFormulaSetsByobjectId ####");
		List<XxrFormulaSets> formulaSetsList = new ArrayList<>();
		List<XxrFormulaSetsResPo> formulaSetResList = new ArrayList<>();
		try {
			formulaSetsList = xxrFormulaSetsRepository.findByobjectId(objectId);
			formulaSetsList.stream().forEach(x -> {
				XxrFormulaSetsResPo xxrFormulaSetsResPo = new XxrFormulaSetsResPo();

				String podName = cloudMetaDataRepository.getValueById(x.getPodId());
				String projectName = cloudMetaDataRepository.getValueById(x.getProjectId());
				String objectCode = cloudMetaDataRepository.getValueById(x.getObjectId());
				String parentObjectCode = cloudMetaDataRepository.getValueById(x.getParentObjectId());

				xxrFormulaSetsResPo.setFormulaSetId(x.getFormulaSetId());
				xxrFormulaSetsResPo.setFormulaSetName(x.getFormulaSetName());
				if (!Validations.isNullOrEmpty(x.getFormulaType()))
					xxrFormulaSetsResPo.setFormulaType(x.getFormulaType());
				if (!Validations.isNullOrEmpty(x.getFormulaValue()))
					xxrFormulaSetsResPo.setFormulaValue(x.getFormulaValue());
				xxrFormulaSetsResPo.setObjectCode(objectCode);
				xxrFormulaSetsResPo.setObjectId(x.getObjectId());
				xxrFormulaSetsResPo.setParentObjectId(x.getParentObjectId());
				xxrFormulaSetsResPo.setParentObjectCode(parentObjectCode);
				xxrFormulaSetsResPo.setProjectId(x.getProjectId());
				xxrFormulaSetsResPo.setProjectName(projectName);
				xxrFormulaSetsResPo.setPodId(x.getPodId());
				xxrFormulaSetsResPo.setPodName(podName);
				if (!Validations.isNullOrEmpty(x.getDescription()))
					xxrFormulaSetsResPo.setDescription(x.getDescription());
				if (!Validations.isNullOrEmpty(x.getSqlQuery()))
					xxrFormulaSetsResPo.setSqlQuery(x.getSqlQuery());
				/*
				 * if (!Validations.isNullOrEmpty(x.getBu()))
				 * xxrFormulaSetsResPo.setBu(x.getBu());
				 */
				if (!Validations.isNullOrEmpty(x.getCloudColumn()))
					xxrFormulaSetsResPo.setCloudColumn(x.getCloudColumn());

				formulaSetResList.add(xxrFormulaSetsResPo);
			});
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return formulaSetResList;
	}

	@Override
	public List<XxrFormulaSetsView> getFormulaSets(Long roleId) throws Exception {
		List<XxrFormulaSetsView> formulaSetLi = new ArrayList<>();
		formulaSetLi = xxrFormulaSetsViewRepository.findByRoleId(roleId);
		return formulaSetLi;
	}

}
