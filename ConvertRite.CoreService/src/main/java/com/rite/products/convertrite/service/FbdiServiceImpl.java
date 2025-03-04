package com.rite.products.convertrite.service;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.RichTextString;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.util.IOUtils;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import com.rite.products.convertrite.Validations.Validations;
import com.rite.products.convertrite.exception.BadRequestException;
import com.rite.products.convertrite.exception.ValidationException;
import com.rite.products.convertrite.model.FbdiTempHdrsView;
import com.rite.products.convertrite.model.XxrFbdiTempCols;
import com.rite.products.convertrite.model.XxrFbdiTempHdrs;
import com.rite.products.convertrite.po.CloudTablesPo;
import com.rite.products.convertrite.po.FbdiColumnSequencePo;
import com.rite.products.convertrite.po.SaveFbdiTemplateColumnsPo;
import com.rite.products.convertrite.po.SaveFbdiTemplateColumnsResPo;
import com.rite.products.convertrite.po.SaveFbdiTemplateHeaderPo;
import com.rite.products.convertrite.po.SaveTemplateHeaderResponsePo;
import com.rite.products.convertrite.respository.SaveFbdiTemplateColumnsDaoImpl;
import com.rite.products.convertrite.respository.SaveFbdiTemplateHeaderDaoImpl;
import com.rite.products.convertrite.respository.XxrCloudConfigRepository;
import com.rite.products.convertrite.respository.XxrCloudTableRepository;
import com.rite.products.convertrite.respository.XxrFbdiTempColsRepository;
import com.rite.products.convertrite.respository.XxrFbdiTempHdrsRepository;
import com.rite.products.convertrite.respository.XxrFbdiTempHdrsViewRepository;
import com.rite.products.convertrite.respository.XxrLookUpValuesRepository;
import com.rite.products.convertrite.utils.ControlFileParser;
import com.rite.products.convertrite.utils.Utils;

@Service
public class FbdiServiceImpl implements FbdiService {

	private static final Logger log = LoggerFactory.getLogger(FbdiServiceImpl.class);

	@Autowired
	XxrFbdiTempHdrsRepository xxrFbdiTempHdrsRepository;
	@Autowired
	XxrLookUpValuesRepository xxrLookUpValuesRepository;
	@Autowired
	XxrCloudTableRepository XxrCloudTableRepository;
	@Autowired
	XxrFbdiTempColsRepository xxrFbdiTempColsRepository;
	@Autowired
	SaveFbdiTemplateHeaderDaoImpl saveFbdiTemplateHeaderDaoImpl;
	@Autowired
	SaveFbdiTemplateColumnsDaoImpl saveFbdiTemplateColumnsDaoImpl;
	@Autowired
	Utils utils;
	@Autowired
	XxrCloudConfigRepository xxrCloudConfigRepository;
	@Autowired
	XxrFbdiTempHdrsViewRepository xxrFbdiTempHdrsViewRepository;

	/*
	 * @Value("${firstpartof-excel-download-url}") private String excelUrlFirstPart;
	 * 
	 * @Value("${secondpartof-excel-download-url}") private String
	 * excelUrlSecondPart;
	 */
	@Value("${firstpartof-ctl-download-url}")
	private String ctlUrlFirstPart;
	@Value("${secondpartof-ctl-download-url}")
	private String ctlUrlSecondPart;

	@Override
	public List<FbdiColumnSequencePo> getFbdiColumnSequence(String fileName, String version, HttpServletResponse resp)
			throws Exception {
		log.info("Start of getFbdiColumnSequence in service ###");
		// LinkedHashMap<String, Integer> columnSequence = new LinkedHashMap<>();
		List<FbdiColumnSequencePo> columnSequencePo = new ArrayList<>();
		List<String> columnNames = new ArrayList<>();
		try {
			Path path = downloadFbdiTemplateFromServer(fileName, version, resp);
//			log.info(path + ":::::::ctlfilepath");
			ControlFileParser ctlFileParser = new ControlFileParser(path.toString() + File.separator + fileName);
			int sequence = 10;
			columnNames = ctlFileParser.getColumnNames();
			for (String columnName : columnNames) {
//				log.info("columnName######3" + columnName);
				FbdiColumnSequencePo fbdiColumnSequencePo = new FbdiColumnSequencePo();
				fbdiColumnSequencePo.setDatabaseColumn(columnName);
				fbdiColumnSequencePo.setSequence(sequence);
				columnSequencePo.add(fbdiColumnSequencePo);
				sequence += 10;
			}

		} catch (Exception e) {
			// e.printStackTrace();
			throw new Exception(e.getMessage());
		}
		return columnSequencePo;
	}

	/*
	 * @Override public List<FbdiColumnSequencePo> getFbdiColumnSequence(String
	 * excelFileName, String version, String sheetName,HttpServletResponse resp)
	 * throws Exception { 
	 * log.info("Start of getFbdiColumnSequence in service ###");
	 * //LinkedHashMap<String, Integer> columnSequence = new LinkedHashMap<>();
	 * List<FbdiColumnSequencePo> columnSequencePo = new ArrayList<>(); Row row =
	 * null; try { Path path = downloadFbdiTemplateFromServer(excelFileName,
	 * version,resp); row = excelReading(excelFileName, sheetName, 1, 3, path); int
	 * lastColumn = row.getLastCellNum(); int sequence = 10; for (int columnNum = 0;
	 * columnNum < lastColumn; columnNum++) { FbdiColumnSequencePo
	 * fbdiColumnSequencePo = new FbdiColumnSequencePo(); String commentValue = "";
	 * 
	 * Cell cell = row.getCell(columnNum);
	 * //columnSequence.put(cell.getStringCellValue(), sequence); RichTextString
	 * comment = cell.getCellComment().getString(); String commentString =
	 * comment.toString(); String [] commentArr=commentString.split("\\r?\\n");
	 * String columnNameString=commentArr[0];
	 * columnNameString=columnNameString.replaceAll(" ", "");
	 * if(columnNameString.indexOf("ColumnName:")!=-1) {
	 * commentValue=columnNameString.substring(columnNameString.indexOf(
	 * "ColumnName:")+11).trim(); } else {
	 * //System.out.println("entering elseeee"+commentArr[1]+"222"+commentArr[2]);
	 * String columnNameStr=commentArr[2];
	 * columnNameStr=columnNameStr.replaceAll(" ", "");
	 * if(columnNameStr.indexOf("ColumnName:")!=-1)
	 * commentValue=columnNameStr.substring(columnNameStr.indexOf("ColumnName:")+11)
	 * .trim(); } //System.out.println("commentValue#3333333"+commentValue); if
	 * (commentString.contains("Data Type")) commentValue = commentString
	 * .substring(commentString.indexOf("Column Name:") + 12,
	 * commentString.indexOf("Data Type:")) .replaceAll("\\n", ""); else if
	 * (commentString.contains("Data Type")) commentValue = commentString
	 * .substring(commentString.indexOf("Column Name:") + 12,
	 * commentString.indexOf("Data Type:")) .replaceAll("\\n", ""); String
	 * fbdiColumnName=cell.getStringCellValue();
	 * 
	 * fbdiColumnSequencePo.setFbdiColumnName(fbdiColumnName);
	 * if(fbdiColumnName.contains("*")) fbdiColumnSequencePo.setRequired("Y"); else
	 * fbdiColumnSequencePo.setRequired("N");
	 * fbdiColumnSequencePo.setSequence(sequence); if(commentValue.contains("*"))
	 * fbdiColumnSequencePo.setDatabaseColumn(commentValue.substring(0,(commentValue
	 * .length()-1))); else fbdiColumnSequencePo.setDatabaseColumn(commentValue);
	 * sequence = sequence + 10; columnSequencePo.add(fbdiColumnSequencePo); } }
	 * catch (Exception e) { e.printStackTrace(); throw new Exception(); } return
	 * columnSequencePo; }
	 * 
	 * private Row excelReading(String excelFileName, String sheetName, int sheetNo,
	 * int rowNo, Path path) throws Exception {
	 * log.info("Start of excelReading Method  ###"); XSSFWorkbook workbook = null;
	 * Row row = null; try { String tempPath = System.getProperty("java.io.tmpdir");
	 * System.out.println("tempPath:::" + tempPath); // tempPath = "C:\\temp";
	 * FileInputStream file = new FileInputStream(new File(path + File.separator +
	 * excelFileName)); // Create // Workbook // instance // holding // reference //
	 * to .xlsx // file workbook = new XSSFWorkbook(file); // Get second sheet from
	 * the workbook // XSSFSheet sheet = workbook.getSheetAt(1); XSSFSheet sheet =
	 * workbook.getSheet(sheetName); row = sheet.getRow(rowNo); } catch
	 * (FileNotFoundException e) { // TODO Auto-generated catch block
	 * e.printStackTrace(); throw new Exception(); } catch (Exception e) { throw new
	 * Exception(); } finally { if (workbook != null) workbook.close(); } return
	 * row; }
	 */

	private Path downloadFbdiTemplateFromServer(String fileName, String version, HttpServletResponse resp)
			throws Exception {
		log.info("Start of downloadFbdiTemplateFromServer Method ###");
		String url = "";
		Path target = null;
		try {
            String sb = ctlUrlFirstPart + version +
                    ctlUrlSecondPart +
                    fileName;
			url = sb;
			resp.setHeader("API", url);
			// create Temp Directory
			target = Files.createTempDirectory("");
			log.info("target:::::" + target);
			// website url
			URL website = new URL(url);
			File file = new File(target + File.separator + fileName);
			try (InputStream in = website.openStream(); OutputStream outputStream = new FileOutputStream(file)) {
				IOUtils.copy(in, outputStream);
			} catch (IOException e) {
				// e.printStackTrace();
				throw new Exception(e.getMessage());
			}
			// Files.copy(in, target, StandardCopyOption.REPLACE_EXISTING);
		} catch (FileNotFoundException e) {
			// e.printStackTrace();
			throw new Exception(e.getMessage());
		} catch (Exception e) {
			// e.printStackTrace();
			throw new Exception(e.getMessage());
		}
		return target;
	}

	/*
	 * @Override public List<XxrFbdiTempHdrsResp> getFbdiTemplates() throws
	 * Exception { log.info("Start of getFbdiTemplates Method in service ###");
	 * List<XxrFbdiTempHdrs> xxrFbdiTempHdrsList = new ArrayList<>();
	 * List<XxrFbdiTempHdrsResp> xxrFbdiTempHdrsRes = new ArrayList<>(); try {
	 * xxrFbdiTempHdrsList = xxrFbdiTempHdrsRepository.findAll();
	 * 
	 * xxrFbdiTempHdrsList.stream().forEach(x -> { XxrFbdiTempHdrsResp
	 * xxrFbdiTempHdrsResp = new XxrFbdiTempHdrsResp();
	 * 
	 * xxrFbdiTempHdrsResp.setFbdiTemplateId(x.getFbdiTemplateId());
	 * xxrFbdiTempHdrsResp.setFbdiTemplateName(x.getFbdiTemplateName());
	 * xxrFbdiTempHdrsResp.setObjectId(x.getObjectId());
	 * xxrFbdiTempHdrsResp.setPodId(x.getPodId());
	 * xxrFbdiTempHdrsResp.setProjectId(x.getProjectId());
	 * xxrFbdiTempHdrsResp.setParentObjectId(x.getParentObjectId()); String podName
	 * = xxrLookUpValuesRepository.getValueById(x.getPodId()); String projectName =
	 * xxrLookUpValuesRepository.getValueById(x.getProjectId()); String
	 * parentObjectCode =
	 * xxrLookUpValuesRepository.getValueById(x.getParentObjectId()); String
	 * objectCode = xxrLookUpValuesRepository.getValueById(x.getObjectId());
	 * xxrFbdiTempHdrsResp.setPodName(podName);
	 * xxrFbdiTempHdrsResp.setObjectCode(objectCode);
	 * xxrFbdiTempHdrsResp.setParentObjectCode(parentObjectCode);
	 * xxrFbdiTempHdrsResp.setProjectName(projectName);
	 * xxrFbdiTempHdrsResp.setVersion(x.getVersion());
	 * xxrFbdiTempHdrsResp.setSheetName(x.getSheetName());
	 * xxrFbdiTempHdrsResp.setApi(x.getApi());
	 * 
	 * xxrFbdiTempHdrsRes.add(xxrFbdiTempHdrsResp); });
	 * 
	 * } catch (Exception e) { throw new Exception(e.getMessage()); } return
	 * xxrFbdiTempHdrsRes; }
	 */

	@Override
	public String getFbdiTemplateName(String objectCode) throws Exception {
		log.info("Start of getFbdiTemplateName Method in service ###");
		String fbdiTemplateName = "";
		try {
			// fbdiTemplateName = utils.getFileName(objectCode);
			fbdiTemplateName = utils.getCtlFileName(objectCode);
		} catch (ValidationException e) {
			throw new ValidationException(e.getMessage());
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return fbdiTemplateName;
	}

	/*
	 * private String getFileName(String objectCode) { String fileName = "";
	 * Map<String, String> objectCodeFileNameMap = new HashMap<>();
	 * 
	 * objectCodeFileNameMap.put("supplier", "PozSuppliersInt");
	 * objectCodeFileNameMap.put("sites", "PozSupplierSitesInt");
	 * objectCodeFileNameMap.put("site assignments", "PozSiteAssignmentsInt");
	 * objectCodeFileNameMap.put("product and services", "PozSupProdServInt");
	 * objectCodeFileNameMap.put("contacts", "PozSupContactsInt");
	 * objectCodeFileNameMap.put("contact address", "PozSupContactAddressesInt");
	 * objectCodeFileNameMap.put("business classifications", "PozSupBusClassInt");
	 * objectCodeFileNameMap.put("attachments", "PozSupAttachmentsInt");
	 * objectCodeFileNameMap.put("site attachment", "PozSupSiteAttachmentsInt");
	 * objectCodeFileNameMap.put("business classification attachment",
	 * "PozSupBusClassAttachmentsInt"); objectCodeFileNameMap.put("address",
	 * "PozSupAddressesInt"); objectCodeFileNameMap.put("third party",
	 * "PozSupThirdPartyInt");
	 * 
	 * 
	 * 
	 * objectCodeFileNameMap.put("supplier", "Supplier");
	 * objectCodeFileNameMap.put("sites", "SupplierSite");
	 * objectCodeFileNameMap.put("site assignments", "SupplierSiteAssignment");
	 * objectCodeFileNameMap.put("product and services",
	 * "SupplierProductsandServicesCategory"); objectCodeFileNameMap.put("contacts",
	 * "SupplierContact"); objectCodeFileNameMap.put("contact address",
	 * "SupplierContact"); objectCodeFileNameMap.put("business classifications",
	 * "SupplierBusinessClassification"); objectCodeFileNameMap.put("attachments",
	 * "SupplierAttachment"); objectCodeFileNameMap.put("address",
	 * "SupplierAddress"); objectCodeFileNameMap.put("third party", "SupplierSite");
	 * 
	 * 
	 * fileName = objectCodeFileNameMap.get(objectCode.toLowerCase());
	 * 
	 * return fileName; }
	 */

	@Override
	public String getSheetName(Long objectId) throws Exception {
		log.info("Start of getSheetName Method in Service ######");
		String sheetName = "";
		List<CloudTablesPo> cloudTables = new ArrayList<>();
		try {
			if (objectId == 230)
				sheetName = "POZ_SUPPLIERS_INT";
			else {
				cloudTables = XxrCloudTableRepository.getTableIdNames(objectId);
				CloudTablesPo cloudTablesPo = cloudTables.get(0);
				sheetName = cloudTablesPo.getTableName();
			}
		} catch (Exception e) {
			e.getMessage();
		}
		return sheetName;
	}

	@Override
	public List<XxrFbdiTempCols> getFbdiTemplateColumns(Long fbdiTemplateId) throws Exception {
		log.info("Start of getFbdiTemplateColumns Method in Service ######");
		List<XxrFbdiTempCols> xxrFbdiTempCols = new ArrayList<>();
		try {
			xxrFbdiTempCols = xxrFbdiTempColsRepository.findByfbdiTemplateId(fbdiTemplateId);
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return xxrFbdiTempCols;
	}

	@Override
	public SaveTemplateHeaderResponsePo saveFbdiTemplateHdrs(List<SaveFbdiTemplateHeaderPo> fbdiTemplateHeaderPo,
			HttpServletRequest request) throws Exception {
		log.info("Start of saveFbdiTemplateHdrs in Service Layer ###");
		String msg = "";
		long templateId = 0;

		SaveTemplateHeaderResponsePo saveTemplateHeaderResponsePo = new SaveTemplateHeaderResponsePo();

		try {
			msg = saveFbdiTemplateHeaderDaoImpl.saveFbdiTemplateHdrs(fbdiTemplateHeaderPo, request);
			saveTemplateHeaderResponsePo.setMessage(msg);
			SaveFbdiTemplateHeaderPo saveFbdiTemplateHeaderPo = fbdiTemplateHeaderPo.get(0);
			String templateName = saveFbdiTemplateHeaderPo.getFbdiTemplateName();
			String version = saveFbdiTemplateHeaderPo.getVersion();
			String sheetName = saveFbdiTemplateHeaderPo.getSheetName();
			if (!Validations.isNullOrEmpty(templateName) && !Validations.isNullOrEmpty(version)
					&& !Validations.isNullOrEmpty(sheetName)) {
				templateId = xxrFbdiTempHdrsRepository.getFbdiTemplateId(templateName, version, sheetName,
						saveFbdiTemplateHeaderPo.getObjectId());
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

	@Override
	public SaveFbdiTemplateColumnsResPo saveFbdiTemplateColumns(List<SaveFbdiTemplateColumnsPo> fbdiTemplateColumnsPo,
			HttpServletRequest request) throws Exception {
		log.info("Start of SaveFbdiTemplateColumnsResPo in Service Layer ###");
		String msg = "";
		List<XxrFbdiTempCols> xxrFbdiTempCols = new ArrayList<>();

		SaveFbdiTemplateColumnsResPo saveFbdiTemplateColumnsResPo = new SaveFbdiTemplateColumnsResPo();
		try {
			msg = saveFbdiTemplateColumnsDaoImpl.saveFbdiTemplateColumns(fbdiTemplateColumnsPo, request);
			saveFbdiTemplateColumnsResPo.setMessage(msg);
			Long templateId = fbdiTemplateColumnsPo.get(0).getFbdiTemplateId();
			if (templateId != null) {
				xxrFbdiTempCols = xxrFbdiTempColsRepository.findByfbdiTemplateId(templateId);
				saveFbdiTemplateColumnsResPo.setFbdiTemplateColumns(xxrFbdiTempCols);
			}
			log.info(msg + "msg###########");
		} catch (BadRequestException e) {
			throw new BadRequestException(e.getMessage());
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}

		return saveFbdiTemplateColumnsResPo;
	}

	@Override
	public SaveTemplateHeaderResponsePo saveCtlAsBlob(Long objectId, String version) throws Exception {
		SaveTemplateHeaderResponsePo saveTemplateHeaderResponsePo = new SaveTemplateHeaderResponsePo();
		String url = "";
		byte[] bytes = null;
		try {
			XxrFbdiTempHdrs xxrFbdiTempHdrs = xxrFbdiTempHdrsRepository.findByObjectIdAndVersion(objectId, version);
			if (xxrFbdiTempHdrs != null) {

                String sb = ctlUrlFirstPart + version +
                        ctlUrlSecondPart +
                        xxrFbdiTempHdrs.getFbdiTemplateName() + ".ctl";
				url = sb;
				// website url
				URL website = new URL(url);
				try (InputStream in = website.openStream()) {
					bytes = IOUtils.toByteArray(in);
				} catch (IOException e) {
					// e.printStackTrace();
					throw new Exception(e.getMessage());
				}

				xxrFbdiTempHdrs.setCtlFileBlob(bytes);
				XxrFbdiTempHdrs xxrFbdiTempHdrRes = xxrFbdiTempHdrsRepository.save(xxrFbdiTempHdrs);
				saveTemplateHeaderResponsePo.setMessage("Updated FdbiHdr with blob successfully");
				saveTemplateHeaderResponsePo.setTemplateId(xxrFbdiTempHdrRes.getFbdiTemplateId());
				saveTemplateHeaderResponsePo.setTemplateName(xxrFbdiTempHdrRes.getFbdiTemplateName());
			} else {
				throw new Exception("There is no fbdi template with above objectId & Version combination");
			}
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return saveTemplateHeaderResponsePo;
	}

	@Override
	public List<FbdiColumnSequencePo> uploadCtlGetColumnSeq(MultipartFile file, String version, Long objectId)
			throws Exception {
		log.info("Start of uploadCtlGetColumnSeq #######");
		byte[] bytes = null;
		Path target = null;
		List<String> columnNames = new ArrayList<>();
		List<FbdiColumnSequencePo> columnSequencePo = new ArrayList<>();
		XxrFbdiTempHdrs xxrFbdiTempHdrs = xxrFbdiTempHdrsRepository.findByObjectIdAndVersion(objectId, version);
		if (xxrFbdiTempHdrs != null) {
			bytes = IOUtils.toByteArray(file.getInputStream());
			xxrFbdiTempHdrs.setCtlFileBlob(bytes);
			XxrFbdiTempHdrs xxrFbdiTempHdrRes = xxrFbdiTempHdrsRepository.save(xxrFbdiTempHdrs);
		}
		target = Files.createTempDirectory("");
		log.info("target:::::" + target);
		String fileName = org.apache.commons.io.FilenameUtils.getName(file.getOriginalFilename());
		// log.info(file.getName()+"######fileName");
		File tmpFile = new File(target + File.separator + fileName);
		try (InputStream in = file.getInputStream(); OutputStream outputStream = new FileOutputStream(tmpFile)) {
			IOUtils.copy(in, outputStream);
		} catch (IOException e) {
			// e.printStackTrace();
			throw new Exception(e.getMessage());
		}

		ControlFileParser ctlFileParser = new ControlFileParser(target + File.separator + fileName);
		int sequence = 10;
		columnNames = ctlFileParser.getColumnNames();
		for (String columnName : columnNames) {
//			log.info("columnName######3" + columnName);
			FbdiColumnSequencePo fbdiColumnSequencePo = new FbdiColumnSequencePo();
			fbdiColumnSequencePo.setDatabaseColumn(columnName);
			fbdiColumnSequencePo.setSequence(sequence);
			columnSequencePo.add(fbdiColumnSequencePo);
			sequence += 10;
		}
		return columnSequencePo;
	}

	@Override
	public List<FbdiColumnSequencePo> generateSapFbdiSeq(String objectCode) throws Exception {
		XSSFWorkbook wb = null;
		List<FbdiColumnSequencePo> li = new ArrayList<>();
		try {
			log.info("Start of generateSapFbdiSeq######");
			InputStream inputStream = this.getClass().getClassLoader()
					.getResourceAsStream("SAP_S4_HANA_Template_File.xlsx");
			wb = new XSSFWorkbook(inputStream);
			XSSFSheet sheet = wb.getSheet(objectCode);
			Row row = sheet.getRow(7);
			int lastColumn = row.getLastCellNum();
			int sequence = 10;
			for (int columnNum = 0; columnNum < lastColumn; columnNum++) {
				FbdiColumnSequencePo fbdiColumnSequencePo = new FbdiColumnSequencePo();
				Cell cell = row.getCell(columnNum);
				RichTextString comment = cell.getRichStringCellValue();
				String commentString = comment.toString();
				String[] commentArr = commentString.split("\\r?\\n");

				fbdiColumnSequencePo.setFbdiColumnName(commentArr[0]);
				fbdiColumnSequencePo.setSequence(sequence);
				if (fbdiColumnSequencePo.getFbdiColumnName().contains("*"))
					fbdiColumnSequencePo.setRequired("Y");
				else
					fbdiColumnSequencePo.setRequired("N");

				String databaseColumn = fbdiColumnSequencePo.getFbdiColumnName().replaceAll("[^a-zA-Z0-9\\s]", "");
				databaseColumn = databaseColumn.replaceAll("\\s+", "_").toUpperCase();
				fbdiColumnSequencePo.setDatabaseColumn(databaseColumn);
				sequence += 10;
				li.add(fbdiColumnSequencePo);
			}
		} finally {
			if (wb != null)
				wb.close();
		}
		return li;
	}

	@Override
	public List<FbdiTempHdrsView> getFbdiTemplates(Long roleId) throws Exception {
		log.info("Start of getFbdiTemplates in Service ####");
		List<FbdiTempHdrsView> fbdiTempHdrsLi = new ArrayList<>();
		fbdiTempHdrsLi = xxrFbdiTempHdrsViewRepository.findByRoleId(roleId);
		return fbdiTempHdrsLi;
	}

	@Override
	public SaveTemplateHeaderResponsePo saveFbdiTemplateHdrsJpa(SaveFbdiTemplateHeaderPo saveFbdiTemplateHeaderPo,
			HttpServletRequest request) throws ValidationException {
		log.info("Start of saveFbdiTemplateHdrsJpa in Service ####");
		SaveTemplateHeaderResponsePo saveTemplateHeaderResponsePo = new SaveTemplateHeaderResponsePo();
		XxrFbdiTempHdrs xxrFbdiTempHdrsRes = new XxrFbdiTempHdrs();
		if (saveFbdiTemplateHeaderPo.getFbdiTemplateId() != null) {
			Optional<XxrFbdiTempHdrs> xxrFbdiTemphdr = xxrFbdiTempHdrsRepository
					.findById(saveFbdiTemplateHeaderPo.getFbdiTemplateId());
			if (!xxrFbdiTemphdr.isPresent()) {
				throw new ValidationException("There is no FbdiTemplate with this TemplateId");
			}

			XxrFbdiTempHdrs fbdiTempHdrs = xxrFbdiTemphdr.get();
			fbdiTempHdrs.setFbdiTemplateName(saveFbdiTemplateHeaderPo.getFbdiTemplateName());
			fbdiTempHdrs.setPodId(saveFbdiTemplateHeaderPo.getPodId());
			fbdiTempHdrs.setProjectId(saveFbdiTemplateHeaderPo.getProjectId());
			fbdiTempHdrs.setParentObjectId(saveFbdiTemplateHeaderPo.getParentObjectId());
			fbdiTempHdrs.setObjectId(saveFbdiTemplateHeaderPo.getObjectId());
			fbdiTempHdrs.setApi(saveFbdiTemplateHeaderPo.getApi());
			fbdiTempHdrs.setSheetName(saveFbdiTemplateHeaderPo.getSheetName());
			fbdiTempHdrs.setVersion(saveFbdiTemplateHeaderPo.getVersion());
			fbdiTempHdrs.setLastUpdatedBy("ConvertRite");
			fbdiTempHdrs.setLastUpdatedDate(new java.sql.Date(new java.util.Date().getTime()));
			fbdiTempHdrs.setAttribute1(saveFbdiTemplateHeaderPo.getAttribute1());
			fbdiTempHdrs.setAttribute2(saveFbdiTemplateHeaderPo.getAttribute2());
			fbdiTempHdrs.setAttribute3(saveFbdiTemplateHeaderPo.getAttribute3());
			fbdiTempHdrs.setAttribute4(saveFbdiTemplateHeaderPo.getAttribute4());
			fbdiTempHdrs.setAttribute5(saveFbdiTemplateHeaderPo.getAttribute5());

			xxrFbdiTempHdrsRes = xxrFbdiTempHdrsRepository.save(fbdiTempHdrs);
		} else {

			XxrFbdiTempHdrs fbdiTempHdrs = xxrFbdiTempHdrsRepository
					.findByFbdiTemplateName(saveFbdiTemplateHeaderPo.getFbdiTemplateName());
			if (fbdiTempHdrs != null)
				throw new ValidationException("This Fbdi TemplateName already exists ");

			XxrFbdiTempHdrs xxrFbdiTempHdrs = new XxrFbdiTempHdrs();
			xxrFbdiTempHdrs.setFbdiTemplateName(saveFbdiTemplateHeaderPo.getFbdiTemplateName());
			xxrFbdiTempHdrs.setPodId(saveFbdiTemplateHeaderPo.getPodId());
			xxrFbdiTempHdrs.setProjectId(saveFbdiTemplateHeaderPo.getProjectId());
			xxrFbdiTempHdrs.setParentObjectId(saveFbdiTemplateHeaderPo.getParentObjectId());
			xxrFbdiTempHdrs.setObjectId(saveFbdiTemplateHeaderPo.getObjectId());
			xxrFbdiTempHdrs.setApi(saveFbdiTemplateHeaderPo.getApi());
			xxrFbdiTempHdrs.setSheetName(saveFbdiTemplateHeaderPo.getSheetName());
			xxrFbdiTempHdrs.setVersion(saveFbdiTemplateHeaderPo.getVersion());
			xxrFbdiTempHdrs.setCreatedBy("ConvertRite");
			xxrFbdiTempHdrs.setCreationDate(new java.sql.Date(new java.util.Date().getTime()));
			xxrFbdiTempHdrs.setLastUpdatedBy("ConvertRite");
			xxrFbdiTempHdrs.setLastUpdatedDate(new java.sql.Date(new java.util.Date().getTime()));
			xxrFbdiTempHdrs.setAttribute1(saveFbdiTemplateHeaderPo.getAttribute1());
			xxrFbdiTempHdrs.setAttribute2(saveFbdiTemplateHeaderPo.getAttribute2());
			xxrFbdiTempHdrs.setAttribute3(saveFbdiTemplateHeaderPo.getAttribute3());
			xxrFbdiTempHdrs.setAttribute4(saveFbdiTemplateHeaderPo.getAttribute4());
			xxrFbdiTempHdrs.setAttribute5(saveFbdiTemplateHeaderPo.getAttribute5());

			xxrFbdiTempHdrsRes = xxrFbdiTempHdrsRepository.save(xxrFbdiTempHdrs);
		}

		saveTemplateHeaderResponsePo.setTemplateId(xxrFbdiTempHdrsRes.getFbdiTemplateId());
		saveTemplateHeaderResponsePo.setTemplateName(xxrFbdiTempHdrsRes.getFbdiTemplateName());
		saveTemplateHeaderResponsePo.setMessage("Successfully Saved/Updated FBDITemplateHdrs");
		return saveTemplateHeaderResponsePo;
	}

	@Override
	public SaveFbdiTemplateColumnsResPo saveFbdiTemplateColumnsWithJpa(
			List<SaveFbdiTemplateColumnsPo> fbdiTemplateColumnsPo, HttpServletRequest request)
			throws BadRequestException {
		log.info("Start of saveFbdiTemplateColumnsWithJpa #######");
		SaveFbdiTemplateColumnsResPo saveFbdiTemplateColumnsResPo = new SaveFbdiTemplateColumnsResPo();
		List<XxrFbdiTempCols> fbdiTempColsLi = new ArrayList<>();
		List<XxrFbdiTempCols> fbdiTempColsResLi = new ArrayList<>();

		for (SaveFbdiTemplateColumnsPo saveFbdiTemplateColumnsPo : fbdiTemplateColumnsPo) {

			if (Validations.isNullOrEmpty(saveFbdiTemplateColumnsPo.getDatabaseColumn())
					|| saveFbdiTemplateColumnsPo.getFbdiTemplateId() == null)
				throw new BadRequestException("fbdiTemplateId and columnName are Mandatory fields");
			if (saveFbdiTemplateColumnsPo.getFbdiColumnId() == null) {
				XxrFbdiTempCols xxrFbdiTempCols = new XxrFbdiTempCols();
				xxrFbdiTempCols.setFbdiTemplateId(saveFbdiTemplateColumnsPo.getFbdiTemplateId());
				xxrFbdiTempCols.setActiveFlag(saveFbdiTemplateColumnsPo.getActiveFlag());
				xxrFbdiTempCols.setDatabaseColumn(saveFbdiTemplateColumnsPo.getDatabaseColumn());
				xxrFbdiTempCols.setFbdiColumnName(saveFbdiTemplateColumnsPo.getFbdiColumnName());
				xxrFbdiTempCols.setDatabaseTable(saveFbdiTemplateColumnsPo.getDatabaseTable());
				xxrFbdiTempCols.setStartDate(saveFbdiTemplateColumnsPo.getStartDate());
				xxrFbdiTempCols.setEndDate(saveFbdiTemplateColumnsPo.getEndDate());
				xxrFbdiTempCols.setRequired(saveFbdiTemplateColumnsPo.getRequired());
				xxrFbdiTempCols.setSequence(saveFbdiTemplateColumnsPo.getSequence());
				xxrFbdiTempCols.setCreatedBy("ConvertRite");
				xxrFbdiTempCols.setCreationDate(new java.sql.Date(new java.util.Date().getTime()));
				xxrFbdiTempCols.setLastUpdatedBy("ConvertRite");
				xxrFbdiTempCols.setLastUpdatedDate(new java.sql.Date(new java.util.Date().getTime()));
				fbdiTempColsLi.add(xxrFbdiTempCols);
			} else {
				XxrFbdiTempCols xxrFbdiTempCols = xxrFbdiTempColsRepository.findByFbdiTemplateIdAndFbdiColumnId(
						saveFbdiTemplateColumnsPo.getFbdiTemplateId(), saveFbdiTemplateColumnsPo.getFbdiColumnId());
				xxrFbdiTempCols.setFbdiTemplateId(saveFbdiTemplateColumnsPo.getFbdiTemplateId());
				xxrFbdiTempCols.setActiveFlag(saveFbdiTemplateColumnsPo.getActiveFlag());
				xxrFbdiTempCols.setDatabaseColumn(saveFbdiTemplateColumnsPo.getDatabaseColumn());
				xxrFbdiTempCols.setFbdiColumnName(saveFbdiTemplateColumnsPo.getFbdiColumnName());
				xxrFbdiTempCols.setDatabaseTable(saveFbdiTemplateColumnsPo.getDatabaseTable());
				xxrFbdiTempCols.setStartDate(saveFbdiTemplateColumnsPo.getStartDate());
				xxrFbdiTempCols.setEndDate(saveFbdiTemplateColumnsPo.getEndDate());
				xxrFbdiTempCols.setRequired(saveFbdiTemplateColumnsPo.getRequired());
				xxrFbdiTempCols.setSequence(saveFbdiTemplateColumnsPo.getSequence());
				xxrFbdiTempCols.setLastUpdatedBy("ConvertRite");
				xxrFbdiTempCols.setLastUpdatedDate(new java.sql.Date(new java.util.Date().getTime()));
				fbdiTempColsLi.add(xxrFbdiTempCols);
			}
		}
		
		fbdiTempColsResLi=xxrFbdiTempColsRepository.saveAll(fbdiTempColsLi);
		
		saveFbdiTemplateColumnsResPo.setFbdiTemplateColumns(fbdiTempColsResLi);
		saveFbdiTemplateColumnsResPo.setMessage("Successfully Saved/Updated FbdiTemplateColumns");
		return saveFbdiTemplateColumnsResPo;
	}

}
