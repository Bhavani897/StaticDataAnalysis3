package com.rite.products.convertrite.service;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.http.HttpServletResponse;
import javax.xml.bind.ValidationException;

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
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Service;

import com.opencsv.CSVWriter;
import com.rite.products.convertrite.exception.BadRequestException;
import com.rite.products.convertrite.model.XxrCloudConfig;
import com.rite.products.convertrite.model.XxrXlsmTempCols;
import com.rite.products.convertrite.model.XxrXlsmTempHdrs;
import com.rite.products.convertrite.po.FbdiColumnSequencePo;
import com.rite.products.convertrite.po.SaveTemplateHeaderResponsePo;
import com.rite.products.convertrite.po.SaveXlsmTemplateColumnsPo;
import com.rite.products.convertrite.po.SaveXlsmTemplateColumnsResPo;
import com.rite.products.convertrite.po.SaveXlsmTemplateHeaderPo;
import com.rite.products.convertrite.po.XxrXlsmTempHdrsResp;
import com.rite.products.convertrite.respository.XxrCloudConfigRepository;
import com.rite.products.convertrite.respository.XxrLookUpValuesRepository;
import com.rite.products.convertrite.respository.XxrXlsmTempColsRepository;
import com.rite.products.convertrite.respository.XxrXlsmTempHdrsRepository;
import com.rite.products.convertrite.utils.Utils;

@Service
public class XlsmServiceImpl implements XlsmService {

	private static final Logger log = LoggerFactory.getLogger(XlsmServiceImpl.class);

	@Value("${firstpartof-excel-download-url}")
	private String excelUrlFirstPart;

	@Value("${secondpartof-excel-download-url}")
	private String excelUrlSecondPart;

	@Autowired
	XxrLookUpValuesRepository xxrLookUpValuesRepository;
	@Autowired
	XxrXlsmTempHdrsRepository xxrXlsmTempHdrsRepository;
	@Autowired
	XxrXlsmTempColsRepository xxrXlsmTempColsRepository;
	@Autowired
	Utils utils;
	@Autowired
	XxrCloudConfigRepository xxrCloudConfigRepository;

	@Override
	public List<XxrXlsmTempHdrsResp> getXlsmTemplates() throws Exception {
		log.info("Start of getXlsmTemplates Method in service ###");
		List<XxrXlsmTempHdrs> xxrXlsmTempHdrsList = new ArrayList<>();
		List<XxrXlsmTempHdrsResp> xxrXlsmTempHdrsRes = new ArrayList<>();
		xxrXlsmTempHdrsList = xxrXlsmTempHdrsRepository.findAll();
		try {
			xxrXlsmTempHdrsList.stream().forEach(x -> {
				XxrXlsmTempHdrsResp xxrXlsmTempHdrsResp = new XxrXlsmTempHdrsResp();

				xxrXlsmTempHdrsResp.setTemplateId(x.getTemplateId());
				xxrXlsmTempHdrsResp.setTemplateName(x.getTemplateName());
				xxrXlsmTempHdrsResp.setObjectId(x.getObjectId());
				xxrXlsmTempHdrsResp.setPodId(x.getPodId());
				xxrXlsmTempHdrsResp.setProjectId(x.getProjectId());
				xxrXlsmTempHdrsResp.setParentObjectId(x.getParentObjectId());
				String podName = xxrLookUpValuesRepository.getValueById(x.getPodId());
				String projectName = xxrLookUpValuesRepository.getValueById(x.getProjectId());
				String parentObjectCode = xxrLookUpValuesRepository.getValueById(x.getParentObjectId());
				String objectCode = xxrLookUpValuesRepository.getValueById(x.getObjectId());
				xxrXlsmTempHdrsResp.setPodName(podName);
				xxrXlsmTempHdrsResp.setObjectCode(objectCode);
				xxrXlsmTempHdrsResp.setParentObjectCode(parentObjectCode);
				xxrXlsmTempHdrsResp.setProjectName(projectName);
				xxrXlsmTempHdrsResp.setVersion(x.getVersion());
				xxrXlsmTempHdrsResp.setSheetName(x.getSheetName());
				xxrXlsmTempHdrsResp.setApi(x.getApi());

				xxrXlsmTempHdrsRes.add(xxrXlsmTempHdrsResp);
			});

		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return xxrXlsmTempHdrsRes;
	}

	@Override
	public List<XxrXlsmTempCols> getXlsmTemplateColumns(Long templateId) throws Exception {
		log.info("Start of getXlsmTemplateColumns Method in Service ######");
		List<XxrXlsmTempCols> xxrXlsmTempCols = new ArrayList<>();
		try {
			xxrXlsmTempCols = xxrXlsmTempColsRepository.findBytemplateId(templateId);
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return xxrXlsmTempCols;
	}

	@Override
	public String getXlsmTemplateName(String objectCode) throws Exception {
		log.info("Start of getXlsmTemplateName Method in service ###");
		String xlsmTemplateName = "";
		try {
			// xlsmTemplateName = utils.getXlsmFileName(objectCode);
			XxrCloudConfig xxrCloudConfig = xxrCloudConfigRepository.findByObjectCode(objectCode);
			if (xxrCloudConfig == null)
				throw new ValidationException(
						"For " + objectCode + " Cloud Configuration is not defined,Please help to define");
			else
				xlsmTemplateName = xxrCloudConfig.getXlsmFileName();
		} catch (ValidationException e) {
			throw new ValidationException(e.getMessage());
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return xlsmTemplateName;
	}

	@Override
	public String getSheetName(String objectCode) throws Exception {
		log.info("Start of getSheetName Method in service ###");
		String sheetName = "";
		try {
			// sheetName = utils.getXlsmSheetName(objectCode);
			XxrCloudConfig xxrCloudConfig = xxrCloudConfigRepository.findByObjectCode(objectCode);
			if (xxrCloudConfig == null)
				throw new ValidationException(
						"For " + objectCode + " Cloud Configuration is not defined,Please help to define");
			else
				sheetName = xxrCloudConfig.getSheetName();
		} catch (ValidationException e) {
			throw new ValidationException(e.getMessage());
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return sheetName;
	}

	@Override
	public SaveTemplateHeaderResponsePo saveXlsmTemplateHdrs(SaveXlsmTemplateHeaderPo xlsmTemplateHeaderPo)
			throws Exception {
		SaveTemplateHeaderResponsePo saveTemplateHeaderResponsePo = new SaveTemplateHeaderResponsePo();
		try {

			Long templateId = xxrXlsmTempHdrsRepository.getXlsmTemplateId(xlsmTemplateHeaderPo.getTemplateName(),
					xlsmTemplateHeaderPo.getVersion(), xlsmTemplateHeaderPo.getSheetName());
			if (templateId != null)
				throw new ValidationException("combination of XlsmTemplateName,SheetName and version already exists ");
			XxrXlsmTempHdrs xxrXlsmTempHdrs = new XxrXlsmTempHdrs();
			xxrXlsmTempHdrs.setPodId(xlsmTemplateHeaderPo.getPodId());
			xxrXlsmTempHdrs.setProjectId(xlsmTemplateHeaderPo.getProjectId());
			xxrXlsmTempHdrs.setParentObjectId(xlsmTemplateHeaderPo.getParentObjectId());
			xxrXlsmTempHdrs.setObjectId(xlsmTemplateHeaderPo.getObjectId());
			xxrXlsmTempHdrs.setSheetName(xlsmTemplateHeaderPo.getSheetName());
			xxrXlsmTempHdrs.setApi(xlsmTemplateHeaderPo.getApi());
			xxrXlsmTempHdrs.setVersion(xlsmTemplateHeaderPo.getVersion());
			xxrXlsmTempHdrs.setTemplateName(xlsmTemplateHeaderPo.getTemplateName());
			xxrXlsmTempHdrs.setCreationDate(new java.sql.Date(new java.util.Date().getTime()));
			xxrXlsmTempHdrs.setCreatedBy("ConvertRite");
			xxrXlsmTempHdrs.setLastUpdatedDate(new java.sql.Date(new java.util.Date().getTime()));
			xxrXlsmTempHdrs.setLastUpdateBy("ConvertRite");
			XxrXlsmTempHdrs xxrXlsmTempHdrsRes = xxrXlsmTempHdrsRepository.save(xxrXlsmTempHdrs);

			saveTemplateHeaderResponsePo.setTemplateId(xxrXlsmTempHdrsRes.getTemplateId());
			saveTemplateHeaderResponsePo.setTemplateName(xxrXlsmTempHdrsRes.getTemplateName());
			saveTemplateHeaderResponsePo.setMessage("Template Header is saved successfully");

		} catch (ValidationException e) {
			throw new ValidationException(e.getMessage());
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return saveTemplateHeaderResponsePo;
	}

	@Override
	public List<FbdiColumnSequencePo> getXlsmColumnSequence(String excelFileName, String sheetName, String version,
			HttpServletResponse response) throws Exception {
		log.info("Start of getFbdiColumnSequence in service ###");
		// LinkedHashMap<String, Integer> columnSequence = new LinkedHashMap<>();
		List<FbdiColumnSequencePo> columnSequencePo = new ArrayList<>();
		Row row = null;
		try {
			Path path = downloadFbdiTemplateFromServer(excelFileName, version, response);
			row = excelReading(excelFileName, sheetName, 1, 3, path);
			int lastColumn = row.getLastCellNum();
			int sequence = 10;
			for (int columnNum = 0; columnNum < lastColumn; columnNum++) {
				FbdiColumnSequencePo fbdiColumnSequencePo = new FbdiColumnSequencePo();
				String commentValue = "";
				Cell cell = row.getCell(columnNum);
				// columnSequence.put(cell.getStringCellValue(), sequence);
				RichTextString comment = cell.getCellComment().getString();
				String commentString = comment.toString();
				String[] commentArr = commentString.split("\\r?\\n");
				String columnNameString = commentArr[0];
				columnNameString = columnNameString.replaceAll(" ", "");
				if (columnNameString.indexOf("ColumnName:") != -1) {
					// System.out.println("entering iff "+columnNameString);
					commentValue = columnNameString.substring(columnNameString.indexOf("ColumnName:") + 11).trim();
					// System.out.println("entering iff 222"+commentValue);
				} else {
					// System.out.println("entering elseeee"+commentArr[1]+"222"+commentArr[2]);
					String columnNameStr = commentArr[2];
					columnNameStr = columnNameStr.replaceAll(" ", "");
					if (columnNameStr.indexOf("ColumnName:") != -1)
						commentValue = columnNameStr.substring(columnNameStr.indexOf("ColumnName:") + 11).trim();
				} // System.out.println("commentValue#3333333"+commentValue);
				/*
				 * if (commentString.contains("Data Type")) commentValue = commentString
				 * .substring(commentString.indexOf("Column Name:") + 12,
				 * commentString.indexOf("Data Type:")) .replaceAll("\\n", ""); else if
				 * (commentString.contains("Data Type")) commentValue = commentString
				 * .substring(commentString.indexOf("Column Name:") + 12,
				 * commentString.indexOf("Data Type:")) .replaceAll("\\n", "");
				 */

				String fbdiColumnName = cell.getStringCellValue();

				fbdiColumnSequencePo.setFbdiColumnName(fbdiColumnName);
				if (fbdiColumnName.contains("*"))
					fbdiColumnSequencePo.setRequired("Y");
				else
					fbdiColumnSequencePo.setRequired("N");
				fbdiColumnSequencePo.setSequence(sequence);
				if (commentValue.contains("*"))
					fbdiColumnSequencePo.setDatabaseColumn(commentValue.substring(0, (commentValue.length() - 1)));
				else
					fbdiColumnSequencePo.setDatabaseColumn(commentValue);
				sequence = sequence + 10;
				columnSequencePo.add(fbdiColumnSequencePo);
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception();
		}
		return columnSequencePo;
	}

	private Row excelReading(String excelFileName, String sheetName, int sheetNo, int rowNo, Path path)
			throws Exception {
		log.info("Start of excelReading Method  ###");
		XSSFWorkbook workbook = null;
		Row row = null;
		try {
			String tempPath = System.getProperty("java.io.tmpdir");
//			log.info("tempPath:::" + tempPath);
			// tempPath = "C:\\temp";
			FileInputStream file = new FileInputStream(new File(path + File.separator + excelFileName));
			// Create Workbook instance holding reference to .xlsx file
			workbook = new XSSFWorkbook(file);
			// Get second sheet from the workbook
			// XSSFSheet sheet = workbook.getSheetAt(1);
			XSSFSheet sheet = workbook.getSheet(sheetName);
			row = sheet.getRow(rowNo);
		} catch (FileNotFoundException e) { // TODO Auto-generated catch block
			e.printStackTrace();
			throw new Exception();
		} catch (Exception e) {
			throw new Exception();
		} finally {
			if (workbook != null)
				workbook.close();
		}
		return row;
	}

	private Path downloadFbdiTemplateFromServer(String fileName, String version, HttpServletResponse resp)
			throws Exception {
		log.info("Start of downloadFbdiTemplateFromServer Method ###");
		String url = "";
		Path target = null;
		try {
			String sb = excelUrlFirstPart + version +
					excelUrlSecondPart +
					fileName + ".xlsm";
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

	@Override
	public SaveXlsmTemplateColumnsResPo saveXlsmTemplateColumns(List<SaveXlsmTemplateColumnsPo> xlsmTemplateColumnsPo)
			throws Exception {
		log.info("Start of saveXlsmTemplateColumns Method:::::");
		SaveXlsmTemplateColumnsResPo saveXlsmTemplateColumnsResPo = new SaveXlsmTemplateColumnsResPo();
		try {
			List<XxrXlsmTempCols> xlsmTemplateColumns = new ArrayList<>();
			for (SaveXlsmTemplateColumnsPo x : xlsmTemplateColumnsPo) {
				if (x.getTemplateId() == null || x.getDatabaseColumn() == null)
					throw new BadRequestException("templateId and columnName are Mandatory fields");
				XxrXlsmTempCols xxrXlsmTempColRec = new XxrXlsmTempCols();
				XxrXlsmTempCols xxrXlsmTempColResp = new XxrXlsmTempCols();
				XxrXlsmTempCols xxrXlsmTempCols = new XxrXlsmTempCols();
				if (x.getColumnId() != null)
					xxrXlsmTempColResp = xxrXlsmTempColsRepository.findBycolumnId(x.getColumnId());
				if (xxrXlsmTempColResp != null) {
					// updating existing record
					xxrXlsmTempColResp.setActiveFlag(x.getActiveFlag());
					xxrXlsmTempColResp.setColumnName(x.getColumnName());
					xxrXlsmTempColResp.setDatabaseColumn(x.getDatabaseColumn());
					xxrXlsmTempColResp.setDatabaseTable(x.getDatabaseTable());
					xxrXlsmTempColResp.setTemplateId(x.getTemplateId());
					if (x.getEndDate() != null)
						xxrXlsmTempColResp.setEndDate(new SimpleDateFormat("dd-MMM-yyyy").format(x.getEndDate()));
					if (x.getStartDate() != null)
						xxrXlsmTempColResp.setStartDate(new SimpleDateFormat("dd-MMM-yyyy").format(x.getStartDate()));
					xxrXlsmTempColResp.setRequired(x.getRequired());
					xxrXlsmTempColResp.setSequence(x.getSequence());
					xxrXlsmTempColResp.setLastUpdatedDate(new java.sql.Date(new java.util.Date().getTime()));
					xxrXlsmTempColResp.setLastUpdateBy("ConvertRite");
					xxrXlsmTempColRec = xxrXlsmTempColsRepository.save(xxrXlsmTempColResp);
				} else {
					// creating new record
					xxrXlsmTempCols.setActiveFlag(x.getActiveFlag());
					xxrXlsmTempCols.setColumnName(x.getColumnName());
					xxrXlsmTempCols.setDatabaseColumn(x.getDatabaseColumn());
					xxrXlsmTempCols.setDatabaseTable(x.getDatabaseTable());
					xxrXlsmTempCols.setTemplateId(x.getTemplateId());
					if (x.getEndDate() != null)
						xxrXlsmTempCols.setEndDate(new SimpleDateFormat("dd-MMM-yyyy").format(x.getEndDate()));
					if (x.getStartDate() != null)
						xxrXlsmTempCols.setStartDate(new SimpleDateFormat("dd-MMM-yyyy").format(x.getStartDate()));
					xxrXlsmTempCols.setRequired(x.getRequired());
					xxrXlsmTempCols.setSequence(x.getSequence());
					xxrXlsmTempCols.setCreationDate(new java.sql.Date(new java.util.Date().getTime()));
					xxrXlsmTempCols.setCreatedBy("ConvertRite");
					xxrXlsmTempCols.setLastUpdatedDate(new java.sql.Date(new java.util.Date().getTime()));
					xxrXlsmTempCols.setLastUpdateBy("ConvertRite");
					xxrXlsmTempColRec = xxrXlsmTempColsRepository.save(xxrXlsmTempCols);
				}
				xlsmTemplateColumns.add(xxrXlsmTempColRec);
			}

			saveXlsmTemplateColumnsResPo.setXlsmTemplateColumns(xlsmTemplateColumns);
			saveXlsmTemplateColumnsResPo.setMessage("Template Columns saved successfully");

		} catch (BadRequestException e) {
			throw new BadRequestException(e.getMessage());
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return saveXlsmTemplateColumnsResPo;
	}

	@Override
	public void downloadXlsmColumnCsv(String excelFileName, String sheetName, String version,
			HttpServletResponse response) throws Exception {
		log.info("Start of downloadXlsmColumnCsv");
		Row row = null;
		try {
			response.setContentType("text/csv");
			response.setHeader(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=" + sheetName + ".csv");
			PrintWriter writer = response.getWriter();
			CSVWriter csvWriter = new CSVWriter(writer, CSVWriter.DEFAULT_SEPARATOR, CSVWriter.NO_QUOTE_CHARACTER,
					CSVWriter.NO_ESCAPE_CHARACTER, CSVWriter.DEFAULT_LINE_END);
			Path path = downloadFbdiTemplateFromServer(excelFileName, version, response);
			row = excelReading(excelFileName, sheetName, 1, 3, path);
			int lastColumn = row.getLastCellNum();
			for (int columnNum = 0; columnNum < lastColumn; columnNum++) {
				String[] strArr = new String[2];
				Cell cell = row.getCell(columnNum);
				strArr[0] = cell.getStringCellValue();
				RichTextString comment = cell.getCellComment().getString();
				strArr[1] = comment.toString().replaceAll("[\\t\\n\\r]+", " ").replaceAll(",", " ");
				csvWriter.writeNext(strArr);
			}
			csvWriter.flush();
			csvWriter.close();
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
	}

	@Override
	public SaveTemplateHeaderResponsePo saveXlsmAsBlob(Long objectId, String version) throws Exception {
		log.info("Start of saveXlsmAsBlob######");
		SaveTemplateHeaderResponsePo saveTemplateHeaderResponsePo = new SaveTemplateHeaderResponsePo();
		String url = "";
		byte[] bytes = null;
		try {
			XxrXlsmTempHdrs xxrXlsmTempHdrs = xxrXlsmTempHdrsRepository.findByObjectIdAndVersion(objectId, version);
			if (xxrXlsmTempHdrs != null) {
				String sb = excelUrlFirstPart + version +
						excelUrlSecondPart +
						xxrXlsmTempHdrs.getTemplateName() + ".xlsm";
				url = sb;
				// website url
				URL website = new URL(url);
				try (InputStream in = website.openStream()) {
					bytes = IOUtils.toByteArray(in);
				} catch (IOException e) {
					// e.printStackTrace();
					throw new Exception(e.getMessage());
				}
				xxrXlsmTempHdrs.setXlsmFileBlob(bytes);
				XxrXlsmTempHdrs xxrXlsmTempHdrRes =xxrXlsmTempHdrsRepository.save(xxrXlsmTempHdrs);
				
				saveTemplateHeaderResponsePo.setMessage("Updated xlsmHdr with blob successfully");
				saveTemplateHeaderResponsePo.setTemplateId(xxrXlsmTempHdrRes.getTemplateId());
				saveTemplateHeaderResponsePo.setTemplateName(xxrXlsmTempHdrRes.getTemplateName());
			} else {
				throw new Exception("There is no xslm template with above objectId & Version combination");
			}
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return saveTemplateHeaderResponsePo;
	}

}
