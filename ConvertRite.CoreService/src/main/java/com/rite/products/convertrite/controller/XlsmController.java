package com.rite.products.convertrite.controller;

import java.util.ArrayList;
import java.util.List;

import javax.servlet.http.HttpServletResponse;

import org.apache.commons.text.StringEscapeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.rite.products.convertrite.Validations.Validations;
import com.rite.products.convertrite.exception.BadRequestException;
import com.rite.products.convertrite.exception.ConvertRiteException;
import com.rite.products.convertrite.exception.ValidationException;
import com.rite.products.convertrite.model.XxrXlsmTempCols;
import com.rite.products.convertrite.po.FbdiColumnSequencePo;
import com.rite.products.convertrite.po.SaveTemplateHeaderResponsePo;
import com.rite.products.convertrite.po.SaveXlsmTemplateColumnsPo;
import com.rite.products.convertrite.po.SaveXlsmTemplateColumnsResPo;
import com.rite.products.convertrite.po.SaveXlsmTemplateHeaderPo;
import com.rite.products.convertrite.po.XxrXlsmTempHdrsResp;
import com.rite.products.convertrite.service.XlsmService;

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

@RestController
public class XlsmController {

	private static final Logger log = LoggerFactory.getLogger(XlsmController.class);

	@Autowired
	XlsmService xlsmService;

	@ApiOperation(value = "This Api returns all xlsm templates")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/getxlsmtemplates")
	@Cacheable(value="xlsmtemphdrcache")
	public ResponseEntity<List<XxrXlsmTempHdrsResp>> getXlsmTemplates() throws ConvertRiteException {
		log.info("Start of getXlsmTemplates Method in Controller ###");
		List<XxrXlsmTempHdrsResp> xlsmTempHdrsResp = new ArrayList<>();
		try {

			xlsmTempHdrsResp = xlsmService.getXlsmTemplates();

		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<List<XxrXlsmTempHdrsResp>>(xlsmTempHdrsResp, new HttpHeaders(), HttpStatus.OK);

	}

	@ApiOperation(value = "This Api returns xlsm columns based on TemplateId")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/getxlsmtemplatecolumns")
	@Cacheable(value="xlsmtemplatecolumnscache")
	public ResponseEntity<List<XxrXlsmTempCols>> getXlsmTemplateColumns(@RequestParam("templateId") Long templateId)
			throws ConvertRiteException {
		log.info("Start of getXlsmTemplateColumns Method in Controller ###");
		List<XxrXlsmTempCols> xlsmTempColsResp = new ArrayList<>();
		try {

			xlsmTempColsResp = xlsmService.getXlsmTemplateColumns(templateId);

		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<List<XxrXlsmTempCols>>(xlsmTempColsResp, new HttpHeaders(), HttpStatus.OK);
	}

	@ApiOperation(value = "This Api returns xlsm template name based on ObjectCode")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/getxlsmtemplatename")
	public ResponseEntity<String> getXlsmTemplateName(@RequestParam("objectCode") String objectCode)
			throws ConvertRiteException {
		log.info("Start of getXlsmTemplateName Method in Controller ###");
		String xlsmTemplateName = "";
		try {
			xlsmTemplateName = xlsmService.getXlsmTemplateName(objectCode);
			////fix for Cross-site Scripting (XSS)
			xlsmTemplateName=StringEscapeUtils.escapeHtml4(xlsmTemplateName);
		} catch (ValidationException e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<String>(xlsmTemplateName, new HttpHeaders(), HttpStatus.OK);

	}

	@ApiOperation(value = "This Api returns SheetName based on ObjectCode")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/getsheetnamebyobjectcode")
	public ResponseEntity<String> getSheetName(@RequestParam("objectCode") String objectCode)
			throws ConvertRiteException {
		log.info("Start of getSheetName Method in Controller ###");
		String sheetName = "";
		try {
			sheetName = xlsmService.getSheetName(objectCode);
			////fix for Cross-site Scripting (XSS)
			sheetName=StringEscapeUtils.escapeHtml4(sheetName);
		} catch (ValidationException e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<String>(sheetName, new HttpHeaders(), HttpStatus.OK);
	}

	@ApiOperation(value = "This Api to save xlsm template header")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@PostMapping("/savexlsmtemplateheader")
	@CacheEvict(value="xlsmtemphdrcache",allEntries = true)
	public ResponseEntity<SaveTemplateHeaderResponsePo> saveXlsmTemplateHdrs(
			@RequestBody SaveXlsmTemplateHeaderPo xlsmTemplateHeaderPo) throws ConvertRiteException {
		log.info("Start of saveXlsmTemplateHdrs Method in Controller ###");
		SaveTemplateHeaderResponsePo saveTemplateHeaderResponsePo = new SaveTemplateHeaderResponsePo();
		try {

			if (xlsmTemplateHeaderPo.getPodId() == null || xlsmTemplateHeaderPo.getObjectId() == null
					|| Validations.isNullOrEmpty(xlsmTemplateHeaderPo.getTemplateName())
					|| xlsmTemplateHeaderPo.getParentObjectId() == null || xlsmTemplateHeaderPo.getProjectId() == null
					|| Validations.isNullOrEmpty(xlsmTemplateHeaderPo.getVersion())
					|| Validations.isNullOrEmpty(xlsmTemplateHeaderPo.getSheetName()))
				throw new BadRequestException(
						"podId,objectId,projectId,parentOjectId,version,sheetName and templateName are Mandatory fields");

			saveTemplateHeaderResponsePo = xlsmService.saveXlsmTemplateHdrs(xlsmTemplateHeaderPo);

		} catch (ValidationException e) {
			log.error(e.getMessage() + "ValidationException");
			saveTemplateHeaderResponsePo.setMessage("Error while saving Xlsm template");
			saveTemplateHeaderResponsePo.setError(e.getMessage());
			return new ResponseEntity<SaveTemplateHeaderResponsePo>(saveTemplateHeaderResponsePo, HttpStatus.OK);
		} catch (BadRequestException e) {
			log.error(e.getMessage() + "BadRequestException");
			saveTemplateHeaderResponsePo.setMessage("Error while saving Xlsm template");
			saveTemplateHeaderResponsePo.setError(e.getMessage());
			return new ResponseEntity<SaveTemplateHeaderResponsePo>(saveTemplateHeaderResponsePo,
					HttpStatus.BAD_REQUEST);
		} catch (Exception e) {
			log.error(e.getMessage() + "Exception");
			saveTemplateHeaderResponsePo.setMessage("Error while saving Xlsm template");
			saveTemplateHeaderResponsePo.setError(e.getMessage());
			return new ResponseEntity<SaveTemplateHeaderResponsePo>(saveTemplateHeaderResponsePo,
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<SaveTemplateHeaderResponsePo>(saveTemplateHeaderResponsePo, new HttpHeaders(),
				HttpStatus.OK);
	}

	@ApiOperation(value = "This Api returns xlsm column sequence")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/getxlsmcolumnsequence")
	public ResponseEntity<List<FbdiColumnSequencePo>> getXlsmColumnSequence(@RequestParam("fileName") String fileName,
			@RequestParam("sheetName") String sheetName, @RequestParam("version") String version,
			HttpServletResponse response) throws ConvertRiteException {
		log.info("Start of getXlsmColumnSequence Method in Controller ###");

		List<FbdiColumnSequencePo> columnSequence = new ArrayList<>();
		try {

			columnSequence = xlsmService.getXlsmColumnSequence(fileName, sheetName, version, response);

		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<List<FbdiColumnSequencePo>>(columnSequence, new HttpHeaders(), HttpStatus.OK);

	}
	
	@ApiOperation(value = "Save xlsm file as BLOB")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@PutMapping("/savexlsmasblob")
	public ResponseEntity<SaveTemplateHeaderResponsePo> saveXlsmAsBlob(@RequestParam("objectId") Long objectId,
			@RequestParam("version") String version) throws Exception {
		log.info("Start of saveXlsmAsBlob Method in Controller ###");
		SaveTemplateHeaderResponsePo saveTemplateHeaderResponsePo = new SaveTemplateHeaderResponsePo();
		try {
			saveTemplateHeaderResponsePo=xlsmService.saveXlsmAsBlob(objectId,version);
		} catch (Exception e) {
			log.error(e.getMessage() + "Exception");
			saveTemplateHeaderResponsePo.setMessage("Error while saving Xlsm as Blob");
			saveTemplateHeaderResponsePo.setError(e.getMessage());
			return new ResponseEntity<SaveTemplateHeaderResponsePo>(saveTemplateHeaderResponsePo,
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<SaveTemplateHeaderResponsePo>(saveTemplateHeaderResponsePo, new HttpHeaders(),
				HttpStatus.OK);
	}
	
	@ApiOperation(value = "This Api will download csv with xlsm columnnames & columncomments")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/downloadxlsmcolumncsv")
	public void downloadXlsmColumnCsv(@RequestParam("fileName") String fileName,
			@RequestParam("sheetName") String sheetName, @RequestParam("version") String version,
			HttpServletResponse response) throws ConvertRiteException {
		log.info("Start of downloadXlsmColumnCsv Method in Controller ###");
		try {
			 xlsmService.downloadXlsmColumnCsv(fileName, sheetName, version, response);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		
	}

	@ApiOperation(value = "This Api is to save xlsm template columns")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error"),
			@ApiResponse(code = 400, message = "Bad Request") })
	@PostMapping("/savexlsmtemplatecolumns")
	@CacheEvict(value="xlsmtemplatecolumnscache",allEntries = true)
	public ResponseEntity<SaveXlsmTemplateColumnsResPo> saveXlsmTemplateColumns(
			@RequestBody List<SaveXlsmTemplateColumnsPo> xlsmTemplateColumnsPo) {
		log.info("Start of saveXlsmTemplateColumns Method in Controller ###");

		SaveXlsmTemplateColumnsResPo saveXlsmTemplateColumnsResPo = new SaveXlsmTemplateColumnsResPo();
		try {
			saveXlsmTemplateColumnsResPo = xlsmService.saveXlsmTemplateColumns(xlsmTemplateColumnsPo);

		} catch (BadRequestException e) {
			log.error(e.getMessage());
			saveXlsmTemplateColumnsResPo.setMessage("Error while saving Xlsm template cloumns");
			saveXlsmTemplateColumnsResPo.setError(e.getMessage());
			return new ResponseEntity<SaveXlsmTemplateColumnsResPo>(saveXlsmTemplateColumnsResPo,
					HttpStatus.BAD_REQUEST);
		} catch (Exception e) {
			log.error(e.getMessage());
			saveXlsmTemplateColumnsResPo.setMessage("Error while saving Xlsm template cloumns");
			saveXlsmTemplateColumnsResPo.setError(e.getMessage());
			return new ResponseEntity<SaveXlsmTemplateColumnsResPo>(saveXlsmTemplateColumnsResPo,
					HttpStatus.INTERNAL_SERVER_ERROR);
		}

		return new ResponseEntity<SaveXlsmTemplateColumnsResPo>(saveXlsmTemplateColumnsResPo, new HttpHeaders(),
				HttpStatus.OK);

	}
}