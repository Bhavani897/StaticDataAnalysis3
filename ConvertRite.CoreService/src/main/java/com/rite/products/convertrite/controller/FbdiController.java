package com.rite.products.convertrite.controller;

import java.util.ArrayList;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
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
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import com.rite.products.convertrite.Validations.Validations;
import com.rite.products.convertrite.exception.BadRequestException;
import com.rite.products.convertrite.exception.ConvertRiteException;
import com.rite.products.convertrite.exception.ValidationException;
import com.rite.products.convertrite.model.FbdiTempHdrsView;
import com.rite.products.convertrite.model.XxrFbdiTempCols;
import com.rite.products.convertrite.po.FbdiColumnSequencePo;
import com.rite.products.convertrite.po.SaveFbdiTemplateColumnsPo;
import com.rite.products.convertrite.po.SaveFbdiTemplateColumnsResPo;
import com.rite.products.convertrite.po.SaveFbdiTemplateHeaderPo;
import com.rite.products.convertrite.po.SaveTemplateHeaderResponsePo;
import com.rite.products.convertrite.service.FbdiService;

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

@RestController
public class FbdiController {

	private static final Logger log = LoggerFactory.getLogger(FbdiController.class);

	@Autowired
	FbdiService fbdiService;

	@ApiOperation(value = "This Api returns all fbdi templates")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/getfbditemplates")
	public ResponseEntity<List<FbdiTempHdrsView>> getFbdiTemplates(@RequestHeader("roleid") Long roleId)
			throws ConvertRiteException {
		log.info("Start of getFbdiTemplates Method in Controller ###");
		List<FbdiTempHdrsView> fbdiTempHdrsResp = new ArrayList<>();
		try {

			fbdiTempHdrsResp = fbdiService.getFbdiTemplates(roleId);

		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<List<FbdiTempHdrsView>>(fbdiTempHdrsResp, new HttpHeaders(), HttpStatus.OK);

	}

	/*
	 * @ApiOperation(value = "This Api returns all fbdi templates")
	 * 
	 * @ApiResponses(value = { @ApiResponse(code = 200, message =
	 * "Successful Response"),
	 * 
	 * @ApiResponse(code = 500, message = "Server Side Error") })
	 * 
	 * @GetMapping("/getfbditemplates")
	 * 
	 * @Cacheable(value = "fbditemphdrcache") public
	 * ResponseEntity<List<XxrFbdiTempHdrsResp>> getFbdiTemplates() throws
	 * ConvertRiteException {
	 * log.info("Start of getFbdiTemplates Method in Controller ###");
	 * List<XxrFbdiTempHdrsResp> fbdiTempHdrsResp = new ArrayList<>(); try {
	 * 
	 * fbdiTempHdrsResp = fbdiService.getFbdiTemplates();
	 * 
	 * } catch (Exception e) { log.error(e.getMessage()); throw new
	 * ConvertRiteException(
	 * "Please contact System Administrator there is an error while processing the request"
	 * , HttpStatus.INTERNAL_SERVER_ERROR); } return new
	 * ResponseEntity<List<XxrFbdiTempHdrsResp>>(fbdiTempHdrsResp, new
	 * HttpHeaders(), HttpStatus.OK);
	 * 
	 * }
	 */

	@ApiOperation(value = "This Api returns fbdi columns based on FbdiTemplateId")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/getfbditemplatecolumns")
	@Cacheable(value = "fbditempcolscache")
	public ResponseEntity<List<XxrFbdiTempCols>> getFbdiTemplateColumns(
			@RequestParam("fbdiTemplateId") Long fbdiTemplateId) throws ConvertRiteException {
		log.info("Start of getFbdiTemplateColumns Method in Controller ###");
		List<XxrFbdiTempCols> fbdiTempColsResp = new ArrayList<>();
		try {
			fbdiTempColsResp = fbdiService.getFbdiTemplateColumns(fbdiTemplateId);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<List<XxrFbdiTempCols>>(fbdiTempColsResp, new HttpHeaders(), HttpStatus.OK);

	}

	@ApiOperation(value = "This Api returns fbdi template name based on ObjectCode")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/getfbditemplatename")
	public ResponseEntity<String> getFbdiTemplateName(@RequestParam("objectCode") String objectCode)
			throws ConvertRiteException {
		log.info("Start of getFbdiTemplateName Method in Controller ###");
		String fbdiTemplateName = "";
		try {
			fbdiTemplateName = fbdiService.getFbdiTemplateName(objectCode);
			//fix for Cross-site Scripting (XSS)
			fbdiTemplateName=StringEscapeUtils.escapeHtml4(fbdiTemplateName);
		} catch (ValidationException e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		
		return new ResponseEntity<String>(fbdiTemplateName, new HttpHeaders(), HttpStatus.OK);

	}

	@ApiOperation(value = "This Api returns Sheet Name based on ObjectId")

	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),

			@ApiResponse(code = 500, message = "Server Side Error") })

	@GetMapping("/getsheetname")
	public ResponseEntity<String> getSheetName(@RequestParam("objectId") Long objectId) throws ConvertRiteException {
		log.info("Start of getSheetName Method in Controller ###");
		String sheetName = "";
		try {

			sheetName = fbdiService.getSheetName(objectId);

		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<String>(sheetName, new HttpHeaders(), HttpStatus.OK);

	}

	@ApiOperation(value = "This Api to save fbdi template header")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@PostMapping("/savefbditemplateheader")
	// @CacheEvict(value = "fbditemphdrcache", allEntries = true)
	public ResponseEntity<SaveTemplateHeaderResponsePo> saveFbdiTemplateHdrs(
			@RequestBody List<SaveFbdiTemplateHeaderPo> fbdiTemplateHeaderPo, HttpServletRequest request)
			throws ConvertRiteException {
		log.info("Start of saveFbdiTemplateHdrs Method in Controller ###");
		SaveTemplateHeaderResponsePo saveTemplateHeaderResponsePo = new SaveTemplateHeaderResponsePo();
		try {
			SaveFbdiTemplateHeaderPo saveFbdiTemplateHeaderPo = fbdiTemplateHeaderPo.get(0);

			if (saveFbdiTemplateHeaderPo.getPodId() == null || saveFbdiTemplateHeaderPo.getObjectId() == null
					|| Validations.isNullOrEmpty(saveFbdiTemplateHeaderPo.getFbdiTemplateName())
					|| saveFbdiTemplateHeaderPo.getParentObjectId() == null
					|| saveFbdiTemplateHeaderPo.getProjectId() == null)
				throw new BadRequestException(
						"podId,objectId,projectId,parentOjectId and fbdiTemplateName are Mandatory fields");

			saveTemplateHeaderResponsePo = fbdiService.saveFbdiTemplateHdrs(fbdiTemplateHeaderPo, request);

		} catch (ValidationException e) {
			log.error(e.getMessage() + "ValidationException");
			saveTemplateHeaderResponsePo.setMessage("Error while saving Fbdi template");
			saveTemplateHeaderResponsePo.setError(e.getMessage());
			return new ResponseEntity<SaveTemplateHeaderResponsePo>(saveTemplateHeaderResponsePo, HttpStatus.OK);
		} catch (BadRequestException e) {
			log.error(e.getMessage() + "BadRequestException");
			saveTemplateHeaderResponsePo.setMessage("Error while saving Fbdi template");
			saveTemplateHeaderResponsePo.setError(e.getMessage());
			return new ResponseEntity<SaveTemplateHeaderResponsePo>(saveTemplateHeaderResponsePo,
					HttpStatus.BAD_REQUEST);
		} catch (Exception e) {
			log.error(e.getMessage() + "Exception");
			saveTemplateHeaderResponsePo.setMessage("Error while saving Fbdi template");
			saveTemplateHeaderResponsePo.setError(e.getMessage());
			return new ResponseEntity<SaveTemplateHeaderResponsePo>(saveTemplateHeaderResponsePo,
					HttpStatus.INTERNAL_SERVER_ERROR);
		}

		return new ResponseEntity<SaveTemplateHeaderResponsePo>(saveTemplateHeaderResponsePo, new HttpHeaders(),
				HttpStatus.OK);

	}

	@ApiOperation(value = "This Api returns fbdi column sequence")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/getfbdicolumnsequence")
	public ResponseEntity<List<FbdiColumnSequencePo>> getFbdiColumnSequence(@RequestParam("fileName") String fileName,
			@RequestParam("version") String version, HttpServletResponse response) throws ConvertRiteException {
		log.info("Start of getFbdiColumnSequence Method in Controller ###");

		List<FbdiColumnSequencePo> columnSequence = new ArrayList<>();
		try {

			columnSequence = fbdiService.getFbdiColumnSequence(fileName, version, response);

		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<List<FbdiColumnSequencePo>>(columnSequence, new HttpHeaders(), HttpStatus.OK);

	}

	@PostMapping("/uploadctlgetcolumnseq")
	public ResponseEntity<List<FbdiColumnSequencePo>> uploadCtlGetColumnSeq(@RequestParam("file") MultipartFile file,
			@RequestParam("version") String version, @RequestParam("objectId") Long objectId)
			throws ConvertRiteException {
		log.info("Start of uploadCtlGetColumnSeq Method in Controller ###");
		List<FbdiColumnSequencePo> columnSequence = new ArrayList<>();
		try {

			columnSequence = fbdiService.uploadCtlGetColumnSeq(file, version, objectId);

		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<List<FbdiColumnSequencePo>>(columnSequence, new HttpHeaders(), HttpStatus.OK);
	}

	@ApiOperation(value = "Save ctl file as BLOB")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@PutMapping("/savectlasblob")
	public ResponseEntity<SaveTemplateHeaderResponsePo> saveCtlAsBlob(@RequestParam("objectId") Long objectId,
			@RequestParam("version") String version) throws Exception {
		log.info("Start of saveCtlAsBlob Method in Controller ###");
		SaveTemplateHeaderResponsePo saveTemplateHeaderResponsePo = new SaveTemplateHeaderResponsePo();
		try {
			saveTemplateHeaderResponsePo = fbdiService.saveCtlAsBlob(objectId, version);
		} catch (Exception e) {
			log.error(e.getMessage() + "Exception");
			saveTemplateHeaderResponsePo.setMessage("Error while saving Ctl as Blob");
			saveTemplateHeaderResponsePo.setError(e.getMessage());
			return new ResponseEntity<SaveTemplateHeaderResponsePo>(saveTemplateHeaderResponsePo,
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<SaveTemplateHeaderResponsePo>(saveTemplateHeaderResponsePo, new HttpHeaders(),
				HttpStatus.OK);
	}

	@ApiOperation(value = "This Api is to save fbditemplate columns")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error"),
			@ApiResponse(code = 400, message = "Bad Request") })
	@PostMapping("/savefbditemplatecolumns")
	@CacheEvict(value = "fbditempcolscache", allEntries = true)
	public ResponseEntity<SaveFbdiTemplateColumnsResPo> saveFbdiTemplateColumns(
			@RequestBody List<SaveFbdiTemplateColumnsPo> fbdiTemplateColumnsPo, HttpServletRequest request) {
		log.info("Start of saveFbdiTemplateColumns Method in Controller ###");

		SaveFbdiTemplateColumnsResPo saveFbdiTemplateColumnsResPo = new SaveFbdiTemplateColumnsResPo();
		try {
			saveFbdiTemplateColumnsResPo = fbdiService.saveFbdiTemplateColumns(fbdiTemplateColumnsPo, request);

		} catch (BadRequestException e) {
			log.error(e.getMessage());
			saveFbdiTemplateColumnsResPo.setMessage("Error while saving fbdi template cloumns");
			saveFbdiTemplateColumnsResPo.setError(e.getMessage());
			return new ResponseEntity<SaveFbdiTemplateColumnsResPo>(saveFbdiTemplateColumnsResPo,
					HttpStatus.BAD_REQUEST);
		} catch (Exception e) {
			log.error(e.getMessage());
			saveFbdiTemplateColumnsResPo.setMessage("Error while saving fbdi template cloumns");
			saveFbdiTemplateColumnsResPo.setError(e.getMessage());
			return new ResponseEntity<SaveFbdiTemplateColumnsResPo>(saveFbdiTemplateColumnsResPo,
					HttpStatus.INTERNAL_SERVER_ERROR);
		}

		return new ResponseEntity<SaveFbdiTemplateColumnsResPo>(saveFbdiTemplateColumnsResPo, new HttpHeaders(),
				HttpStatus.OK);

	}

	@ApiOperation(value = "This Api is to get sequence of SAP Template")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error"),
			@ApiResponse(code = 400, message = "Bad Request") })
	@PostMapping("/generatedsapfbdiseq")
	public ResponseEntity<?> generateSapFbdiSeq(@RequestParam("objectCode") String objectCode) {
		log.info("Start of generateSapFbdiSeq");
		List<FbdiColumnSequencePo> li = new ArrayList<>();
		try {
			li = fbdiService.generateSapFbdiSeq(objectCode);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return new ResponseEntity<List<FbdiColumnSequencePo>>(li, HttpStatus.OK);
	}

	@ApiOperation(value = "This Api to save fbdi template header with jpa")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@PostMapping("/savefbditemplateheaderwithjpa")
	public ResponseEntity<SaveTemplateHeaderResponsePo> saveFbdiTemplateHdrsWithJpa(
			@RequestBody SaveFbdiTemplateHeaderPo saveFbdiTemplateHeaderPo, HttpServletRequest request)
			throws ConvertRiteException {
		log.info("Start of saveFbdiTemplateHdrsWithJpa Method in Controller ###");
		SaveTemplateHeaderResponsePo saveTemplateHeaderResponsePo = new SaveTemplateHeaderResponsePo();
		try {
			if (saveFbdiTemplateHeaderPo.getPodId() == null || saveFbdiTemplateHeaderPo.getObjectId() == null
					|| Validations.isNullOrEmpty(saveFbdiTemplateHeaderPo.getFbdiTemplateName())
					|| saveFbdiTemplateHeaderPo.getParentObjectId() == null
					|| saveFbdiTemplateHeaderPo.getProjectId() == null)
				throw new BadRequestException(
						"podId,objectId,projectId,parentOjectId and fbdiTemplateName are Mandatory fields");

			saveTemplateHeaderResponsePo = fbdiService.saveFbdiTemplateHdrsJpa(saveFbdiTemplateHeaderPo, request);

		} catch (ValidationException e) {
			log.error(e.getMessage() + "ValidationException");
			saveTemplateHeaderResponsePo.setMessage(e.getMessage());
			saveTemplateHeaderResponsePo
					.setError("Please contact System Administrator there is an error while processing the request");
			return new ResponseEntity<SaveTemplateHeaderResponsePo>(saveTemplateHeaderResponsePo, HttpStatus.OK);
		} catch (BadRequestException e) {
			log.error(e.getMessage() + "BadRequestException");
			saveTemplateHeaderResponsePo.setMessage(e.getMessage());
			saveTemplateHeaderResponsePo
					.setError("Please contact System Administrator there is an error while processing the request");
			return new ResponseEntity<SaveTemplateHeaderResponsePo>(saveTemplateHeaderResponsePo,
					HttpStatus.BAD_REQUEST);
		} catch (Exception e) {
			log.error(e.getMessage() + "Exception");
			saveTemplateHeaderResponsePo
					.setMessage("Please contact System Administrator there is an error while processing the request");
			saveTemplateHeaderResponsePo.setError(e.getMessage());
			return new ResponseEntity<SaveTemplateHeaderResponsePo>(saveTemplateHeaderResponsePo,
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<SaveTemplateHeaderResponsePo>(saveTemplateHeaderResponsePo, new HttpHeaders(),
				HttpStatus.OK);
	}

	@ApiOperation(value = "This Api is to save fbditemplate columns with jpa")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error"),
			@ApiResponse(code = 400, message = "Bad Request") })
	@PostMapping("/savefbditemplatecolumnswithjpa")
	@CacheEvict(value = "fbditempcolscache", allEntries = true)
	public ResponseEntity<SaveFbdiTemplateColumnsResPo> saveFbdiTemplateColumnsWithJpa(
			@RequestBody List<SaveFbdiTemplateColumnsPo> fbdiTemplateColumnsPo, HttpServletRequest request) {
		log.info("Start of saveFbdiTemplateColumnsWithJpa Method in Controller ###");

		SaveFbdiTemplateColumnsResPo saveFbdiTemplateColumnsResPo = new SaveFbdiTemplateColumnsResPo();
		try {
			saveFbdiTemplateColumnsResPo = fbdiService.saveFbdiTemplateColumnsWithJpa(fbdiTemplateColumnsPo, request);

		} catch (BadRequestException e) {
			log.error(e.getMessage());
			saveFbdiTemplateColumnsResPo.setMessage(e.getMessage());
			saveFbdiTemplateColumnsResPo
					.setError("Please contact System Administrator there is an error while processing the request");
			return new ResponseEntity<SaveFbdiTemplateColumnsResPo>(saveFbdiTemplateColumnsResPo,
					HttpStatus.BAD_REQUEST);
		} catch (Exception e) {
			log.error(e.getMessage());
			saveFbdiTemplateColumnsResPo
					.setMessage("Please contact System Administrator there is an error while processing the request");
			saveFbdiTemplateColumnsResPo.setError(e.getMessage());
			return new ResponseEntity<SaveFbdiTemplateColumnsResPo>(saveFbdiTemplateColumnsResPo,
					HttpStatus.INTERNAL_SERVER_ERROR);
		}

		return new ResponseEntity<SaveFbdiTemplateColumnsResPo>(saveFbdiTemplateColumnsResPo, new HttpHeaders(),
				HttpStatus.OK);

	}

}
