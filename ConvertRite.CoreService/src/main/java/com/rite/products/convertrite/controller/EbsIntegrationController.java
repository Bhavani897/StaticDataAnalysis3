package com.rite.products.convertrite.controller;

import java.util.ArrayList;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

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
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import com.rite.products.convertrite.Validations.Validations;
import com.rite.products.convertrite.exception.BadRequestException;
import com.rite.products.convertrite.exception.ConvertRiteException;
import com.rite.products.convertrite.exception.ValidationException;
import com.rite.products.convertrite.model.XxrSrcSystemSqlClob;
import com.rite.products.convertrite.po.DbLinkReqPo;
import com.rite.products.convertrite.po.DbLinkResPo;
import com.rite.products.convertrite.po.LoadDataFromEbsReqPo;
import com.rite.products.convertrite.po.LoadDataFromEbsResPo;
import com.rite.products.convertrite.po.LoadMetaDataFromEbsReqPo;
import com.rite.products.convertrite.po.LoadMetaDataFromEbsRes;
import com.rite.products.convertrite.po.SaveEbsIntegrationDetailsPo;
import com.rite.products.convertrite.po.SaveEbsIntegrationResponsePo;
import com.rite.products.convertrite.po.SaveEbsViewReqpo;
import com.rite.products.convertrite.po.SrcSystemLobIdRes;
import com.rite.products.convertrite.po.XxrEbsIntegrationDetailsResPo;
import com.rite.products.convertrite.respository.XxrSourceTablesRepository;
import com.rite.products.convertrite.service.EbsIntegrationService;

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

@RestController
public class EbsIntegrationController {

	private static final Logger log = LoggerFactory.getLogger(EbsIntegrationController.class);

	@Autowired
	EbsIntegrationService ebsIntegrationService;
	@Autowired
	XxrSourceTablesRepository xxrSourceTablesRepository;

	@ApiOperation(value = "This api is to save ebsintegrationdetails")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error"),
			@ApiResponse(code = 400, message = "Bad Request") })
	@PostMapping("/saveebsintegrationdetails")
	@CacheEvict(value = "ebsintegrationdetailscache", allEntries = true)
	public ResponseEntity<SaveEbsIntegrationResponsePo> saveEbsIntegrationDetails(
			@RequestBody SaveEbsIntegrationDetailsPo ebsIntegartionDetailsPo) {
		log.info("Start of saveEbsIntegrationDetails Method in Controller ###");

		SaveEbsIntegrationResponsePo saveEbsIntegrationResponsePo = new SaveEbsIntegrationResponsePo();
		try {
			if (ebsIntegartionDetailsPo.getPodId() == null || ebsIntegartionDetailsPo.getProjectId() == null
					|| ebsIntegartionDetailsPo.getParentObjectId() == null || ebsIntegartionDetailsPo.getPort() == null
					|| Validations.isNullOrEmpty(ebsIntegartionDetailsPo.getUserName())
					|| Validations.isNullOrEmpty(ebsIntegartionDetailsPo.getServiceName())
					|| Validations.isNullOrEmpty(ebsIntegartionDetailsPo.getPassword())
					|| Validations.isNullOrEmpty(ebsIntegartionDetailsPo.getHostName()))
				throw new BadRequestException(
						"podId,projectId,parentObjectId,hostName,serviceName,userName,password and port are Mandatory fields");

			saveEbsIntegrationResponsePo = ebsIntegrationService.saveEbsIntegrationDetails(ebsIntegartionDetailsPo);

		} catch (ValidationException e) {
			log.error(e.getMessage() + "ValidationException");
			saveEbsIntegrationResponsePo.setMessage("Error while saving ebs integration details");
			saveEbsIntegrationResponsePo.setError(e.getMessage());
			return new ResponseEntity<SaveEbsIntegrationResponsePo>(saveEbsIntegrationResponsePo, HttpStatus.OK);
		} catch (BadRequestException e) {
			log.error(e.getMessage() + "BadRequestException");
			saveEbsIntegrationResponsePo.setMessage("Error while saving ebs integration details");
			saveEbsIntegrationResponsePo.setError(e.getMessage());
			return new ResponseEntity<SaveEbsIntegrationResponsePo>(saveEbsIntegrationResponsePo,
					HttpStatus.BAD_REQUEST);
		} catch (Exception e) {
			log.error(e.getMessage() + "Exception");
			saveEbsIntegrationResponsePo.setMessage("Error while saving ebs integration details");
			saveEbsIntegrationResponsePo.setError(e.getMessage());
			return new ResponseEntity<SaveEbsIntegrationResponsePo>(saveEbsIntegrationResponsePo,
					HttpStatus.INTERNAL_SERVER_ERROR);
		}

		return new ResponseEntity<SaveEbsIntegrationResponsePo>(saveEbsIntegrationResponsePo, new HttpHeaders(),
				HttpStatus.OK);

	}

	/*@ApiOperation(value = "This Api return all ebs integration details")

	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),

			@ApiResponse(code = 500, message = "Server Side Error"),

			@ApiResponse(code = 400, message = "Bad Request") })

	
	 * @GetMapping("/getebsintegrationdetails")
	 * 
	 * @Cacheable(value = "ebsintegrationdetailscache") public
	 * ResponseEntity<List<XxrEbsIntegrationDbDetailsResPo>>
	 * getEbsIntegrationDetails() throws ConvertRiteException {
	 * log.info("Start of getEbsIntegrationDetails Method in Controller ###");
	 * List<XxrEbsIntegrationDbDetailsResPo> ebsIntegrationDetailsList = new
	 * ArrayList<>(); try { ebsIntegrationDetailsList =
	 * ebsIntegrationService.getEbsIntegrationDetails(); } catch (Exception e) {
	 * log.error(e.getMessage()); throw new ConvertRiteException(
	 * "Please contact System Administrator there is an error while processing the request"
	 * , HttpStatus.INTERNAL_SERVER_ERROR); } return new
	 * ResponseEntity<List<XxrEbsIntegrationDbDetailsResPo>>(
	 * ebsIntegrationDetailsList, new HttpHeaders(), HttpStatus.OK); }
	 */

	@ApiOperation(value = "This Api return all ebs integration details")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error"),
			@ApiResponse(code = 400, message = "Bad Request") })
	@GetMapping("/getebsintegrationdetails")
	@Cacheable(value = "ebsintegrationdetailscache")
	public ResponseEntity<List<XxrEbsIntegrationDetailsResPo>> getEbsIntegrationDetails() throws ConvertRiteException {
		log.info("Start of getEbsIntegrationDetails Method in Controller ###");
		List<XxrEbsIntegrationDetailsResPo> ebsIntegrationDetailsList = new ArrayList<>();
		try {
			ebsIntegrationDetailsList = ebsIntegrationService.getEbsIntegrationDetails();
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<List<XxrEbsIntegrationDetailsResPo>>(ebsIntegrationDetailsList, new HttpHeaders(),
				HttpStatus.OK);
	}

	

	
	@ApiOperation(value = "This Api return dblink based on podId,projectId,objectId,parentObjectId")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error"),
			@ApiResponse(code = 400, message = "Bad Request") })
	@PostMapping("/getdblink")
	public ResponseEntity<DbLinkResPo> getDbLink(@RequestBody DbLinkReqPo dbLinkReqPo) throws ConvertRiteException {
		log.info("Start of getDbLink Method in Controller ###");
		DbLinkResPo dbLinkResPo = new DbLinkResPo();
		try {
			dbLinkResPo = ebsIntegrationService.getDbLink(dbLinkReqPo);
		} catch (ValidationException e) {
			log.error(e.getMessage());
			dbLinkResPo.setError(e.getMessage());
			dbLinkResPo
					.setMessage("Please contact System Administrator there is an error while processing the request");
			return new ResponseEntity<DbLinkResPo>(dbLinkResPo, new HttpHeaders(), HttpStatus.OK);
		} catch (Exception e) {
			log.error(e.getMessage());
			dbLinkResPo.setError(e.getMessage());
			dbLinkResPo
					.setMessage("Please contact System Administrator there is an error while processing the request");
			return new ResponseEntity<DbLinkResPo>(dbLinkResPo, new HttpHeaders(), HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<DbLinkResPo>(dbLinkResPo, new HttpHeaders(), HttpStatus.OK);
	}

	@ApiOperation(value = "This Api loads ebs data")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error"),
			@ApiResponse(code = 400, message = "Bad Request") })
	@PostMapping("/loaddatafromebs")
	public ResponseEntity<LoadDataFromEbsResPo> loadDataFromEbs(@RequestBody LoadDataFromEbsReqPo loadDataFromEbsReqPo,
			HttpServletRequest request) throws ConvertRiteException {
		log.info("Start of loadDataFromEbs Method in Controller ###");
		LoadDataFromEbsResPo loadDataFromEbsResPo = new LoadDataFromEbsResPo();
		try {
			if (Validations.isNullOrEmpty(loadDataFromEbsReqPo.getDbLink())
					|| loadDataFromEbsReqPo.getSourceTemplateId() == null
					//|| Validations.isNullOrEmpty(loadDataFromEbsReqPo.getObjectCode())
					|| Validations.isNullOrEmpty(loadDataFromEbsReqPo.getSourceSystem())
					|| Validations.isNullOrEmpty(loadDataFromEbsReqPo.getVersion()))
				throw new BadRequestException(
						"dbLink,sourceTemplateId,sourceSystem and version are Mandatory fields");
			String userId=request.getHeader("userId");
			loadDataFromEbsResPo = ebsIntegrationService.loadDataFromEbs(loadDataFromEbsReqPo, userId);
		} catch (BadRequestException e) {
			log.error(e.getMessage() + "BadRequestException");
			loadDataFromEbsResPo.setMessage("Something went wrong while loading data from EBS");
			loadDataFromEbsResPo.setError(e.getMessage());
			return new ResponseEntity<LoadDataFromEbsResPo>(loadDataFromEbsResPo, HttpStatus.BAD_REQUEST);
		} catch (ValidationException e) {
			log.error(e.getMessage() + "ValidationException");
			loadDataFromEbsResPo.setMessage("Something went wrong while loading data from EBS");
			loadDataFromEbsResPo.setError(e.getMessage());
			return new ResponseEntity<LoadDataFromEbsResPo>(loadDataFromEbsResPo, HttpStatus.OK);
		} catch (Exception e) {
			log.error(e.getMessage());
			loadDataFromEbsResPo.setMessage("Something went wrong while loading data from EBS");
			loadDataFromEbsResPo.setError(e.getMessage());
		}
		return new ResponseEntity<LoadDataFromEbsResPo>(loadDataFromEbsResPo, new HttpHeaders(), HttpStatus.OK);
	}
	

	@ApiOperation(value = "This Api loads ebs data")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error"),
			@ApiResponse(code = 400, message = "Bad Request") })
	@PostMapping("/loaddatafromebsv1")
	public ResponseEntity<LoadDataFromEbsResPo> loadDataFromEbsV1(@RequestBody LoadDataFromEbsReqPo loadDataFromEbsReqPo,
			HttpServletRequest request) throws ConvertRiteException {
		log.info("Start of loadDataFromEbs Method in Controller ###");
		LoadDataFromEbsResPo loadDataFromEbsResPo = new LoadDataFromEbsResPo();
		try {
			if (Validations.isNullOrEmpty(loadDataFromEbsReqPo.getDbLink())
					|| loadDataFromEbsReqPo.getSourceTemplateId() == null
					//|| Validations.isNullOrEmpty(loadDataFromEbsReqPo.getObjectCode())
					|| Validations.isNullOrEmpty(loadDataFromEbsReqPo.getSourceSystem())
					|| Validations.isNullOrEmpty(loadDataFromEbsReqPo.getVersion())
					|| Validations.isNullOrEmpty(loadDataFromEbsReqPo.getBatchName()))
				throw new BadRequestException(
						"dbLink,sourceTemplateId,sourceSystem,batchName and version are Mandatory fields");
			loadDataFromEbsResPo = ebsIntegrationService.loadDataFromEbsV1(loadDataFromEbsReqPo, request);
		} catch (BadRequestException e) {
			log.error(e.getMessage() + "BadRequestException");
			loadDataFromEbsResPo.setError("Something went wrong while loading data from EBS");
			loadDataFromEbsResPo.setMessage(e.getMessage());
			return new ResponseEntity<LoadDataFromEbsResPo>(loadDataFromEbsResPo, HttpStatus.BAD_REQUEST);
		} catch (ValidationException e) {
			log.error(e.getMessage() + "ValidationException");
			loadDataFromEbsResPo.setError("Something went wrong while loading data from EBS");
			loadDataFromEbsResPo.setMessage(e.getMessage());
			return new ResponseEntity<LoadDataFromEbsResPo>(loadDataFromEbsResPo, HttpStatus.OK);
		} catch (Exception e) {
			log.error(e.getMessage());
			loadDataFromEbsResPo.setMessage("Please contact System Administrator there is an error while processing the request");
			loadDataFromEbsResPo.setError(e.getMessage());
		}
		return new ResponseEntity<LoadDataFromEbsResPo>(loadDataFromEbsResPo, new HttpHeaders(), HttpStatus.OK);
	}

	@ApiOperation(value = "This Api loads ebs metadata")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error"),
			@ApiResponse(code = 400, message = "Bad Request") })
	@PostMapping("/loadmetadatafromebs")
	public ResponseEntity<LoadMetaDataFromEbsRes> loadMetaDataFromEbs(
			@RequestBody LoadMetaDataFromEbsReqPo loadMetaDataFromEbsReqPo) {
		log.info("Start of loadMetaDataFromEbs Method in Controller ###");
		LoadMetaDataFromEbsRes loadMetaDataFromEbsRes = new LoadMetaDataFromEbsRes();
		Long metaDataTableId = null;
		try {
			if (Validations.isNullOrEmpty(loadMetaDataFromEbsReqPo.getDbLink())
					|| Validations.isNullOrEmpty(loadMetaDataFromEbsReqPo.getMetaDataTableName())
					|| Validations.isNullOrEmpty(loadMetaDataFromEbsReqPo.getObjectCode())
					|| Validations.isNullOrEmpty(loadMetaDataFromEbsReqPo.getSourceSystem())
					|| Validations.isNullOrEmpty(loadMetaDataFromEbsReqPo.getVersion()))
				throw new BadRequestException(
						"dbLink,metaDataTableName,objectCode,sourceSystem and version are Mandatory fields");
			metaDataTableId = xxrSourceTablesRepository.getTableId(loadMetaDataFromEbsReqPo.getMetaDataTableName());
			if (metaDataTableId != null)
				throw new ValidationException("This MetaDataTableName Already exists,Please give different name");

			loadMetaDataFromEbsRes = ebsIntegrationService.loadMetaDataFromEbs(loadMetaDataFromEbsReqPo);
		} catch (BadRequestException e) {
			log.error(e.getMessage() + "BadRequestException");
			loadMetaDataFromEbsRes.setMessage("Something went wrong while loading meta data from EBS");
			loadMetaDataFromEbsRes.setError(e.getMessage());
			return new ResponseEntity<LoadMetaDataFromEbsRes>(loadMetaDataFromEbsRes, HttpStatus.BAD_REQUEST);
		} catch (ValidationException e) {
			log.error(e.getMessage() + "ValidationException");
			loadMetaDataFromEbsRes.setMessage("Something went wrong while loading meta data from EBS");
			loadMetaDataFromEbsRes.setError(e.getMessage());
			return new ResponseEntity<LoadMetaDataFromEbsRes>(loadMetaDataFromEbsRes, HttpStatus.OK);
		} catch (Exception e) {
			log.error(e.getMessage() + "Exception");
			loadMetaDataFromEbsRes.setMessage("Something went wrong while loading meta data from EBS");
			loadMetaDataFromEbsRes.setError(e.getMessage());
			return new ResponseEntity<LoadMetaDataFromEbsRes>(loadMetaDataFromEbsRes, new HttpHeaders(),
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<LoadMetaDataFromEbsRes>(loadMetaDataFromEbsRes, new HttpHeaders(), HttpStatus.OK);
	}

	@ApiOperation(value = "This Api is for uploading sql file")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@PostMapping("/uploadsqlfile")
	public ResponseEntity<XxrSrcSystemSqlClob> uploadSqlFile(@RequestParam("file") MultipartFile file,
			@RequestParam("sourceSystem") String sourceSystem, @RequestParam("objectCode") String objectCode,
			@RequestParam("version") String version) throws ConvertRiteException {
		log.info("Start of uploadSqlFile Method in Controller######");
		XxrSrcSystemSqlClob xxrSrcSystemSqlClob = new XxrSrcSystemSqlClob();
		try {
			xxrSrcSystemSqlClob = ebsIntegrationService.uploadSqlFile(file, sourceSystem, objectCode, version);
		} catch (Exception e) {
			e.printStackTrace();
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<XxrSrcSystemSqlClob>(xxrSrcSystemSqlClob, new HttpHeaders(), HttpStatus.OK);
	}
	
	
	@ApiOperation(value = "This Api is for uploading sql file")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@PostMapping("/saveebsview")
	public ResponseEntity<XxrSrcSystemSqlClob> saveEbsView(@RequestBody SaveEbsViewReqpo saveEbsViewReqpo) throws ConvertRiteException {
		log.info("Start of uploadSqlFile Method in Controller######");
		XxrSrcSystemSqlClob xxrSrcSystemSqlClob = new XxrSrcSystemSqlClob();
		try {
			xxrSrcSystemSqlClob = ebsIntegrationService.saveEbsView(saveEbsViewReqpo);
		} catch (Exception e) {
			e.printStackTrace();
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<XxrSrcSystemSqlClob>(xxrSrcSystemSqlClob, new HttpHeaders(), HttpStatus.OK);
	}
	
	@ApiOperation(value = "This Api is for downloading ebs view sqlfile")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/downloadebsview")
	public void downloadEbsView(@RequestParam("id") Long id, HttpServletResponse response) throws ConvertRiteException {
		log.info("Start of downloadebsview Method in Controller######");
		// response.setContentType("text/plain");
		response.addHeader("content-type", "text/plain; charset=utf-8");
		try {
			ebsIntegrationService.downloadEbsView(id, response);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
	}

	@ApiOperation(value = "This Api is to fetch id based on objectcode,srcsystem &version")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/getsrcsystemlobId")
	public ResponseEntity<SrcSystemLobIdRes> getSrcSystemLobId(@RequestParam("objectCode") String objectCode,
			@RequestParam("sourceSystem") String sourceSystem, @RequestParam("version") String version)
			throws ConvertRiteException {
		log.info("Start of getSrcSystemLobId Method in Controller######");
		SrcSystemLobIdRes srcSystemLobIdRes = new SrcSystemLobIdRes();
		try {
			srcSystemLobIdRes = ebsIntegrationService.getSrcSystemLobId(objectCode, sourceSystem, version);
		} catch (Exception e) {
			log.error(e.getMessage());
			srcSystemLobIdRes
					.setMessage("Please contact System Administrator there is an error while processing the request");
			srcSystemLobIdRes.setError(e.getMessage());
			return new ResponseEntity<SrcSystemLobIdRes>(srcSystemLobIdRes, HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<SrcSystemLobIdRes>(srcSystemLobIdRes, HttpStatus.OK);
	}


}
