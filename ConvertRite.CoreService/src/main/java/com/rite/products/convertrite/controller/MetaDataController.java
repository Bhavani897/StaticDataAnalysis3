package com.rite.products.convertrite.controller;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import com.rite.products.convertrite.Validations.Validations;
import com.rite.products.convertrite.exception.ConvertRiteException;
import com.rite.products.convertrite.exception.ValidationException;
import com.rite.products.convertrite.po.LoadMetaDataFromCloudRes;
import com.rite.products.convertrite.po.MetaDataResPo;
import com.rite.products.convertrite.po.UploadFileResponse;
import com.rite.products.convertrite.respository.XxrCloudDataProcessRepository;
import com.rite.products.convertrite.respository.XxrCloudTableRepository;
import com.rite.products.convertrite.service.LoadMetaDataService;

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

@RestController
public class MetaDataController {

	private static final Logger log = LoggerFactory.getLogger(MetaDataController.class);

	@Autowired
	LoadMetaDataService loadMetaDataService;
	@Autowired
	XxrCloudTableRepository xxrCloudTableRepository;
	@Autowired
	XxrCloudDataProcessRepository xxrCloudDataProcessRepository;

	@ApiOperation(value = "This Api helps to Load Source MetaData Records,Pass metaDataFileType value as 'TABLE OR COLUMN'")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error"),
			@ApiResponse(code = 400, message = "Bad Request") })
	@PostMapping("/processsourcemetadatarec")
	public ResponseEntity<MetaDataResPo> processSourceRecords(@RequestParam("fileName") String fileName,
			@RequestParam("metaDataFileType") String metaDataFileType, HttpServletRequest request)
			throws ConvertRiteException {
		log.info("Start of processSourceRecords Method in Controller ###");
		log.debug("fileName:::::::" + fileName);
		String status = "";
		MetaDataResPo metaDataRes = new MetaDataResPo();
		if ("TABLE".equalsIgnoreCase(metaDataFileType) || "COLUMN".equalsIgnoreCase(metaDataFileType)) {
			try {
				if (!(Validations.isNullOrEmpty(fileName))) {
					status = loadMetaDataService.processSourceRecords(fileName, metaDataFileType, request);
					if (status.equalsIgnoreCase("Y"))
						metaDataRes.setMessage("success");
					else
						metaDataRes.setError("something went wrong while processing");
				} else
					throw new ConvertRiteException("Missing  fileName in the Request", HttpStatus.BAD_REQUEST);
			} catch (Exception e) {
				log.error(e.getMessage());
				throw new ConvertRiteException(
						"Please contact System Administrator there is an error while processing the request",
						HttpStatus.INTERNAL_SERVER_ERROR);
			}
		} else
			throw new ConvertRiteException("metaDataFileType Value Should be TABLE OR COLUMN", HttpStatus.BAD_REQUEST);

		return new ResponseEntity<MetaDataResPo>(metaDataRes, new HttpHeaders(), HttpStatus.OK);
	}

	@ApiOperation(value = "This Api helps to Load Cloud MetaData Records,Pass metaDataFileType value as 'TABLE OR COLUMN'")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error"),
			@ApiResponse(code = 400, message = "Bad Request") })
	@PostMapping("/processcloudmetadatarec")
	public ResponseEntity<MetaDataResPo> processCloudRecords(@RequestParam("fileName") String fileName,
			@RequestParam("metaDataFileType") String metaDataFileType, HttpServletRequest request)
			throws ConvertRiteException {
		log.info("Start of processCloudRecords Method in Controller ###");
		log.debug("fileName:::::::" + fileName);
		String status = "";
		MetaDataResPo metaDataRes = new MetaDataResPo();
		if ("TABLE".equalsIgnoreCase(metaDataFileType) || "COLUMN".equalsIgnoreCase(metaDataFileType)) {
			try {
				if (!(Validations.isNullOrEmpty(fileName))) {
					status = loadMetaDataService.processCloudRecords(fileName, metaDataFileType, request);
					if (status.equalsIgnoreCase("Y"))
						metaDataRes.setMessage("success");
					else
						metaDataRes.setError("something went wrong while processing");
				} else
					throw new ConvertRiteException("Missing objectCode or fileName in the Request",
							HttpStatus.BAD_REQUEST);
			} catch (Exception e) {
				log.error(e.getMessage());
				throw new ConvertRiteException(
						"Please contact System Administrator there is an error while processing the request",
						HttpStatus.INTERNAL_SERVER_ERROR);
			}
		} else
			throw new ConvertRiteException("metaDataFileType Value Should be TABLE OR COLUMN", HttpStatus.BAD_REQUEST);

		return new ResponseEntity<MetaDataResPo>(metaDataRes, new HttpHeaders(), HttpStatus.OK);
	}

	@ApiOperation(value = "This Api is for downloading SourceTemplate file,Pass metaDataFileType value as 'TABLE OR COLUMN' ")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/downloadsourcetemplate")
	public void downloadSourceFile(@RequestParam(name = "objectCode", required = false) String objectCode,
			@RequestParam("metaDataFileType") String metaDataFileType, @RequestParam("environment") String environment,HttpServletResponse response)
			throws ConvertRiteException {
		log.info("Start of downloadSourceFile Method in Controller######");
		response.setContentType("text/csv");

		if ("TABLE".equalsIgnoreCase(metaDataFileType) || "COLUMN".equalsIgnoreCase(metaDataFileType)) {
			if ("TABLE".equalsIgnoreCase(metaDataFileType)) {
				response.setHeader(HttpHeaders.CONTENT_DISPOSITION,
						"attachment; filename=Rite_Source_Table_Template.csv");
				if (Validations.isNullOrEmpty(objectCode)) {
					//throw new ConvertRiteException("ObjectCode is Mandatory in Request", HttpStatus.BAD_REQUEST);
				}} else if ("COLUMN".equalsIgnoreCase(metaDataFileType))
				response.setHeader(HttpHeaders.CONTENT_DISPOSITION,
						"attachment; filename=Rite_Source_MetaData_Template.csv");
			try {
				loadMetaDataService.downloadSourceFile(response.getWriter(), objectCode, metaDataFileType,environment);
			} catch (Exception e) {
				log.error(e.getMessage());
				throw new ConvertRiteException(
						"Please contact System Administrator there is an error while processing the request",
						HttpStatus.INTERNAL_SERVER_ERROR);
			}

		} else
			throw new ConvertRiteException("metaDataFileType Value Should be TABLE OR COLUMN", HttpStatus.BAD_REQUEST);
	}

	@ApiOperation(value = "This Api is for downloading CloudTemplate file,Pass metaDataFileType value as 'TABLE OR COLUMN' ")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/downloadcloudtemplate")
	public void downloadCloudFile(@RequestParam("objectCode") String objectCode,
			@RequestParam("metaDataFileType") String metaDataFileType, HttpServletResponse response)
			throws ConvertRiteException {
		log.info("Start of downloadCloudFile Method in Controller######");
		response.setContentType("text/csv");

		if ("TABLE".equalsIgnoreCase(metaDataFileType) || "COLUMN".equalsIgnoreCase(metaDataFileType)) {
			try {
				if ("TABLE".equalsIgnoreCase(metaDataFileType))
					response.setHeader(HttpHeaders.CONTENT_DISPOSITION,
							"attachment; filename=Rite_Cloud_Table_Template.csv");
				else if ("COLUMN".equalsIgnoreCase(metaDataFileType))
					response.setHeader(HttpHeaders.CONTENT_DISPOSITION,
							"attachment; filename=Rite_Cloud_MetaData_Template.csv");

				loadMetaDataService.downloadCloudFile(response.getWriter(), objectCode, metaDataFileType);
			} catch (Exception e) {
				log.error(e.getMessage());
				throw new ConvertRiteException(
						"Please contact System Administrator there is an error while processing the request",
						HttpStatus.INTERNAL_SERVER_ERROR);
			}
		} else
			throw new ConvertRiteException("metaDataFileType Value Should be TABLE OR COLUMN", HttpStatus.BAD_REQUEST);
	}

	@ApiOperation(value = "This Api is for uploading file,'type' field value should be MetaData or Data")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@PostMapping("/uploadfile")
	public ResponseEntity<UploadFileResponse> uploadFile(@RequestParam("file") MultipartFile file,
			@RequestParam("type") String type) throws ConvertRiteException {
		log.info("Start of uploadFile Method in Controller######");
		String fileName = "";
		try {
			fileName = loadMetaDataService.uploadFile(file);
		} catch (Exception e) {
			e.printStackTrace();
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<UploadFileResponse>(
				new UploadFileResponse(fileName, file.getContentType(), file.getSize()), new HttpHeaders(),
				HttpStatus.OK);
	}

	@ApiOperation(value = "This Api loads metadata from cloud")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error"),
			@ApiResponse(code = 400, message = "Bad Request") })
	@PostMapping("/loadmetadatafromcloud")
	public ResponseEntity<LoadMetaDataFromCloudRes> loadMetaDataFromCloud(@RequestParam("objectCode") String objectCode,
			@RequestParam("metaDataTableName") String metaDataTableName, @RequestParam("podId") Long podId,
			@RequestParam("projectId") Long projectId, HttpServletRequest request) throws ConvertRiteException {
		log.info("Start of loadMetaDataFromCloud Method in Controller ###");

		LoadMetaDataFromCloudRes loadMetaDataFromCloudRes = new LoadMetaDataFromCloudRes();
		try {
			loadMetaDataFromCloudRes = loadMetaDataService.loadMetaDataFromCloud(objectCode, metaDataTableName, podId,
					projectId, request);
		} catch (ValidationException e) {
			log.error(e.getMessage() + "ValidationException");
			loadMetaDataFromCloudRes.setMessage(e.getMessage());
			loadMetaDataFromCloudRes
					.setError("Please contact System Administrator there is an error while processing the request");
			return new ResponseEntity<LoadMetaDataFromCloudRes>(loadMetaDataFromCloudRes, HttpStatus.OK);
		} catch (Exception e) {
			log.error(e.getMessage() + "Exception");
			loadMetaDataFromCloudRes
					.setMessage("Please contact System Administrator there is an error while processing the request");
			loadMetaDataFromCloudRes.setError(e.getMessage());
			return new ResponseEntity<LoadMetaDataFromCloudRes>(loadMetaDataFromCloudRes, new HttpHeaders(),
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<LoadMetaDataFromCloudRes>(loadMetaDataFromCloudRes, new HttpHeaders(), HttpStatus.OK);
	}

	@ApiOperation(value = "This Api loads metadata from cloud")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error"),
			@ApiResponse(code = 400, message = "Bad Request") })
	@PostMapping("/loadrejectnmetadatafrmcld")
	public ResponseEntity<LoadMetaDataFromCloudRes> loadRejectionMetaDataFromCloud(
			@RequestParam("objectCode") String objectCode, @RequestParam("podId") Long podId,@RequestParam("metaDataTableName") String metaDataTableName,
			@RequestParam("projectId") Long projectId, HttpServletRequest request) throws ConvertRiteException {
		log.info("Start of loadRejectionMetaDataFromCloud Method in Controller ###");

		LoadMetaDataFromCloudRes loadMetaDataFromCloudRes = new LoadMetaDataFromCloudRes();
		try {
			loadMetaDataFromCloudRes = loadMetaDataService.loadRejectionMetaDataFromCloud(objectCode, podId,
					projectId, request,metaDataTableName);
		} catch (ValidationException e) {
			log.error(e.getMessage() + "ValidationException");
			loadMetaDataFromCloudRes.setMessage(e.getMessage());
			loadMetaDataFromCloudRes
					.setError("Please contact System Administrator there is an error while processing the request");
			return new ResponseEntity<LoadMetaDataFromCloudRes>(loadMetaDataFromCloudRes, HttpStatus.OK);
		} catch (Exception e) {
			log.error(e.getMessage() + "Exception");
			loadMetaDataFromCloudRes
					.setMessage("Please contact System Administrator there is an error while processing the request");
			loadMetaDataFromCloudRes.setError(e.getMessage());
			return new ResponseEntity<LoadMetaDataFromCloudRes>(loadMetaDataFromCloudRes, new HttpHeaders(),
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<LoadMetaDataFromCloudRes>(loadMetaDataFromCloudRes, new HttpHeaders(), HttpStatus.OK);
	}

	@ApiOperation(value = "This Api loads hdl metadata from cloud")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error"),
			@ApiResponse(code = 400, message = "Bad Request") })
	@PostMapping("/loadhdlcloudmetadata")
	public ResponseEntity<LoadMetaDataFromCloudRes> loadHdlCloudMetaData(@RequestParam("objectCode") String objectCode,
			@RequestParam("metaDataTableName") String metaDataTableName, @RequestParam("file") MultipartFile file)
			throws ConvertRiteException {
		log.info("Start of loadHdlCloudMetaData Method in Controller ###");

		LoadMetaDataFromCloudRes loadMetaDataFromCloudRes = new LoadMetaDataFromCloudRes();
		try {
			loadMetaDataFromCloudRes = loadMetaDataService.loadHdlCloudMetaData(objectCode, metaDataTableName, file);
		} catch (ValidationException e) {
			log.error(e.getMessage() + "ValidationException");
			loadMetaDataFromCloudRes.setMessage("Something went wrong while loading hdl metadata");
			loadMetaDataFromCloudRes.setError(e.getMessage());
			return new ResponseEntity<LoadMetaDataFromCloudRes>(loadMetaDataFromCloudRes, HttpStatus.OK);
		} catch (Exception e) {
			log.error(e.getMessage() + "Exception");
			loadMetaDataFromCloudRes.setMessage("Something went wrong while loading hdl metadata");
			loadMetaDataFromCloudRes.setError(e.getMessage());
			return new ResponseEntity<LoadMetaDataFromCloudRes>(loadMetaDataFromCloudRes, new HttpHeaders(),
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<LoadMetaDataFromCloudRes>(loadMetaDataFromCloudRes, new HttpHeaders(), HttpStatus.OK);
	}

	/*
	 * @ApiOperation(value = "This Api loads metadata from cloud")
	 * 
	 * @ApiResponses(value = { @ApiResponse(code = 200, message =
	 * "Successful Response"),
	 * 
	 * @ApiResponse(code = 500, message = "Server Side Error"),
	 * 
	 * @ApiResponse(code = 400, message = "Bad Request") })
	 * 
	 * @PostMapping("/loadmetadatafromcloud") public
	 * ResponseEntity<LoadMetaDataFromCloudRes>
	 * loadMetaDataFromCloud(@RequestParam("requestId") String requestId,
	 * 
	 * @RequestParam("metaDataTableName") String metaDataTableName,
	 * 
	 * @RequestParam("objectCode") String objectCode) {
	 * log.info("Start of loadMetaDataFromCloud Method in Controller ###");
	 * LoadMetaDataFromCloudRes loadMetaDataFromCloudRes = new
	 * LoadMetaDataFromCloudRes(); Long metaDataTableId = null; String status = "";
	 * try { XxrCloudDataProcess cloudDataProcess =
	 * xxrCloudDataProcessRepository.findAll().stream() .filter(x ->
	 * x.getRequestId().equalsIgnoreCase(requestId)).findFirst().get(); if
	 * (cloudDataProcess != null) { status = cloudDataProcess.getStatus(); if
	 * (status.equalsIgnoreCase("processing") ||
	 * status.equalsIgnoreCase("starting")) throw new
	 * ValidationException("Reconcile is still in process please wait for sometime"
	 * ); else if (status.equalsIgnoreCase("error")) throw new ValidationException(
	 * "Something went wrong while Reconcile,Please contact system administrator");
	 * else if (status.equalsIgnoreCase("completed")) { metaDataTableId =
	 * xxrCloudTableRepository.getTableId(metaDataTableName); if (metaDataTableId !=
	 * null) throw new ValidationException(
	 * "This MetaDataTableName Already exists,Please give different name");
	 * 
	 * loadMetaDataFromCloudRes =
	 * loadMetaDataService.loadMetaDataFromCloud(objectCode, metaDataTableName,
	 * requestId); } } } catch (ValidationException e) { log.error(e.getMessage() +
	 * "ValidationException"); loadMetaDataFromCloudRes.
	 * setMessage("Something went wrong while loading meta data from cloud");
	 * loadMetaDataFromCloudRes.setError(e.getMessage()); return new
	 * ResponseEntity<LoadMetaDataFromCloudRes>(loadMetaDataFromCloudRes,
	 * HttpStatus.OK); } catch (Exception e) { log.error(e.getMessage() +
	 * "Exception"); loadMetaDataFromCloudRes.
	 * setMessage("Something went wrong while loading meta data from cloud");
	 * loadMetaDataFromCloudRes.setError(e.getMessage()); return new
	 * ResponseEntity<LoadMetaDataFromCloudRes>(loadMetaDataFromCloudRes, new
	 * HttpHeaders(), HttpStatus.INTERNAL_SERVER_ERROR); } return new
	 * ResponseEntity<LoadMetaDataFromCloudRes>(loadMetaDataFromCloudRes, new
	 * HttpHeaders(), HttpStatus.OK);
	 * 
	 * }
	 */



}
