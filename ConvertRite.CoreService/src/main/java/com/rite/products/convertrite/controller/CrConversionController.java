package com.rite.products.convertrite.controller;

import com.rite.products.convertrite.Validations.Validations;
import com.rite.products.convertrite.exception.BadRequestException;
import com.rite.products.convertrite.exception.ConvertRiteException;
import com.rite.products.convertrite.model.CrProcessRequestsView;
import com.rite.products.convertrite.po.*;
import com.rite.products.convertrite.service.CrConversionService;
import com.rite.products.convertrite.service.CrErpIntegrationService;
import com.rite.products.convertrite.service.ErpIntegrationService;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Produces;
import java.util.List;

@RestController
@RequestMapping("/api/convertritecore/conversion")
@Slf4j
public class CrConversionController {
	@Autowired
	CrConversionService crConversionService;
	@Autowired
	ErpIntegrationService erpIntegrationService;
	@Autowired
	CrErpIntegrationService crErpIntegrationService;

	@ApiOperation(value = "This Api returns templates state")
	@GetMapping("/gettemplatestate")
	public ResponseEntity<?> getTemplatesState() {
		try {
			return new ResponseEntity<>(crConversionService.getTemplateState(), HttpStatus.OK);
		} catch (Exception e) {
			log.error(e.getMessage());
			return new ResponseEntity<>(e.getCause(), HttpStatus.EXPECTATION_FAILED);
		}
	}

	@ApiOperation(value = "This Api returns template statistics")
	@GetMapping("/gettemplatestatistics")
	public ResponseEntity<?> getTemplatesStatistics() {
		try {
			return new ResponseEntity<>(crConversionService.getTemplateStatistics(), HttpStatus.OK);
		} catch (Exception e) {
			log.error(e.getMessage());
			return new ResponseEntity<>(e.getCause(), HttpStatus.EXPECTATION_FAILED);
		}
	}

	@ApiOperation(value = "This Api returns transform stats")
	@GetMapping("/insertTransformStats")
	public ResponseEntity<?> insertTransformStats(@RequestParam String userId , @RequestParam Long cloudTemplateId,
												  @RequestParam String batchName) {
		try {
			return new ResponseEntity<>(crConversionService.insertTransformStats(userId,cloudTemplateId,batchName), HttpStatus.OK);
		} catch (Exception e) {
			log.error(e.getMessage());
			return new ResponseEntity<>(e.getCause(), HttpStatus.EXPECTATION_FAILED);
		}
	}


	@ApiOperation(value = "This Api returns status of process requests")
	@GetMapping("/getprocessrequests")
	public ResponseEntity<?> getProcessRequests(CrProcessRequestsPagePo crProcessRequestsPagePo) {
		try {
			HttpHeaders httpHeaders = new HttpHeaders();
			List<CrProcessRequestsView> requests = crConversionService.getProcessRequests(crProcessRequestsPagePo, httpHeaders);
			return new ResponseEntity<>(requests, httpHeaders, HttpStatus.OK);
		} catch (Exception e) {
			log.error(e.getMessage());
			return new ResponseEntity<>(e.getCause(), HttpStatus.EXPECTATION_FAILED);
		}
	}

	@ApiOperation(value = "This Api is for processing Job, 'type' Param value should be Validation or Conversion or ReProcesses")
	@PostMapping("/processjobv1")
	public ResponseEntity<ProcessJobPo> processJobV1(@RequestParam("cloudTemplateName") String cloudTemplateName,
													 @RequestParam("batchName") String batchName,
													 @RequestParam("type") String type, HttpServletRequest request)
			throws ConvertRiteException {
		ProcessJobPo processJobPo = new ProcessJobPo();
		if (type.matches("(?i)Validation|Conversion|ReProcesses")) {
			try {
				if (cloudTemplateName != null && !Validations.isNullOrEmpty(type))
					processJobPo = crConversionService.processJobV1(cloudTemplateName, type, batchName, request);
				else
					throw new ConvertRiteException("Missing cloudTemplateName or type in the Request",
							HttpStatus.BAD_REQUEST);
			} catch (Exception e) {
				log.error(e.getMessage());
				throw new ConvertRiteException(
						"Please contact System Administrator there is an error while processing the request",
						HttpStatus.INTERNAL_SERVER_ERROR);
			}
		} else {
			throw new ConvertRiteException("type Param value should be Validation or Conversion or ReProcesses",
					HttpStatus.BAD_REQUEST);
		}
		return new ResponseEntity<ProcessJobPo>(processJobPo, new HttpHeaders(), HttpStatus.OK);
	}
	@ApiOperation(value = "This Api is to Transform the Data to Cloud")
	@PostMapping("/transformDataToCloud")
	public ResponseEntity<Object> transformDataToCloud(@RequestParam("cloudTemplateName") String cloudTemplateName, @RequestParam("pReprocessFlag") String pReprocessFlag, @RequestParam("pBatchFlag") String pBatchFlag, @RequestParam("pBatchName") String pBatchName,HttpServletRequest request ) throws Exception{
		String tenantId=request.getHeader("X-TENANT-ID");
		String userId=request.getHeader("userId");
		return  crConversionService.transformDataToCloud(cloudTemplateName,pReprocessFlag, pBatchFlag,pBatchName,tenantId,userId);
	}
	@ApiOperation(value = "This Api is for downloading fbdi from ftp")
	@GetMapping("/downloadfbdi")
	public void downloadFbdi(@RequestParam("cloudTemplateId") Long cloudTemplateId, @RequestParam("batchName") String batchName, HttpServletResponse response)
			throws Exception {
		try {
			crConversionService.downloadFbdi(cloudTemplateId,batchName, response);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new Exception(e.getMessage());
		}
	}

	@ApiOperation(value = "This Api is for generating hdl from lob")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/generatehdl")
	public void generateHdlFromLob(@RequestParam("cloudTemplateId") String cloudTemplateId,
								   @RequestParam("batchName") String batchName, @RequestParam("isIntialLoad") String isIntialLoad, HttpServletResponse response) throws Exception {

		try {
			crConversionService.generateHdlFromLob(cloudTemplateId, batchName,isIntialLoad, response);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new Exception(e.getMessage());
		}

	}
	@ApiOperation(value = "This Api is to load and import data")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error"),
			@ApiResponse(code = 400, message = "Bad Request") })
	@PostMapping("/loadandimportdatav1")
	public ResponseEntity<LoadandImportDataResPo> loadAndImportDataV1(
			@RequestBody LoadandImportDataReqPo loadandImportDataReqPo,@RequestHeader("Authorization") String bearerToken) {
		log.info("Start of loadAndImportData Method in Controller ###");

		LoadandImportDataResPo loadandImportDataResPo = new LoadandImportDataResPo();
		try {
			if (loadandImportDataReqPo.getCloudTemplateId() == null
					|| Validations.isNullOrEmpty(loadandImportDataReqPo.getBatchName()))
				throw new BadRequestException("cloudTemplateId and batchName are Mandatory fields");

				loadandImportDataResPo = erpIntegrationService.loadAndImportDataV2(loadandImportDataReqPo, bearerToken);

		}  catch (Exception e) {
			log.error(e.getMessage());
			loadandImportDataResPo
					.setMessage("Please contact System Administrator there is an error while processing the request");
			loadandImportDataResPo.setError(e.getMessage());
			return new ResponseEntity<LoadandImportDataResPo>(loadandImportDataResPo, HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<LoadandImportDataResPo>(loadandImportDataResPo, new HttpHeaders(), HttpStatus.OK);
	}

	@ApiOperation(value = "This Api is to load and import data")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error"),
			@ApiResponse(code = 400, message = "Bad Request") })
	@GetMapping("/getcldimportstatus")
	public ResponseEntity<BasicResponsePo> getCldImportStatus(@RequestParam("cldTemplateId") Long cldTemplateId,@RequestParam("batchName") String batchName){
		return new ResponseEntity<BasicResponsePo>(erpIntegrationService.getCldImportStatus(cldTemplateId,batchName),HttpStatus.OK);
	}

	@ApiOperation(value = "This api is to download exported output in zip")
	@ApiResponses(value = {@ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error"),
			@ApiResponse(code = 400, message = "Bad Request")})
	@GetMapping({"/downloadexportoutput", "/downloadessjobdetails"})
	@Produces("application/zip")
	public ResponseEntity<byte[]> downloadEssJobDetails(@RequestParam("resultId") String requestId, @RequestHeader("X-TENANT-ID") Long podId, @RequestHeader("Authorization") String bearerToken)
			throws ConvertRiteException {
		log.info("Start of downloadessjobdetails Method in Controller ###");
		byte[] resp = null;
		try {
			resp = crErpIntegrationService.downloadESSJobExecutionDetails(requestId, podId, bearerToken);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		HttpHeaders responseHeaders = new HttpHeaders();
		responseHeaders.set("Content-Type", "application/zip");
		responseHeaders.set("Content-Disposition", "attachment; filename=" + requestId + ".zip");
		return new ResponseEntity<byte[]>(resp, responseHeaders, HttpStatus.OK);
	}
}
