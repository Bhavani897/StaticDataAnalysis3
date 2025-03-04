package com.rite.products.convertrite.controller;

import java.util.ArrayList;
import java.util.List;

import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.rite.products.convertrite.Validations.Validations;
import com.rite.products.convertrite.exception.BadRequestException;
import com.rite.products.convertrite.exception.ConvertRiteException;
import com.rite.products.convertrite.exception.ValidationException;
import com.rite.products.convertrite.model.XxrCloudDataProcess;
import com.rite.products.convertrite.model.XxrCloudDataProcessView;
import com.rite.products.convertrite.po.CloudDataProcessPagReqPo;
import com.rite.products.convertrite.po.CloudDataProcessingReqPo;
import com.rite.products.convertrite.po.DeleteRes;
import com.rite.products.convertrite.service.CloudDataProcessingService;

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

@RestController
public class CloudDataProcessingController {

	private static final Logger log = LoggerFactory.getLogger(CloudDataProcessingController.class);

	@Autowired
	CloudDataProcessingService cloudDataProcessingService;

	@ApiOperation(value = "This Api Insert Query into cloud data process table ")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@PostMapping("/clouddataprocessingrequest")
	public ResponseEntity<XxrCloudDataProcess> cloudDataProcessingRequests(
			@RequestBody CloudDataProcessingReqPo cloudDataProcessingReqPo) throws ConvertRiteException {
		log.info("Start of cloudDataProcessingRequests in controller ###");
		XxrCloudDataProcess xxrCloudDataProcess = new XxrCloudDataProcess();
		try {
			if (Validations.isNullOrEmpty(cloudDataProcessingReqPo.getSqlQuery())
					|| Validations.isNullOrEmpty(cloudDataProcessingReqPo.getLookUpFlag())
					|| cloudDataProcessingReqPo.getPodId() == null || cloudDataProcessingReqPo.getProjectId() == null)
				throw new BadRequestException("SqlQuery,podId,projectId and LookupFlag are mandatory fields");
			log.debug("sqlquery:::::::::" + cloudDataProcessingReqPo.getSqlQuery());
			xxrCloudDataProcess = cloudDataProcessingService.cloudDataProcessingRequests(cloudDataProcessingReqPo);
		} catch (BadRequestException e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(e.getMessage(), HttpStatus.BAD_REQUEST);
		} catch (Exception e) {
			e.printStackTrace();
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<XxrCloudDataProcess>(xxrCloudDataProcess, new HttpHeaders(), HttpStatus.OK);
	}

	@ApiOperation(value = "This Api return all cloud data process requests")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/getallclouddatarequest")
	public ResponseEntity<List<XxrCloudDataProcessView>> getAllCloudDataRequests(
			CloudDataProcessPagReqPo cloudDataProcessPagReqPo) throws ConvertRiteException {
		log.info("Start of getAllCloudDataRequests in controller ###");
		List<XxrCloudDataProcessView> xxrCloudDataProcess = new ArrayList<>();
		HttpHeaders httpHeaders = new HttpHeaders();
		try {
			xxrCloudDataProcess = cloudDataProcessingService.getAllCloudDataRequests(cloudDataProcessPagReqPo,
					httpHeaders);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<List<XxrCloudDataProcessView>>(xxrCloudDataProcess, httpHeaders, HttpStatus.OK);
	}

	/*@ApiOperation(value = "This Api is to download csv file")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/downloadcsvfile")
	public void downloadCsvFile(@RequestParam("requestId") String requestId, HttpServletResponse response)
			throws ConvertRiteException {
		log.info("Start of downloadCsvFile in controller ###");

		try {
			response.setContentType("text/csv");
			response.setHeader(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=" + requestId + ".csv");
			cloudDataProcessingService.downloadCsvFile(requestId, response);

		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}

	}*/

	@ApiOperation(value = "Deleting the Adhoc data which having extraction flag as 'Y'' ")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 404, message = "Not Found"),
			@ApiResponse(code = 400, message = "Bad Request"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@DeleteMapping("/deleteadhocdata/{requestid}")
	public ResponseEntity<DeleteRes> deleteAdhocData(@PathVariable("requestid") String requestId) {
		DeleteRes deleteRes=new DeleteRes();
		log.info("start of  deleteAdhocData method##");
		try {
			if (Validations.isNullOrEmpty(requestId)) {
				throw new BadRequestException("requestId is mandatory field");
			} else {
				cloudDataProcessingService.deleteAdhocData(requestId);
				deleteRes.setMessage("Deleting data  By given requestid succesfully");
			}
		}
		catch (ValidationException e) {
			log.error(e.getMessage());
			deleteRes.setErrorMessage(e.getMessage());
			return new ResponseEntity<>(deleteRes, HttpStatus.NOT_FOUND);
		}
		catch (BadRequestException e) {
			log.error(e.getMessage());
			deleteRes.setErrorMessage(e.getMessage());
			return new ResponseEntity<>(deleteRes, HttpStatus.BAD_REQUEST);
		} catch (Exception e) {
			log.error(e.getMessage());
			deleteRes.setErrorMessage(e.getMessage());
			return new ResponseEntity<>(deleteRes, HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<>(deleteRes, HttpStatus.OK);
	}
}
