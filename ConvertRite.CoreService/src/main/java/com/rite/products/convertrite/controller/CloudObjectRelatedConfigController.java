package com.rite.products.convertrite.controller;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.rite.products.convertrite.Validations.Validations;
import com.rite.products.convertrite.exception.BadRequestException;
import com.rite.products.convertrite.exception.ConvertRiteException;
import com.rite.products.convertrite.exception.ValidationException;
import com.rite.products.convertrite.model.XxrCloudConfig;
import com.rite.products.convertrite.po.SaveCloudObjectRelatedConfigDetailsResPo;
import com.rite.products.convertrite.po.SaveCloudObjectRelatedConfigReqPo;
import com.rite.products.convertrite.service.CloudObjectRelatedConfigService;

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

@RestController
public class CloudObjectRelatedConfigController {
	
	private static final Logger log = LoggerFactory.getLogger(CloudObjectRelatedConfigController.class);
	
	@Autowired
	CloudObjectRelatedConfigService CloudObjectRelatedConfigService;

	@ApiOperation(value = "Get Cloud object realted Configuration details")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/getcloudobjectrealtedconfig")
	public ResponseEntity<List<XxrCloudConfig>> getCloudObjectRelatedConfig() throws ConvertRiteException {
		log.info("Start of getCloudObjectRelatedConfig Method in CloudObjectRelatedConfigController :::");
		List<XxrCloudConfig> xxrCloudConfig = new ArrayList<>();
		try {
			xxrCloudConfig = CloudObjectRelatedConfigService.getCloudObjectRelatedConfig();

		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<List<XxrCloudConfig>>(xxrCloudConfig, new HttpHeaders(),
				HttpStatus.OK);
	}

	@ApiOperation(value = "This api is to save cloud object related config details")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error"),
			@ApiResponse(code = 400, message = "Bad Request") })
	@PostMapping("/savecloudobjectrelatedconfig")
	public ResponseEntity<SaveCloudObjectRelatedConfigDetailsResPo> saveCloudObjectRelatedConfig(
			@RequestBody SaveCloudObjectRelatedConfigReqPo saveCloudObjectRelatedConfigReqPo) {
		log.info("Start of saveCloudObjectRelatedConfig Method in Controller ###");

		SaveCloudObjectRelatedConfigDetailsResPo saveCloudConfigDetailsResPo = new SaveCloudObjectRelatedConfigDetailsResPo();
		try {
			if (Validations.isNullOrEmpty(saveCloudObjectRelatedConfigReqPo.getObjectCode())
					|| Validations.isNullOrEmpty(saveCloudObjectRelatedConfigReqPo.getSheetName())
					|| Validations.isNullOrEmpty(saveCloudObjectRelatedConfigReqPo.getXmlsmFileName())
					|| Validations.isNullOrEmpty(saveCloudObjectRelatedConfigReqPo.getCtlFileName())
					|| Validations.isNullOrEmpty(saveCloudObjectRelatedConfigReqPo.getInterfaceTableName())
					|| Validations.isNullOrEmpty(saveCloudObjectRelatedConfigReqPo.getRejectionTableName())
					|| Validations.isNullOrEmpty(saveCloudObjectRelatedConfigReqPo.getEssJobSatusColumn()))
				throw new BadRequestException("objectCode,sheetName,xlsmFileName,ctlFileName,rejectionTableName,essJobStatusCol and interfaceTableName are Mandatory fields");

			saveCloudConfigDetailsResPo = CloudObjectRelatedConfigService.saveCloudObjectRelatedConfig(saveCloudObjectRelatedConfigReqPo);

		} catch (ValidationException e) {
			log.error(e.getMessage() + "ValidationException");
			saveCloudConfigDetailsResPo.setMessage("Error while saving cloud object related config details");
			saveCloudConfigDetailsResPo.setError(e.getMessage());
			return new ResponseEntity<SaveCloudObjectRelatedConfigDetailsResPo>(saveCloudConfigDetailsResPo, HttpStatus.OK);
		} catch (BadRequestException e) {
			log.error(e.getMessage() + "BadRequestException");
			saveCloudConfigDetailsResPo.setMessage("Error while saving cloud object related config details");
			saveCloudConfigDetailsResPo.setError(e.getMessage());
			return new ResponseEntity<SaveCloudObjectRelatedConfigDetailsResPo>(saveCloudConfigDetailsResPo, HttpStatus.BAD_REQUEST);
		} catch (Exception e) {
			log.error(e.getMessage() + "Exception");
			saveCloudConfigDetailsResPo.setMessage("Error while saving cloud object related config details");
			saveCloudConfigDetailsResPo.setError(e.getMessage());
			return new ResponseEntity<SaveCloudObjectRelatedConfigDetailsResPo>(saveCloudConfigDetailsResPo,
					HttpStatus.INTERNAL_SERVER_ERROR);
		}

		return new ResponseEntity<SaveCloudObjectRelatedConfigDetailsResPo>(saveCloudConfigDetailsResPo, new HttpHeaders(),
				HttpStatus.OK);

	}

}
