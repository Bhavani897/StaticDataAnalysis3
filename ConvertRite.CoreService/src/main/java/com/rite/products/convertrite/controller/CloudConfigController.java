package com.rite.products.convertrite.controller;

import java.util.ArrayList;
import java.util.List;

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
import org.springframework.web.bind.annotation.RestController;

import com.rite.products.convertrite.Validations.Validations;
import com.rite.products.convertrite.exception.BadRequestException;
import com.rite.products.convertrite.exception.ConvertRiteException;
import com.rite.products.convertrite.exception.ValidationException;
import com.rite.products.convertrite.po.SaveCloudConfigDetailsResPo;
import com.rite.products.convertrite.po.SaveCloudConfigReqPo;
import com.rite.products.convertrite.po.XxrCloudConfigResPo;
import com.rite.products.convertrite.service.CloudConfigService;

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

@RestController
public class CloudConfigController {

	private static final Logger log = LoggerFactory.getLogger(CloudConfigController.class);

	@Autowired
	CloudConfigService cloudConfigService;

	@ApiOperation(value = "Get Cloud Configuration details")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/getcloudconfig")
	@Cacheable(value = "cloudconfigdetailscache")
	public ResponseEntity<List<XxrCloudConfigResPo>> getCloudConfig() throws ConvertRiteException {
		log.info("Start of getCloudConfig Method in CloudConfigController :::");
		List<XxrCloudConfigResPo> xxrCloudDataProcessConfig = new ArrayList<>();
		try {
			xxrCloudDataProcessConfig = cloudConfigService.getCloudConfig();

		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<List<XxrCloudConfigResPo>>(xxrCloudDataProcessConfig, new HttpHeaders(),
				HttpStatus.OK);
	}

	@ApiOperation(value = "This api is to save cloud config details")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error"),
			@ApiResponse(code = 400, message = "Bad Request") })
	@PostMapping("/savecloudconfigdetails")
	@CacheEvict(value = "cloudconfigdetailscache", allEntries = true)
	public ResponseEntity<SaveCloudConfigDetailsResPo> saveCloudConfigDetails(
			@RequestBody SaveCloudConfigReqPo saveCloudConfigReqPo) {
		log.info("Start of saveCloudConfigDetails Method in Controller ###");
		SaveCloudConfigDetailsResPo saveCloudConfigDetailsResPo = new SaveCloudConfigDetailsResPo();
		try {
			if (Validations.isNullOrEmpty(saveCloudConfigReqPo.getCloudUrl())
					|| Validations.isNullOrEmpty(saveCloudConfigReqPo.getUserName())
					|| Validations.isNullOrEmpty(saveCloudConfigReqPo.getPassword())
					|| saveCloudConfigReqPo.getPodId() == null || saveCloudConfigReqPo.getProjectId() == null)
				throw new BadRequestException("cloudUrl,userName,password,podId and projectId are Mandatory fields");

			saveCloudConfigDetailsResPo = cloudConfigService.saveCloudConfigDetails(saveCloudConfigReqPo);

		} catch (ValidationException e) {
			log.error(e.getMessage() + "ValidationException");
			saveCloudConfigDetailsResPo.setMessage("Error while saving cloud config details");
			saveCloudConfigDetailsResPo.setError(e.getMessage());
			return new ResponseEntity<SaveCloudConfigDetailsResPo>(saveCloudConfigDetailsResPo, HttpStatus.OK);
		} catch (BadRequestException e) {
			log.error(e.getMessage() + "BadRequestException");
			saveCloudConfigDetailsResPo.setMessage("Error while saving cloud config details");
			saveCloudConfigDetailsResPo.setError(e.getMessage());
			return new ResponseEntity<SaveCloudConfigDetailsResPo>(saveCloudConfigDetailsResPo, HttpStatus.BAD_REQUEST);
		} catch (Exception e) {
			log.error(e.getMessage() + "Exception");
			saveCloudConfigDetailsResPo.setMessage("Error while saving cloud config details");
			saveCloudConfigDetailsResPo.setError(e.getMessage());
			return new ResponseEntity<SaveCloudConfigDetailsResPo>(saveCloudConfigDetailsResPo,
					HttpStatus.INTERNAL_SERVER_ERROR);
		}

		return new ResponseEntity<SaveCloudConfigDetailsResPo>(saveCloudConfigDetailsResPo, new HttpHeaders(),
				HttpStatus.OK);

	}

}
