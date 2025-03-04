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
import com.rite.products.convertrite.po.DateConfigurationResPo;
import com.rite.products.convertrite.po.SaveDateConfigurationReqPo;
import com.rite.products.convertrite.po.SaveDateConfigurationResPo;
import com.rite.products.convertrite.service.DateConfigurationService;

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;



@RestController
public class DateConfigurationController {
	
	@Autowired
	DateConfigurationService dateConfigurationService;
	
	private static final Logger log=LoggerFactory.getLogger(DateConfigurationController.class);
	
	@ApiOperation(value = "This api is to get date configuration data")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error"),
			@ApiResponse(code = 400, message = "Bad Request") })
	@GetMapping("/getdateconfiguration")
	public ResponseEntity<List<DateConfigurationResPo>> getDateConfiguration()
			throws ConvertRiteException {
		log.info("Start of getDateConfiguration Method in Controller ###");
		List<DateConfigurationResPo> dateConfigurationLi = new ArrayList<>();
		try {
			dateConfigurationLi = dateConfigurationService.getDateConfiguration();
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<List<DateConfigurationResPo>>(dateConfigurationLi, new HttpHeaders(), HttpStatus.OK);
	}
	
	@ApiOperation(value = "This api is to save date configuration")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error"),
			@ApiResponse(code = 400, message = "Bad Request") })
	@PostMapping("/savedateconfiguration")
	public ResponseEntity<SaveDateConfigurationResPo> saveDateConfiguration(
			@RequestBody SaveDateConfigurationReqPo saveDateConfigurationReqPo) {
		log.info("Start of saveDateConfiguration Method in Controller ###");

		SaveDateConfigurationResPo saveDateConfigurationResPo = new SaveDateConfigurationResPo();
		try {
			if (Validations.isNullOrEmpty(saveDateConfigurationReqPo.getCloudDateFormat())
					|| Validations.isNullOrEmpty(saveDateConfigurationReqPo.getSourceDateFormat())
					||saveDateConfigurationReqPo.getPodId()==null
					||saveDateConfigurationReqPo.getProjectId()==null || saveDateConfigurationReqPo.getObjectId()==null
					||saveDateConfigurationReqPo.getParentObjectId()==null)
				throw new BadRequestException(
						"cloudDateFormat,sourceDateFormat,podId,objectId,parentObjectId and projectId are Mandatory fields");

			saveDateConfigurationResPo = dateConfigurationService.saveDateConfiguration(saveDateConfigurationReqPo);

		} catch (ValidationException e) {
			log.error(e.getMessage() + "ValidationException");
			saveDateConfigurationResPo.setMessage("Error while saving date configuration");
			saveDateConfigurationResPo.setError(e.getMessage());
			return new ResponseEntity<SaveDateConfigurationResPo>(saveDateConfigurationResPo, HttpStatus.OK);
		} catch (BadRequestException e) {
			log.error(e.getMessage() + "BadRequestException");
			saveDateConfigurationResPo.setMessage("Error while saving date configuration");
			saveDateConfigurationResPo.setError(e.getMessage());
			return new ResponseEntity<SaveDateConfigurationResPo>(saveDateConfigurationResPo,
					HttpStatus.BAD_REQUEST);
		} catch (Exception e) {
			log.error(e.getMessage() + "Exception");
			saveDateConfigurationResPo.setMessage("Error while saving date configuration");
			saveDateConfigurationResPo.setError(e.getMessage());
			return new ResponseEntity<SaveDateConfigurationResPo>(saveDateConfigurationResPo,
					HttpStatus.INTERNAL_SERVER_ERROR);
		}

		return new ResponseEntity<SaveDateConfigurationResPo>(saveDateConfigurationResPo, new HttpHeaders(),
				HttpStatus.OK);

	}
	

}
