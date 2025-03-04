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
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.rite.products.convertrite.Validations.Validations;
import com.rite.products.convertrite.exception.BadRequestException;
import com.rite.products.convertrite.exception.ConvertRiteException;
import com.rite.products.convertrite.exception.ValidationException;
import com.rite.products.convertrite.model.XxrObjectCodeGrouping;
import com.rite.products.convertrite.po.SaveObjectCodeGroupingLinesPo;
import com.rite.products.convertrite.po.SaveObjectCodeGroupingLinesResPo;
import com.rite.products.convertrite.po.SaveObjectCodeGroupingPo;
import com.rite.products.convertrite.po.SaveObjectCodeGroupingResPo;
import com.rite.products.convertrite.po.XxrObjectCodeGroupingLinesResPo;
import com.rite.products.convertrite.service.ObjectGroupingService;

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

@RestController
public class ObjectGroupingController {
	
	private static final Logger log = LoggerFactory.getLogger(ObjectGroupingController.class);
	
	@Autowired
	ObjectGroupingService ObjectGroupingService;
	
	@ApiOperation(value = "This api is to save objectcodegrouping")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error"),
			@ApiResponse(code = 400, message = "Bad Request") })
	@PostMapping("/saveobjectcodegrouping")
	@CacheEvict(value="objectgroupingcache",allEntries = true)
	public ResponseEntity<SaveObjectCodeGroupingResPo> saveObjectCodeGrouping(
			@RequestBody SaveObjectCodeGroupingPo saveObjectCodeGroupingPo) {
		log.info("Start of saveObjectCodeGrouping Method in Controller ###");

		SaveObjectCodeGroupingResPo saveObjectCodeGroupingResPo = new SaveObjectCodeGroupingResPo();
		try {
			if (Validations.isNullOrEmpty(saveObjectCodeGroupingPo.getGroupName())
					|| Validations.isNullOrEmpty(saveObjectCodeGroupingPo.getCtlFile())
					|| Validations.isNullOrEmpty(saveObjectCodeGroupingPo.getVersion()))
				throw new BadRequestException(
						"groupName,ctlFile and version are Mandatory fields");

			saveObjectCodeGroupingResPo = ObjectGroupingService.saveObjectCodeGrouping(saveObjectCodeGroupingPo);

		} catch (ValidationException e) {
			log.error(e.getMessage() + "ValidationException");
			saveObjectCodeGroupingResPo.setMessage("Error while saving objectcode grouping");
			saveObjectCodeGroupingResPo.setError(e.getMessage());
			return new ResponseEntity<SaveObjectCodeGroupingResPo>(saveObjectCodeGroupingResPo, HttpStatus.OK);
		} catch (BadRequestException e) {
			log.error(e.getMessage() + "BadRequestException");
			saveObjectCodeGroupingResPo.setMessage("Error while saving objectcode grouping");
			saveObjectCodeGroupingResPo.setError(e.getMessage());
			return new ResponseEntity<SaveObjectCodeGroupingResPo>(saveObjectCodeGroupingResPo,
					HttpStatus.BAD_REQUEST);
		} catch (Exception e) {
			log.error(e.getMessage() + "Exception");
			saveObjectCodeGroupingResPo.setMessage("Error while saving objectcode grouping");
			saveObjectCodeGroupingResPo.setError(e.getMessage());
			return new ResponseEntity<SaveObjectCodeGroupingResPo>(saveObjectCodeGroupingResPo,
					HttpStatus.INTERNAL_SERVER_ERROR);
		}

		return new ResponseEntity<SaveObjectCodeGroupingResPo>(saveObjectCodeGroupingResPo, new HttpHeaders(),
				HttpStatus.OK);

	}
	
	@ApiOperation(value = "This api is to save objectcodegrouping Lines")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error"),
			@ApiResponse(code = 400, message = "Bad Request") })
	@PostMapping("/saveobjectcodegroupinglines")
	@CacheEvict(value="objectgroupinglinescache",allEntries = true)
	public ResponseEntity<SaveObjectCodeGroupingLinesResPo> saveObjectCodeGroupingLines(
			@RequestBody List<SaveObjectCodeGroupingLinesPo> saveObjectCodeGroupingLinesPo) {
		log.info("Start of saveObjectCodeGroupingLines Method in Controller ###");

		SaveObjectCodeGroupingLinesResPo saveObjectCodeGroupingLinesResPo = new SaveObjectCodeGroupingLinesResPo();
		try {
			
			saveObjectCodeGroupingLinesResPo = ObjectGroupingService.saveObjectCodeGroupingLines(saveObjectCodeGroupingLinesPo);

		} catch (ValidationException e) {
			log.error(e.getMessage() + "ValidationException");
			saveObjectCodeGroupingLinesResPo.setMessage("Error while saving objectcode grouping lines");
			saveObjectCodeGroupingLinesResPo.setError(e.getMessage());
			return new ResponseEntity<SaveObjectCodeGroupingLinesResPo>(saveObjectCodeGroupingLinesResPo, HttpStatus.OK);
		} catch (BadRequestException e) {
			log.error(e.getMessage() + "BadRequestException");
			saveObjectCodeGroupingLinesResPo.setMessage("Error while saving objectcode grouping lines");
			saveObjectCodeGroupingLinesResPo.setError(e.getMessage());
			return new ResponseEntity<SaveObjectCodeGroupingLinesResPo>(saveObjectCodeGroupingLinesResPo,
					HttpStatus.BAD_REQUEST);
		} catch (Exception e) {
			log.error(e.getMessage() + "Exception");
			saveObjectCodeGroupingLinesResPo.setMessage("Error while saving objectcode grouping lines");
			saveObjectCodeGroupingLinesResPo.setError(e.getMessage());
			return new ResponseEntity<SaveObjectCodeGroupingLinesResPo>(saveObjectCodeGroupingLinesResPo,
					HttpStatus.INTERNAL_SERVER_ERROR);
		}

		return new ResponseEntity<SaveObjectCodeGroupingLinesResPo>(saveObjectCodeGroupingLinesResPo, new HttpHeaders(),
				HttpStatus.OK);

	}
	
	
	@ApiOperation(value = "Gets all ObjectCodeGroupings")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/getobjectcodegroupings")
	@Cacheable(value = "objectgroupingcache")
	public ResponseEntity<List<XxrObjectCodeGrouping>> getObjectCodeGroupings() throws ConvertRiteException {
		log.info("Start of getObjectCodeGroupings Method in Controller :::");
		List<XxrObjectCodeGrouping> xxrObjectCodeGrouping=new ArrayList<>();
		try {
			xxrObjectCodeGrouping = ObjectGroupingService.getObjectCodeGroupings();

		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<List<XxrObjectCodeGrouping>>(xxrObjectCodeGrouping, new HttpHeaders(), HttpStatus.OK);
	}
	
	@ApiOperation(value = "Gets  ObjectCodeGroupingLines By Id")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/getobjectcodegroupinglines")
	@Cacheable(value = "objectgroupinglinescache")
	public ResponseEntity<List<XxrObjectCodeGroupingLinesResPo>> getObjectCodeGroupingLines(@RequestParam("groupId") Long groupId) throws ConvertRiteException {
		log.info("Start of getObjectCodeGroupingLines Method in Controller :::");
		List<XxrObjectCodeGroupingLinesResPo> xxrObjectCodeGroupingLines=new ArrayList<>();
		try {
			xxrObjectCodeGroupingLines = ObjectGroupingService.getObjectCodeGroupingLines(groupId);

		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<List<XxrObjectCodeGroupingLinesResPo>>(xxrObjectCodeGroupingLines, new HttpHeaders(), HttpStatus.OK);
	}
}
