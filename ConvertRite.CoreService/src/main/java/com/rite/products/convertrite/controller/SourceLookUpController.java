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
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.rite.products.convertrite.Validations.Validations;
import com.rite.products.convertrite.exception.BadRequestException;
import com.rite.products.convertrite.exception.ConvertRiteException;
import com.rite.products.convertrite.model.XxrSrcLookupSets;
import com.rite.products.convertrite.model.XxrSrcLookupValues;
import com.rite.products.convertrite.po.LoadCustomLookupReqPo;
import com.rite.products.convertrite.po.LoadLookupReqPo;
import com.rite.products.convertrite.po.LoadLookupResPo;
import com.rite.products.convertrite.po.LookUpSearchReqPo;
import com.rite.products.convertrite.po.LookUpsetNameResPo;
import com.rite.products.convertrite.service.SourceLookUpService;

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

@RestController
public class SourceLookUpController {

	private static final Logger log = LoggerFactory.getLogger(SourceLookUpController.class);

	@Autowired
	SourceLookUpService sourceLookUpService;

	@ApiOperation(value = "This api performs search based on operator selected")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@PostMapping("/searchsrclookups")
	public ResponseEntity<List<XxrSrcLookupSets>> searchSrcLookups(@RequestBody LookUpSearchReqPo lookUpSearchReqPo)
			throws ConvertRiteException {
		log.info("Start of searchSrcLookups SourceLookUpController#######");
		List<XxrSrcLookupSets> lookupSetsLi = new ArrayList<>();
		HttpHeaders header = new HttpHeaders();
		try {
			if (Validations.isNullOrEmpty(lookUpSearchReqPo.getOperator()) || lookUpSearchReqPo.getPageNo() == null
					|| lookUpSearchReqPo.getPageSize() == null)
				throw new BadRequestException("operator,pageNo and pageSize are mandatory fields");

			lookupSetsLi = sourceLookUpService.searchSrcLookups(lookUpSearchReqPo, header);
		} catch (BadRequestException e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(e.getMessage(), HttpStatus.BAD_REQUEST);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<List<XxrSrcLookupSets>>(lookupSetsLi, header, HttpStatus.OK);
	}

	@ApiOperation("This Api Loads Lookups from source System")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 400, message = "Bad Request"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@PostMapping("/loadlookupsfromsrc")
	public ResponseEntity<LoadLookupResPo> loadLookupsFromSrc(@RequestBody LoadLookupReqPo loadLookupReqPo) {
		log.info("Start of loadLookupsFromSrc SourceLookUpController########");
		LoadLookupResPo loadLookupResPo = new LoadLookupResPo();
		try {
			if (Validations.isNullOrEmpty(loadLookupReqPo.getDbLink()))
				throw new BadRequestException("dbLink is  mandatory field");
			loadLookupResPo = sourceLookUpService.loadLookupsFromSrc(loadLookupReqPo.getDbLink());
		} catch (BadRequestException e) {
			log.error(e.getMessage());
			loadLookupResPo.setError(e.getMessage());
			return new ResponseEntity<LoadLookupResPo>(loadLookupResPo, HttpStatus.BAD_REQUEST);
		} catch (Exception e) {
			log.error(e.getMessage());
			loadLookupResPo.setError(e.getMessage());
			return new ResponseEntity<LoadLookupResPo>(loadLookupResPo, HttpStatus.OK);
		}
		return new ResponseEntity<LoadLookupResPo>(loadLookupResPo, HttpStatus.OK);
	}
	
	@ApiOperation("This Api Loads Lookups from source System")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 400, message = "Bad Request"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@PostMapping("/loadcustomlookupsfromsrc")
	public ResponseEntity<LoadLookupResPo> loadCustomLookupsFromSrc(@RequestBody LoadCustomLookupReqPo loadCustomLookupReqPo) {
		log.info("Start of loadLookupsFromSrc SourceLookUpController########");
		LoadLookupResPo loadLookupResPo = new LoadLookupResPo();
		try {
			if (Validations.isNullOrEmpty(loadCustomLookupReqPo.getDbLink()) || Validations.isNullOrEmpty(loadCustomLookupReqPo.getCustomQuery()))
				throw new BadRequestException("dbLink and customquery are   mandatory field");
			loadLookupResPo = sourceLookUpService.loadCustomLookupsFromSrc(loadCustomLookupReqPo);
		} catch (BadRequestException e) {
			log.error(e.getMessage());
			loadLookupResPo.setError(e.getMessage());
			return new ResponseEntity<LoadLookupResPo>(loadLookupResPo, HttpStatus.BAD_REQUEST);
		} catch (Exception e) {
			log.error(e.getMessage());
			loadLookupResPo.setError(e.getMessage());
			return new ResponseEntity<LoadLookupResPo>(loadLookupResPo, HttpStatus.OK);
		}
		return new ResponseEntity<LoadLookupResPo>(loadLookupResPo, HttpStatus.OK);
	}

	@ApiOperation("This Api gets Source Lookup Values")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 400, message = "Bad Request"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/getsrclookupvalues")
	public ResponseEntity<List<XxrSrcLookupValues>> getSrcLookupValues(@RequestParam("lookupSetId") Long lookupSetId)
			throws Exception {
		log.info("Start of getSrcLookupValues SourceLookUpController######");
		List<XxrSrcLookupValues> srcLookupValuesLi=new ArrayList<>();
		try {
			srcLookupValuesLi=sourceLookUpService.getSrcLookupValues(lookupSetId);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new Exception(e.getMessage());
		}
		return new ResponseEntity<List<XxrSrcLookupValues>>(srcLookupValuesLi,HttpStatus.OK);
	}
	
	@ApiOperation(value = "get all lookupsetnames based on search value")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/getsrclookupsetnames")
	public ResponseEntity<List<LookUpsetNameResPo>> getSrcLookupSetName(@RequestParam("lookupSetName") String lookupSetName) throws ConvertRiteException {
		log.info("Start of getSrcLookupSetName Method in SourceLookUpController :::");
		List<LookUpsetNameResPo> lookUpSetList = new ArrayList<>();
		try {
			lookUpSetList = sourceLookUpService.getSrcLookupSetName(lookupSetName);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<List<LookUpsetNameResPo>>(lookUpSetList, new HttpHeaders(), HttpStatus.OK);
	}

}
