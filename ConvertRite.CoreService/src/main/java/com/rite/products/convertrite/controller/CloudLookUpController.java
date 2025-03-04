package com.rite.products.convertrite.controller;

import java.util.ArrayList;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.rite.products.convertrite.Validations.Validations;
import com.rite.products.convertrite.exception.BadRequestException;
import com.rite.products.convertrite.exception.ConvertRiteException;
import com.rite.products.convertrite.exception.ValidationException;
import com.rite.products.convertrite.model.XxrLookUpValues;
import com.rite.products.convertrite.model.XxrLookupSets;
import com.rite.products.convertrite.po.LookUpSearchReqPo;
import com.rite.products.convertrite.po.LookUpsetNameResPo;
import com.rite.products.convertrite.po.SaveCloudLookUpSetHeadersResPo;
import com.rite.products.convertrite.po.SaveLookUpSetPo;
import com.rite.products.convertrite.po.SaveLookUpValuesPo;
import com.rite.products.convertrite.po.SaveLookUpValuesResPo;
import com.rite.products.convertrite.service.CloudLookUpService;

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

@RestController
@RequestMapping("api/cloud/LookUp")
public class CloudLookUpController {
	private static final Logger log = LoggerFactory.getLogger(CloudLookUpController.class);

	@Autowired
	CloudLookUpService cloudLookUpService;

	/*
	 * @ApiOperation(value = "Gets all cloud lookup headers")
	 * 
	 * @ApiResponses(value = { @ApiResponse(code = 200, message =
	 * "Successful Response"),
	 * 
	 * @ApiResponse(code = 500, message = "Server Side Error") })
	 * 
	 * @GetMapping("/getlookupsetheaders") public
	 * ResponseEntity<List<XxrCloudMasterLookUpSet>> getLookupSetHeaders() throws
	 * ConvertRiteException {
	 * log.info("Start of getLookupSetHeaders Method in CloudLookUpController :::");
	 * List<XxrCloudMasterLookUpSet> cloudLookUpSetList = new ArrayList<>(); try {
	 * cloudLookUpSetList = cloudLookUpService.getLookupSetHeaders();
	 * 
	 * } catch (Exception e) { log.error(e.getMessage()); throw new
	 * ConvertRiteException(
	 * "Please contact System Administrator there is an error while processing the request"
	 * , HttpStatus.INTERNAL_SERVER_ERROR); } return new
	 * ResponseEntity<List<XxrCloudMasterLookUpSet>>(cloudLookUpSetList, new
	 * HttpHeaders(), HttpStatus.OK); }
	 */

	@ApiOperation(value = "Gets all lookup headers")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/getlookupset")
	public ResponseEntity<List<XxrLookupSets>> getLookupSet() throws ConvertRiteException {
		log.info("Start of getLookupSet Method in CloudLookUpController :::");
		List<XxrLookupSets> lookUpSetList = new ArrayList<>();
		try {
			lookUpSetList = cloudLookUpService.getLookupSet();

		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<List<XxrLookupSets>>(lookUpSetList, new HttpHeaders(), HttpStatus.OK);
	}

	@ApiOperation(value = "This api performs search based on operator selected")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@PostMapping("/searchbyoperator")
	public ResponseEntity<List<XxrLookupSets>> searchByOperator(@RequestBody LookUpSearchReqPo lookUpSearchReqPo)
			throws ConvertRiteException {
		log.info("Start of searchByOperator Method in CloudLookUpController :::");
		List<XxrLookupSets> lookUpSetList = new ArrayList<>();
		HttpHeaders header = new HttpHeaders();
		try {
			if (Validations.isNullOrEmpty(lookUpSearchReqPo.getOperator()) || lookUpSearchReqPo.getPageNo() == null
					|| lookUpSearchReqPo.getPageSize() == null)
				throw new BadRequestException("operator,pageNo and pageSize are mandatory fields");
			lookUpSetList = cloudLookUpService.searchByOperator(lookUpSearchReqPo, header);

		} catch (BadRequestException e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(e.getMessage(), HttpStatus.BAD_REQUEST);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<List<XxrLookupSets>>(lookUpSetList, header, HttpStatus.OK);
	}

	@ApiOperation(value = "Gets all lookup Values")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/getlookupvalues")
	public ResponseEntity<List<XxrLookUpValues>> getlookupvalues(@RequestParam("lookUpSetId") Long lookUpSetId)
			throws ConvertRiteException {
		log.info("Start of getLookupSetValues Method in CloudLookUpController :::");
		List<XxrLookUpValues> lookUpValuesList = new ArrayList<>();
		try {
			lookUpValuesList = cloudLookUpService.getlookupvalues(lookUpSetId);

		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<List<XxrLookUpValues>>(lookUpValuesList, new HttpHeaders(), HttpStatus.OK);
	}

	@ApiOperation(value = "Gets  lookup Values by value search")

	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),

			@ApiResponse(code = 500, message = "Server Side Error") })

	@GetMapping("/getlookupvaluesbyval")
	public ResponseEntity<List<XxrLookUpValues>> getlookupvaluesByVal(@RequestParam("lookUpSetId") Long lookUpSetId,
			@RequestParam("lookUpValue") String lookUpValue) throws ConvertRiteException {
		log.info("Start of getLookupSetValues Method in CloudLookUpController :::");
		List<XxrLookUpValues> lookUpValuesList = new ArrayList<>();
		try {
			lookUpValuesList = cloudLookUpService.getlookupvaluesByVal(lookUpSetId,lookUpValue);

		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<List<XxrLookUpValues>>(lookUpValuesList, new HttpHeaders(), HttpStatus.OK);
	}

	@ApiOperation(value = "get all lookupsetnames based on search value")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/getlookupsetname")
	public ResponseEntity<List<LookUpsetNameResPo>> getLookupSetName(
			@RequestParam("lookupSetName") String lookupSetName) throws ConvertRiteException {
		log.info("Start of getLookupSetName Method in CloudLookUpController :::");
		List<LookUpsetNameResPo> lookUpSetList = new ArrayList<>();
		try {
			lookUpSetList = cloudLookUpService.getLookupSetName(lookupSetName);

		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<List<LookUpsetNameResPo>>(lookUpSetList, new HttpHeaders(), HttpStatus.OK);
	}

	@ApiOperation(value = "Get column names")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/getcolumnnames")
	public ResponseEntity<List<String>> getColumnNames() throws ConvertRiteException {
		log.info("Start of getColumnNames Method in CloudLookUpController :::");
		List<String> columnNames = new ArrayList<>();
		try {
			columnNames = cloudLookUpService.getColumnNames();

		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<List<String>>(columnNames, new HttpHeaders(), HttpStatus.OK);
	}

	/*
	 * @ApiOperation(value = "Gets all cloud lookup Values")
	 * 
	 * @ApiResponses(value = { @ApiResponse(code = 200, message =
	 * "Successful Response"),
	 * 
	 * @ApiResponse(code = 500, message = "Server Side Error") })
	 * 
	 * @GetMapping("/getlookupsetvalues") public
	 * ResponseEntity<List<XxrCloudMasterLookupValues>> getLookupSetValues(
	 * 
	 * @RequestParam("lookUpSetId") Long lookUpSetId) throws ConvertRiteException {
	 * log.info("Start of getLookupSetValues Method in CloudLookUpController :::");
	 * List<XxrCloudMasterLookupValues> cloudLookUpValuesList = new ArrayList<>();
	 * try { cloudLookUpValuesList =
	 * cloudLookUpService.getLookupSetValues(lookUpSetId);
	 * 
	 * } catch (Exception e) { log.error(e.getMessage()); throw new
	 * ConvertRiteException(
	 * "Please contact System Administrator there is an error while processing the request"
	 * , HttpStatus.INTERNAL_SERVER_ERROR); } return new
	 * ResponseEntity<List<XxrCloudMasterLookupValues>>(cloudLookUpValuesList, new
	 * HttpHeaders(), HttpStatus.OK); }
	 */

	/*
	 * @ApiOperation(value = "This Api is to save the cloud lookup set headers")
	 * 
	 * @ApiResponses(value = { @ApiResponse(code = 200, message =
	 * "Successful Response"),
	 * 
	 * @ApiResponse(code = 500, message = "Server Side Error"),
	 * 
	 * @ApiResponse(code = 400, message = "Bad Request") })
	 * 
	 * @PostMapping("/savecloudlookupsetheaders") public
	 * ResponseEntity<SaveCloudLookUpSetHeadersResPo> saveCloudLookUpSetHeaders(
	 * 
	 * @RequestBody List<SaveCloudLookUpSetHeadersPo> cloudLookUpSetHeadersPo) {
	 * log.info("Start of saveCloudLookUpSetHeaders Method in Controller ###");
	 * 
	 * SaveCloudLookUpSetHeadersResPo saveCloudLookUpSetHeadersResPo = new
	 * SaveCloudLookUpSetHeadersResPo(); try {
	 * 
	 * saveCloudLookUpSetHeadersResPo =
	 * cloudLookUpService.saveCloudLookUpSetHeaders(cloudLookUpSetHeadersPo);
	 * 
	 * } catch (Exception e) { log.error(e.getMessage());
	 * saveCloudLookUpSetHeadersResPo.
	 * setMessage("Error while saving cloud LookupSet Headers");
	 * saveCloudLookUpSetHeadersResPo.setError(e.getMessage()); new
	 * ResponseEntity<SaveCloudLookUpSetHeadersResPo>(
	 * saveCloudLookUpSetHeadersResPo, HttpStatus.INTERNAL_SERVER_ERROR); }
	 * 
	 * return new ResponseEntity<SaveCloudLookUpSetHeadersResPo>(
	 * saveCloudLookUpSetHeadersResPo, new HttpHeaders(), HttpStatus.OK);
	 * 
	 * }
	 */

	@ApiOperation(value = "This Api is to save the lookup set headers")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error"),
			@ApiResponse(code = 400, message = "Bad Request") })
	@PostMapping("/savelookupsetheaders")
	public ResponseEntity<SaveCloudLookUpSetHeadersResPo> saveLookUpSetHeaders(
			@RequestBody List<SaveLookUpSetPo> lookUpSetHeadersPo, HttpServletRequest request) {
		log.info("Start of saveLookUpSetHeaders Method in Controller ###");

		SaveCloudLookUpSetHeadersResPo saveLookUpSetHeadersResPo = new SaveCloudLookUpSetHeadersResPo();
		try {
			SaveLookUpSetPo lookUpSetHeader = lookUpSetHeadersPo.get(0);
			if (Validations.isNullOrEmpty(lookUpSetHeader.getLookUpSetName()))
				throw new BadRequestException("LookupSetName is Mandatory field");
			saveLookUpSetHeadersResPo = cloudLookUpService.saveLookUpSetHeaders(lookUpSetHeadersPo, request);

		} catch (BadRequestException e) {
			log.error(e.getMessage());
			saveLookUpSetHeadersResPo.setMessage("Error while saving  LookupSet Headers");
			saveLookUpSetHeadersResPo.setError(e.getMessage());
			return new ResponseEntity<SaveCloudLookUpSetHeadersResPo>(saveLookUpSetHeadersResPo,
					HttpStatus.BAD_REQUEST);
		} catch (ValidationException e) {
			log.error(e.getMessage());
			saveLookUpSetHeadersResPo.setMessage("Error while saving  LookupSet Headers");
			saveLookUpSetHeadersResPo.setError(e.getMessage());
			return new ResponseEntity<SaveCloudLookUpSetHeadersResPo>(saveLookUpSetHeadersResPo, HttpStatus.OK);
		} catch (Exception e) {
			log.error(e.getMessage());
			saveLookUpSetHeadersResPo.setMessage("Error while saving  LookupSet Headers");
			saveLookUpSetHeadersResPo.setError(e.getMessage());
			return new ResponseEntity<SaveCloudLookUpSetHeadersResPo>(saveLookUpSetHeadersResPo,
					HttpStatus.INTERNAL_SERVER_ERROR);
		}

		return new ResponseEntity<SaveCloudLookUpSetHeadersResPo>(saveLookUpSetHeadersResPo, new HttpHeaders(),
				HttpStatus.OK);

	}

	/*
	 * @ApiOperation(value = "This Api is to save the cloud lookup set values")
	 * 
	 * @ApiResponses(value = { @ApiResponse(code = 200, message =
	 * "Successful Response"),
	 * 
	 * @ApiResponse(code = 500, message = "Server Side Error"),
	 * 
	 * @ApiResponse(code = 400, message = "Bad Request") })
	 * 
	 * @PostMapping("/savecloudlookupsetvalues") public
	 * ResponseEntity<SaveCloudLookUpSetColumnsResPo> saveCloudLookUpSetValues(
	 * 
	 * @RequestBody List<SaveCloudLookUpSetColumnsPo> cloudLookUpSetColumnsPo) {
	 * log.info("Start of saveCloudLookUpSetValues Method in Controller ###");
	 * 
	 * SaveCloudLookUpSetColumnsResPo saveCloudLookUpSetColumnsResPo = new
	 * SaveCloudLookUpSetColumnsResPo(); try {
	 * 
	 * saveCloudLookUpSetColumnsResPo =
	 * cloudLookUpService.saveCloudLookUpSetValues(cloudLookUpSetColumnsPo);
	 * 
	 * } catch (Exception e) { log.error(e.getMessage());
	 * saveCloudLookUpSetColumnsResPo.
	 * setMessage("Error while saving cloud LookupSet Values");
	 * saveCloudLookUpSetColumnsResPo.setError(e.getMessage()); new
	 * ResponseEntity<SaveCloudLookUpSetColumnsResPo>(
	 * saveCloudLookUpSetColumnsResPo, HttpStatus.INTERNAL_SERVER_ERROR); }
	 * 
	 * return new ResponseEntity<SaveCloudLookUpSetColumnsResPo>(
	 * saveCloudLookUpSetColumnsResPo, new HttpHeaders(), HttpStatus.OK);
	 * 
	 * }
	 */

	@ApiOperation(value = "This Api is to save the lookup values")

	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),

			@ApiResponse(code = 500, message = "Server Side Error"),

			@ApiResponse(code = 400, message = "Bad Request") })

	@PostMapping("/savelookupvalues")
	public ResponseEntity<SaveLookUpValuesResPo> saveLookUpValues(@RequestBody List<SaveLookUpValuesPo> lookUpValuesPo,
			HttpServletRequest request) {
		log.info("Start of saveLookUpValues Method in Controller ###");

		SaveLookUpValuesResPo saveCloudLookUpSetColumnsResPo = new SaveLookUpValuesResPo();
		try {

			saveCloudLookUpSetColumnsResPo = cloudLookUpService.saveLookUpValues(lookUpValuesPo, request);

		} catch (BadRequestException e) {
			log.error(e.getMessage());
			saveCloudLookUpSetColumnsResPo.setMessage("Error while saving  LookupSet Values");
			saveCloudLookUpSetColumnsResPo.setError(e.getMessage());
			return new ResponseEntity<SaveLookUpValuesResPo>(saveCloudLookUpSetColumnsResPo, HttpStatus.BAD_REQUEST);
		} catch (ValidationException e) {
			log.error(e.getMessage());
			saveCloudLookUpSetColumnsResPo.setMessage("Error while saving  LookupSet Values");
			saveCloudLookUpSetColumnsResPo.setError(e.getMessage());
			return new ResponseEntity<SaveLookUpValuesResPo>(saveCloudLookUpSetColumnsResPo, HttpStatus.OK);
		} catch (Exception e) {
			log.error(e.getMessage());
			saveCloudLookUpSetColumnsResPo.setMessage("Error while saving  LookupSet Values");
			saveCloudLookUpSetColumnsResPo.setError(e.getMessage());
			return new ResponseEntity<SaveLookUpValuesResPo>(saveCloudLookUpSetColumnsResPo,
					HttpStatus.INTERNAL_SERVER_ERROR);
		}

		return new ResponseEntity<SaveLookUpValuesResPo>(saveCloudLookUpSetColumnsResPo, new HttpHeaders(),
				HttpStatus.OK);

	}

}
