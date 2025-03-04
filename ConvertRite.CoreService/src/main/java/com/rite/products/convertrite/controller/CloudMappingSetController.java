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
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.rite.products.convertrite.Validations.Validations;
import com.rite.products.convertrite.exception.BadRequestException;
import com.rite.products.convertrite.exception.ConvertRiteException;
import com.rite.products.convertrite.exception.ValidationException;
import com.rite.products.convertrite.model.XxrMappingSetsView;
import com.rite.products.convertrite.model.XxrMappingValue;
import com.rite.products.convertrite.model.XxrMappingValues;
import com.rite.products.convertrite.po.CloudColumnsPo;
import com.rite.products.convertrite.po.MappingSetsResPo;
import com.rite.products.convertrite.po.SaveCloudMappingSetColumnsPo;
import com.rite.products.convertrite.po.SaveCloudMappingSetColumnsResPo;
import com.rite.products.convertrite.po.SaveCloudMappingSetHdrPo;
import com.rite.products.convertrite.po.SaveCloudMappingSetHeaderJpaRes;
import com.rite.products.convertrite.po.SaveCloudMappingSetHeaderResPo;
import com.rite.products.convertrite.po.SaveCloudMappingSetHeadersPo;
import com.rite.products.convertrite.po.SaveCloudMappingSetValuesResPo;
import com.rite.products.convertrite.po.SourceColumnsPo;
import com.rite.products.convertrite.po.XxrMappingSetsResPo;
import com.rite.products.convertrite.service.CloudMappingSetService;

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

@RestController
@RequestMapping("api/cloud/mappingset")
public class CloudMappingSetController {

	private static final Logger log = LoggerFactory.getLogger(CloudMappingSetController.class);

	@Autowired
	CloudMappingSetService cloudMappingSetService;

	@ApiOperation(value = "Gets all the SourceObjects based on the params supplies")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/getsourceobjects")
	public ResponseEntity<String[]> getSourceObjects(@RequestParam("podId") Long podId,
			@RequestParam("projectId") Long projectId, @RequestParam("parentObjectId") Long parentObjectId,
			@RequestParam("objectId") Long objectId) throws ConvertRiteException {
		log.info("Start of getSourceObjects Method in CloudMappingSetController :::");
		String[] viewNames = {};
		try {
			if (podId != null && projectId != null && parentObjectId != null && objectId != null)
				viewNames = cloudMappingSetService.getSourceObjects(podId, projectId, parentObjectId, objectId);
			/*
			 * else throw new ConvertRiteException(
			 * "Please contact System Administrator there is an error while processing the request"
			 * , HttpStatus.BAD_REQUEST);
			 */
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<String[]>(viewNames, new HttpHeaders(), HttpStatus.OK);
	}

	@ApiOperation(value = "Gets all the SourceObjects without any filteration")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/getallsourceobjects")
	public ResponseEntity<String[]> getAllSourceObjects() throws ConvertRiteException {
		log.info("Start of getAllSourceObjects Method in CloudMappingSetController :::");
		String[] viewNames = {};
		try {
			viewNames = cloudMappingSetService.getAllSourceObjects();

		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<String[]>(viewNames, new HttpHeaders(), HttpStatus.OK);
	}

	@ApiOperation(value = "Gets all the SourceColumns based on the viewname")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/getsourcecolumns")
	public ResponseEntity<List<SourceColumnsPo>> getSourceColumns(@RequestParam("viewName") String viewName)
			throws ConvertRiteException {
		log.info("Start of getSourceColumns Method in CloudMappingSetController :::");
		List<SourceColumnsPo> sourceTemplateColumns = new ArrayList<>();
		try {
			if (!Validations.isNullOrEmpty(viewName))
				sourceTemplateColumns = cloudMappingSetService.getSourceColumns(viewName);
			/*
			 * else throw new ConvertRiteException("Missing viewName in Request",
			 * HttpStatus.BAD_REQUEST);
			 */
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<List<SourceColumnsPo>>(sourceTemplateColumns, new HttpHeaders(), HttpStatus.OK);
	}

	@ApiOperation(value = "Gets all the SourceFields based on the columnname and viewname")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/getsourcefields")
	public ResponseEntity<List<Object>> getSourceFields(@RequestParam("columnName") String columnName,
			@RequestParam("viewName") String viewName) throws ConvertRiteException {
		log.info("Start of getSourceFields Method in CloudMappingSetController :::");
		List<Object> sourceFields = new ArrayList<>();
		try {
			if (!Validations.isNullOrEmpty(viewName) && !Validations.isNullOrEmpty(columnName))
				sourceFields = cloudMappingSetService.getSourceFields(viewName, columnName);
			/*
			 * else throw new
			 * ConvertRiteException("Missing viewName or columnName in Request",
			 * HttpStatus.BAD_REQUEST);
			 */
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<List<Object>>(sourceFields, new HttpHeaders(), HttpStatus.OK);
	}

	@ApiOperation(value = "Gets all the CloudColumns based on ObjectId")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/getcloudcolumns")
	public ResponseEntity<List<CloudColumnsPo>> getCloudColumns(@RequestParam("objectId") Long objectId)
			throws ConvertRiteException {
		log.info("Start of getCloudColumns Method in CloudMappingSetController :::");
		List<CloudColumnsPo> cloudColumnsList = new ArrayList<>();
		try {
			if (objectId != null)
				cloudColumnsList = cloudMappingSetService.getCloudColumns(objectId);
			/*
			 * else throw new ConvertRiteException("Missing objectCode in Request",
			 * HttpStatus.BAD_REQUEST);
			 */
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<List<CloudColumnsPo>>(cloudColumnsList, new HttpHeaders(), HttpStatus.OK);
	}

	@ApiOperation(value = "Gets all the CloudValues based on ObjectId and CloudColumn")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/getcloudvalues")
	public ResponseEntity<List<String>> getCloudValues(@RequestParam("objectId") Long objectId,
			@RequestParam("cloudColumn") String cloudColumn) throws ConvertRiteException {
		log.info("Start of getCloudValues Method in CloudMappingSetController :::");
		// String[] cloudValues = {};
		List<String> cloudValues = null;
		try {
			if (objectId != null && !Validations.isNullOrEmpty(cloudColumn))
				cloudValues = cloudMappingSetService.getCloudValues(objectId, cloudColumn);
			/*
			 * else throw new
			 * ConvertRiteException("Missing objectId or cloudColumn in Request",
			 * HttpStatus.BAD_REQUEST);
			 */
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		// return new ResponseEntity<String[]>(cloudValues, new HttpHeaders(),
		// HttpStatus.OK);
		return new ResponseEntity<List<String>>(cloudValues, new HttpHeaders(), HttpStatus.OK);
	}

	@ApiOperation(value = "Gets all the MappingSets  based on podId,projectId and objectId")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/getcloudmappingsets")
	public ResponseEntity<List<XxrMappingSetsResPo>> getCloudMappingSets(@RequestParam("podId") Long podId,
			@RequestParam("projectId") Long projectId, @RequestParam("objectId") Long objectId)
			throws ConvertRiteException {
		log.info("Start of getCloudMappingSets Method in CloudMappingSetController :::");
		List<XxrMappingSetsResPo> cloudMappingSetList = new ArrayList<>();
		try {
			if (podId != null && projectId != null && objectId != null)
				cloudMappingSetList = cloudMappingSetService.getCloudMappingSets(podId, projectId, objectId);
			/*
			 * else throw new
			 * ConvertRiteException("Missing podId or projectId or objectId in Request",
			 * HttpStatus.BAD_REQUEST);
			 */
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<List<XxrMappingSetsResPo>>(cloudMappingSetList, new HttpHeaders(), HttpStatus.OK);
	}

	
	
	
	@ApiOperation(value = "Gets all the MappingSets without any filteration")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/getallcloudmappingsets")
	public ResponseEntity<List<XxrMappingSetsView>> getAllCloudMappingSets(@RequestHeader("roleid") Long roleId) throws ConvertRiteException {
		log.info("Start of getAllCloudMappingSets Method in CloudMappingSetController :::");
		List<XxrMappingSetsView> cloudMappingSetList = new ArrayList<>();
		try {
			cloudMappingSetList = cloudMappingSetService.getAllCloudMappingSets(roleId);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<List<XxrMappingSetsView>>(cloudMappingSetList, new HttpHeaders(), HttpStatus.OK);
	}
	
	/*
	 * @ApiOperation(value = "Gets all the MappingSets without any filteration")
	 * 
	 * @ApiResponses(value = { @ApiResponse(code = 200, message =
	 * "Successful Response"),
	 * 
	 * @ApiResponse(code = 500, message = "Server Side Error") })
	 * 
	 * @GetMapping("/getallcloudmappingsets") public
	 * ResponseEntity<List<XxrMappingSetsResPo>> getAllCloudMappingSets() throws
	 * ConvertRiteException { log.
	 * info("Start of getAllCloudMappingSets Method in CloudMappingSetController :::"
	 * ); List<XxrMappingSetsResPo> cloudMappingSetList = new ArrayList<>(); try {
	 * cloudMappingSetList = cloudMappingSetService.getAllCloudMappingSets();
	 * 
	 * } catch (Exception e) { log.error(e.getMessage()); throw new
	 * ConvertRiteException(
	 * "Please contact System Administrator there is an error while processing the request"
	 * , HttpStatus.INTERNAL_SERVER_ERROR); } return new
	 * ResponseEntity<List<XxrMappingSetsResPo>>(cloudMappingSetList, new
	 * HttpHeaders(), HttpStatus.OK); }
	 */

	@ApiOperation(value = "Gets all the MappingSets without any filteration")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/getallmappingsets")
	public ResponseEntity<List<MappingSetsResPo>> getAllMappingSets() throws ConvertRiteException {
		log.info("Start of getAllMappingSets Method in CloudMappingSetController :::");
		List<MappingSetsResPo> cloudMappingSetList = new ArrayList<>();
		try {
			cloudMappingSetList = cloudMappingSetService.getAllMappingSets();

		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<List<MappingSetsResPo>>(cloudMappingSetList, new HttpHeaders(), HttpStatus.OK);
	}

	@ApiOperation(value = "Gets all the MappingSetValues  based on MappingSetId")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/getcloudmappingsetvalues")
	public ResponseEntity<List<XxrMappingValues>> getCloudMappingSetValues(
			@RequestParam("mappingSetId") Long mappingSetId) throws ConvertRiteException {
		log.info("Start of getCloudMappingSetValues Method in CloudMappingSetController :::");
		List<XxrMappingValues> cloudMappingSetValues = new ArrayList<>();
		try {
			if (mappingSetId != null)
				cloudMappingSetValues = cloudMappingSetService.getCloudMappingSetValues(mappingSetId);
			/*
			 * else throw new ConvertRiteException("Missing mappingSetId in Request",
			 * HttpStatus.BAD_REQUEST);
			 */
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<List<XxrMappingValues>>(cloudMappingSetValues, new HttpHeaders(), HttpStatus.OK);
	}

	@ApiOperation(value = "Gets all the MappingSetValues  based on MappingSetId")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/getcloudmappingsetvalue")
	public ResponseEntity<List<XxrMappingValue>> getCloudMappingSetValue(
			@RequestParam("mappingSetId") Long mappingSetId) throws ConvertRiteException {
		log.info("Start of getCloudMappingSetValue Method in CloudMappingSetController :::");
		List<XxrMappingValue> cloudMappingSetValues = new ArrayList<>();
		try {
			if (mappingSetId != null)
				cloudMappingSetValues = cloudMappingSetService.getCloudMappingSetValue(mappingSetId);
			/*
			 * else throw new ConvertRiteException("Missing mappingSetId in Request",
			 * HttpStatus.BAD_REQUEST);
			 */
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<List<XxrMappingValue>>(cloudMappingSetValues, new HttpHeaders(), HttpStatus.OK);
	}

	@ApiOperation(value = "This Api is to save CloudMappingSetHeaders")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@PostMapping("/savecloudmappingsethdr")
	public ResponseEntity<SaveCloudMappingSetHeaderResPo> saveCloudMappingSetHdr(
			@RequestBody SaveCloudMappingSetHdrPo mappingHeader, HttpServletRequest request) {
		log.info("Start of saveCloudMappingSetHeaders Method in Controller ###");
		SaveCloudMappingSetHeaderResPo saveCloudMappingSetHeaderResponsePo = new SaveCloudMappingSetHeaderResPo();

		try {

			if (Validations.isNullOrEmpty(mappingHeader.getMapSetName())
					|| Validations.isNullOrEmpty(mappingHeader.getMapSetType())
					|| mappingHeader.getSourceLookupSetId1() == null || mappingHeader.getCloudLookupSetId() == null
					|| mappingHeader.getPodId() == null || mappingHeader.getProjectId() == null
					|| mappingHeader.getObjectId() == null || mappingHeader.getParentObjectId() == null)
				throw new BadRequestException(
						"mapSetName,mapSetType,sourceLookupSetId1,podId,projectId,parentObjectId,objectId and cloudLookupsetId  are Mandatory fields");
			saveCloudMappingSetHeaderResponsePo = cloudMappingSetService.saveCloudMappingSetHdr(mappingHeader, request);
		} catch (BadRequestException e) {
			log.error(e.getMessage());
			saveCloudMappingSetHeaderResponsePo.setMessage(e.getMessage());
			saveCloudMappingSetHeaderResponsePo.setError("Error while saving mappingset header");
			return new ResponseEntity<SaveCloudMappingSetHeaderResPo>(saveCloudMappingSetHeaderResponsePo,
					HttpStatus.BAD_REQUEST);
		} catch (ValidationException e) {
			log.error(e.getMessage());
			saveCloudMappingSetHeaderResponsePo.setMessage(e.getMessage());
			saveCloudMappingSetHeaderResponsePo.setError("Error while saving mappingset header");
			return new ResponseEntity<SaveCloudMappingSetHeaderResPo>(saveCloudMappingSetHeaderResponsePo,
					HttpStatus.OK);
		} catch (Exception e) {
			log.error(e.getMessage());
			saveCloudMappingSetHeaderResponsePo.setMessage("Please contact System Administrator there is an error while processing the request");
			saveCloudMappingSetHeaderResponsePo.setError(e.getMessage());
			return new ResponseEntity<SaveCloudMappingSetHeaderResPo>(saveCloudMappingSetHeaderResponsePo,
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<SaveCloudMappingSetHeaderResPo>(saveCloudMappingSetHeaderResponsePo,
				new HttpHeaders(), HttpStatus.OK);
	}

	@ApiOperation(value = "This Api is to save CloudMappingSetHeaders")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@PostMapping("/savecloudmappingsetheaders")
	public ResponseEntity<SaveCloudMappingSetHeaderResPo> saveCloudMappingSetHeaders(
			@RequestBody List<SaveCloudMappingSetHeadersPo> saveCloudMappingSetHeadersPo, HttpServletRequest request) {
		log.info("Start of saveCloudMappingSetHeaders Method in Controller ###");
		SaveCloudMappingSetHeaderResPo saveCloudMappingSetHeaderResponsePo = new SaveCloudMappingSetHeaderResPo();

		try {
			SaveCloudMappingSetHeadersPo mappingHeader = saveCloudMappingSetHeadersPo.get(0);
			if (Validations.isNullOrEmpty(mappingHeader.getMapSetName())
					|| Validations.isNullOrEmpty(mappingHeader.getMapSetType())
					|| Validations.isNullOrEmpty(mappingHeader.getSourceObject1())
					|| Validations.isNullOrEmpty(mappingHeader.getCloudColumn())
					|| Validations.isNullOrEmpty(mappingHeader.getSourceColumn1())
					|| Validations.isNullOrEmpty(mappingHeader.getLookupSetName()) || mappingHeader.getPodId() == null
					|| mappingHeader.getProjectId() == null || mappingHeader.getObjectId() == null
					|| mappingHeader.getParentObjectId() == null)
				throw new BadRequestException(
						"mapSetName,mapSetType,sourceObject1,sourceColumn1,podId,projectId,parentObjectId,objectId,lookupSetName and cloudColumn are Mandatory fields");

			saveCloudMappingSetHeaderResponsePo = cloudMappingSetService
					.saveCloudMappingSetHeaders(saveCloudMappingSetHeadersPo, request);
		} catch (BadRequestException e) {
			log.error(e.getMessage());
			saveCloudMappingSetHeaderResponsePo.setMessage(e.getMessage());
			saveCloudMappingSetHeaderResponsePo.setError("Error while saving mappingset header");
			return new ResponseEntity<SaveCloudMappingSetHeaderResPo>(saveCloudMappingSetHeaderResponsePo,
					HttpStatus.BAD_REQUEST);
		} catch (ValidationException e) {
			log.error(e.getMessage());
			saveCloudMappingSetHeaderResponsePo.setMessage(e.getMessage());
			saveCloudMappingSetHeaderResponsePo.setError("Error while saving mappingset header");
			return new ResponseEntity<SaveCloudMappingSetHeaderResPo>(saveCloudMappingSetHeaderResponsePo,
					HttpStatus.OK);
		} catch (Exception e) {
			log.error(e.getMessage());
			saveCloudMappingSetHeaderResponsePo.setMessage("Please contact System Administrator there is an error while processing the request");
			saveCloudMappingSetHeaderResponsePo.setError(e.getMessage());

			return new ResponseEntity<SaveCloudMappingSetHeaderResPo>(saveCloudMappingSetHeaderResponsePo,
					HttpStatus.INTERNAL_SERVER_ERROR);
		}

		return new ResponseEntity<SaveCloudMappingSetHeaderResPo>(saveCloudMappingSetHeaderResponsePo,
				new HttpHeaders(), HttpStatus.OK);

	}

	@ApiOperation(value = "This Api is for copy CloudMappingSet")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error"),
			@ApiResponse(code = 400, message = "Bad Request") })
	@PostMapping("/copymappingset")
	public ResponseEntity<SaveCloudMappingSetHeaderResPo> copyMappingSet(
			@RequestParam("newMapSetName") String newMapSetName, @RequestParam("mapSetId") Long mapSetId,
			@RequestParam("podId") Long podId, HttpServletRequest request) {
		log.info("Start of copyCloudMappingSet Method in Controller ###");
		SaveCloudMappingSetHeaderResPo saveCloudMappingSetHeaderResponsePo = new SaveCloudMappingSetHeaderResPo();
		try {
			saveCloudMappingSetHeaderResponsePo = cloudMappingSetService.copyCloudMappingSet(newMapSetName, mapSetId,
					podId, request);
		} catch (Exception e) {
			saveCloudMappingSetHeaderResponsePo.setMessage("Please contact System Administrator there is an error while processing the request");
			saveCloudMappingSetHeaderResponsePo.setError(e.getMessage());
			log.error(e.getMessage());
			return new ResponseEntity<SaveCloudMappingSetHeaderResPo>(saveCloudMappingSetHeaderResponsePo,
					new HttpHeaders(), HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<SaveCloudMappingSetHeaderResPo>(saveCloudMappingSetHeaderResponsePo,
				new HttpHeaders(), HttpStatus.OK);
	}
	
	@ApiOperation(value = "This Api is for copying CloudMappingSet")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error"),
			@ApiResponse(code = 400, message = "Bad Request") })
	@PostMapping("/copycloudmappingset")
	public ResponseEntity<SaveCloudMappingSetHeaderResPo> copyCloudMappingSet(
			@RequestParam("newMapSetName") String newMapSetName, @RequestParam("mapSetId") Long mapSetId,
			@RequestParam("podId") Long podId, HttpServletRequest request) {
		log.info("Start of copyCloudMappingSet Method in Controller ###");
		SaveCloudMappingSetHeaderResPo saveCloudMappingSetHeaderResponsePo = new SaveCloudMappingSetHeaderResPo();
		try {
			saveCloudMappingSetHeaderResponsePo = cloudMappingSetService.copyMappingSet(newMapSetName, mapSetId,
					podId, request);
		} catch (Exception e) {
			saveCloudMappingSetHeaderResponsePo.setMessage("Please contact System Administrator there is an error while processing the request");
			saveCloudMappingSetHeaderResponsePo.setError(e.getMessage());
			log.error(e.getMessage());
			return new ResponseEntity<SaveCloudMappingSetHeaderResPo>(saveCloudMappingSetHeaderResponsePo,
					new HttpHeaders(), HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<SaveCloudMappingSetHeaderResPo>(saveCloudMappingSetHeaderResponsePo,
				new HttpHeaders(), HttpStatus.OK);
	}
	
	

	@ApiOperation(value = "This Api is to save CloudMappingSetColumns")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@PostMapping("/savecloudmappingsetcolumns")
	public ResponseEntity<SaveCloudMappingSetColumnsResPo> saveCloudMappingSetColumns(
			@RequestBody List<SaveCloudMappingSetColumnsPo> cloudMappingSetColumnsPo, HttpServletRequest request) {
		log.info("Start of saveCloudMappingSetColumns Method in Controller ###");
		SaveCloudMappingSetColumnsResPo cloudMappingSetColumnsResPo = new SaveCloudMappingSetColumnsResPo();
		try {

			cloudMappingSetColumnsResPo = cloudMappingSetService.saveCloudMappingSetColumns(cloudMappingSetColumnsPo,
					request);

		} catch (BadRequestException e) {
			log.error(e.getMessage());
			cloudMappingSetColumnsResPo.setMessage(e.getMessage());
			cloudMappingSetColumnsResPo.setError("Error while saving MappingSet cloumns");
			return new ResponseEntity<SaveCloudMappingSetColumnsResPo>(cloudMappingSetColumnsResPo,
					HttpStatus.BAD_REQUEST);
		} catch (Exception e) {
			log.error(e.getMessage());
			cloudMappingSetColumnsResPo.setMessage("Please contact System Administrator there is an error while processing the request");
			cloudMappingSetColumnsResPo.setError(e.getMessage());
			return new ResponseEntity<SaveCloudMappingSetColumnsResPo>(cloudMappingSetColumnsResPo,
					HttpStatus.INTERNAL_SERVER_ERROR);
		}

		return new ResponseEntity<SaveCloudMappingSetColumnsResPo>(cloudMappingSetColumnsResPo, new HttpHeaders(),
				HttpStatus.OK);
	}
	

	@ApiOperation(value = "This Api is to save CloudMappingSetColumns")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@PostMapping("/savecloudmappingsetvalues")
	public ResponseEntity<SaveCloudMappingSetValuesResPo> saveCloudMappingSetValues(
			@RequestBody List<SaveCloudMappingSetColumnsPo> cloudMappingSetColumnsPo, HttpServletRequest request) {
		log.info("Start of saveCloudMappingSetValues Method in Controller ###");
		SaveCloudMappingSetValuesResPo cloudMappingSetColumnsResPo = new SaveCloudMappingSetValuesResPo();
		try {
			cloudMappingSetColumnsResPo = cloudMappingSetService.saveCloudMappingSetValues(cloudMappingSetColumnsPo,
					request);

		} catch (BadRequestException e) {
			log.error(e.getMessage());
			cloudMappingSetColumnsResPo.setError("Error while saving MappingSet cloumns");
			cloudMappingSetColumnsResPo.setMessage(e.getMessage());
			return new ResponseEntity<SaveCloudMappingSetValuesResPo>(cloudMappingSetColumnsResPo,
					HttpStatus.BAD_REQUEST);
		} catch (ValidationException e) {
			log.error(e.getMessage());
			cloudMappingSetColumnsResPo.setError("Error while saving MappingSet cloumns");
			cloudMappingSetColumnsResPo.setMessage(e.getMessage());
			return new ResponseEntity<SaveCloudMappingSetValuesResPo>(cloudMappingSetColumnsResPo, HttpStatus.OK);
		} catch (Exception e) {
			log.error(e.getMessage());
			cloudMappingSetColumnsResPo.setMessage("Please contact System Administrator there is an error while processing the request");
			cloudMappingSetColumnsResPo.setError(e.getMessage());
			return new ResponseEntity<SaveCloudMappingSetValuesResPo>(cloudMappingSetColumnsResPo,
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<SaveCloudMappingSetValuesResPo>(cloudMappingSetColumnsResPo, new HttpHeaders(),
				HttpStatus.OK);
	}

	@ApiOperation(value = "This Api is to save cloud mapping set hdrs with jpa")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@PostMapping("/savecloudmappingsetshdrswithjpa")
	public ResponseEntity<SaveCloudMappingSetHeaderJpaRes> saveCloudMappingSetHdrWithJpa(
			@RequestBody List<SaveCloudMappingSetHeadersPo> saveCloudMappingSetHeadersPos) {
		log.info("Start of saveCloudMappingSetHdrWithJpa Method in Controller ###");
		SaveCloudMappingSetHeaderJpaRes saveCloudMappingSetHeaderJpaRes = new SaveCloudMappingSetHeaderJpaRes();

		try {
			saveCloudMappingSetHeaderJpaRes = cloudMappingSetService.saveCloudMappingSetHdrWithJpa(saveCloudMappingSetHeadersPos);
		} catch (BadRequestException e) {
			log.error(e.getMessage());
			saveCloudMappingSetHeaderJpaRes.setMessage(e.getMessage());
			saveCloudMappingSetHeaderJpaRes.setError("Please contact System Administrator there is an error while processing the request");
			return new ResponseEntity<SaveCloudMappingSetHeaderJpaRes>(saveCloudMappingSetHeaderJpaRes,
					HttpStatus.BAD_REQUEST);
		} catch (Exception e) {
			log.error(e.getMessage());
			saveCloudMappingSetHeaderJpaRes.setMessage("Please contact System Administrator there is an error while processing the request");
			saveCloudMappingSetHeaderJpaRes.setError(e.getMessage());
			return new ResponseEntity<SaveCloudMappingSetHeaderJpaRes>(saveCloudMappingSetHeaderJpaRes,
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<SaveCloudMappingSetHeaderJpaRes>(saveCloudMappingSetHeaderJpaRes,HttpStatus.OK);
	}
	
	@ApiOperation(value = "This Api is to save cloud mapping set values with jpa")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@PostMapping("/savecloudmappingsetvalueswithjpa")
	public ResponseEntity<SaveCloudMappingSetColumnsResPo> saveCloudMappingSetValuesWithJpa(
			@RequestBody List<SaveCloudMappingSetColumnsPo> saveCloudMappingSetColumnsPos) {
		log.info("Start of saveCloudMappingSetValuesWithJpa Method in Controller ###");
		SaveCloudMappingSetColumnsResPo saveCloudMappingSetColumnsResPo = new SaveCloudMappingSetColumnsResPo();

		try {
			saveCloudMappingSetColumnsResPo = cloudMappingSetService.saveCloudMappingSetValuesWithJpa(saveCloudMappingSetColumnsPos);
		} catch (BadRequestException e) {
			log.error(e.getMessage());
			saveCloudMappingSetColumnsResPo.setMessage(e.getMessage());
			saveCloudMappingSetColumnsResPo.setError("Please contact System Administrator there is an error while processing the request");
			return new ResponseEntity<SaveCloudMappingSetColumnsResPo>(saveCloudMappingSetColumnsResPo,
					HttpStatus.BAD_REQUEST);
		} catch (Exception e) {
			log.error(e.getMessage());
			saveCloudMappingSetColumnsResPo.setMessage("Please contact System Administrator there is an error while processing the request");
			saveCloudMappingSetColumnsResPo.setError(e.getMessage());
			return new ResponseEntity<SaveCloudMappingSetColumnsResPo>(saveCloudMappingSetColumnsResPo,
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<SaveCloudMappingSetColumnsResPo>(saveCloudMappingSetColumnsResPo,HttpStatus.OK);
	}

}
