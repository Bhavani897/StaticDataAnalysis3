package com.rite.products.convertrite.controller;

import com.rite.products.convertrite.exception.ConvertRiteException;
import com.rite.products.convertrite.po.*;
import com.rite.products.convertrite.service.CrCloudTemplateService;
import com.rite.products.convertrite.service.CrHookUsageService;

import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/convertritecore/cloud")
@Slf4j
public class CrCloudTemplateController {

	@Autowired
	CrCloudTemplateService crCloudTemplateService;

	@Autowired
	CrHookUsageService crHookUsageService;

	@ApiOperation(value = "API to get all Cloud Templates")
	@GetMapping("/getallcloudtemplates")
	public ResponseEntity<?> getAllCloudTemplates() {
		try {
			return new ResponseEntity<>(crCloudTemplateService.getAllCloudTemplates(), HttpStatus.OK);
		} catch (Exception e) {
			log.error(e.getMessage());
			return new ResponseEntity<>(e.getCause(), HttpStatus.EXPECTATION_FAILED);
		}
	}

	@ApiOperation(value = "API to get Cloud Template by ID")
	@GetMapping("/getcloudtemplatebyid/{templateId}")
	public ResponseEntity<?> getCloudTemplateById(@PathVariable Long templateId) {
		try {
			return new ResponseEntity<>(crCloudTemplateService.getCloudTemplateById(templateId), HttpStatus.OK);
		} catch (Exception e) {
			log.error(e.getMessage());
			return new ResponseEntity<>(e.getCause(), HttpStatus.EXPECTATION_FAILED);
		}
	}

	@ApiOperation(value = "This Api is to save new cloudtemplates")
	@PostMapping("/savecloudtemplateheaders")
	public ResponseEntity<?> saveCloudTemplateHeaders(
			@RequestBody CrCloudTemplateHeaderReqPo cloudTemplateHeaderCreateReqPo) {
		try {
			return new ResponseEntity<Object>(
					crCloudTemplateService.saveCloudTemplateHeaders(cloudTemplateHeaderCreateReqPo), HttpStatus.OK);
		} catch (Exception e) {
			log.error(e.getMessage());
			return new ResponseEntity<>(e.getCause(), HttpStatus.EXPECTATION_FAILED);
		}
	}

	@ApiOperation(value = "API to delete Cloud Template by ID")
	@DeleteMapping("/deletecloudtemplatebyid/{templateId}")
	public ResponseEntity<?> deleteCloudTemplateById(@PathVariable Long templateId) {
		try {
			crCloudTemplateService.deleteCloudTemplateById(templateId);
			return new ResponseEntity<>("Success", HttpStatus.OK);
		} catch (Exception e) {
			log.error(e.getMessage());
			return new ResponseEntity<>(e.getCause(), HttpStatus.EXPECTATION_FAILED);
		}
	}

	@ApiOperation(value = "Api to get existing Cloud Template Columns data based on the given TemplateId")
	@GetMapping("/getcloudtemplatecolumns")
	public ResponseEntity<?> getCloudTemplateColumns(@RequestParam("templateId") Long templateId) {
		try {
			return new ResponseEntity<>(crCloudTemplateService.getCloudTemplateColumns(templateId), HttpStatus.OK);
		} catch (Exception e) {
			log.error(e.getMessage());
			return new ResponseEntity<>(e.getMessage(), HttpStatus.EXPECTATION_FAILED);
		}
	}

	@ApiOperation(value = "Api to get existing Cloud Template Columns data based on the given Column Id")
	@GetMapping("/getcloudtemplatecolumnbyid/{columnId}")
	public ResponseEntity<?> getCloudTemplateColumnById(@PathVariable("columnId") Long columnId)
			throws ConvertRiteException {
		try {
			return new ResponseEntity<>(crCloudTemplateService.getCloudTemplateByColumnId(columnId), HttpStatus.OK);
		} catch (Exception e) {
			log.error(e.getMessage());
			return new ResponseEntity<>(e.getCause(), HttpStatus.EXPECTATION_FAILED);
		}
	}

	@ApiOperation(value = "This Api is to save cloud template column")
	@PostMapping("/savecloudtemplatecolumn")
	public ResponseEntity<?> saveCloudTemplateColumns(
			@RequestBody CrCloudTemplateColumnsReqPo cloudTemplateColumnsReqPo) {
		try {
			return new ResponseEntity<>(crCloudTemplateService.saveCloudTemplateColumn(cloudTemplateColumnsReqPo),
					HttpStatus.OK);
		} catch (Exception e) {
			log.error(e.getMessage());
			return new ResponseEntity<>(e.getCause(), HttpStatus.EXPECTATION_FAILED);
		}
	}

	@ApiOperation(value = "This Api is to save cloud template columns")
	@PostMapping("/saveallcloudtemplatecolumns")
	public ResponseEntity<?> saveAllCloudTemplateColumns(
			@RequestBody List<CrCloudTemplateColumnsReqPo> cloudTemplateColumnsReqPo) {
		try {
			System.out.println();
			return new ResponseEntity<>(crCloudTemplateService.saveAllCloudTemplateColumns(cloudTemplateColumnsReqPo),
					HttpStatus.OK);
		} catch (Exception e) {
			log.error(e.getMessage());
			return new ResponseEntity<>(e.getCause(), HttpStatus.EXPECTATION_FAILED);
		}
	}

	@ApiOperation(value = "This Api returns source and cloud cloumns,we has to provide sourcetemplateName and cloudtable names as query params")
	@GetMapping("/getcloudsourcecolumns")
	public ResponseEntity<?> getCloudSourceColumns(
			@RequestParam(required = false, name = "sourceTemplateName") String sourceTemplateName,
			@RequestParam("cloudTableName") String cloudTableName) throws ConvertRiteException {
		try {
			return new ResponseEntity<>(
					crCloudTemplateService.getCloudSourceColumns(sourceTemplateName, cloudTableName), HttpStatus.OK);
		} catch (Exception e) {
			log.error(e.getMessage());
			return new ResponseEntity<>(e.getCause(), HttpStatus.EXPECTATION_FAILED);
		}
	}

	@ApiOperation(value = "This Api returns source and cloud cloumns,we has to provide sourceTemplateId and CloudTableId names as query params")
	@GetMapping("/getcloudsourcecolumnsbyids")
	public ResponseEntity<CloudSourceColumnsPo> getCloudSourceColumnsById(
			@RequestParam(required = false, name = "templateId") Long templateId, @RequestParam("tableId") Long tableId)
			throws ConvertRiteException {

		CloudSourceColumnsPo cloudSourceColumnsPo = new CloudSourceColumnsPo();
		try {
			if (tableId != null)
				cloudSourceColumnsPo = crCloudTemplateService.getCloudSourceColumnsByIds(templateId, tableId);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<CloudSourceColumnsPo>(cloudSourceColumnsPo, new HttpHeaders(), HttpStatus.OK);

	}

	@ApiOperation(value = "This Api will return Mapping Set Names")
	@GetMapping("/getcloudmappingsetnames")
	public ResponseEntity<?> getCloudMappingSetNames() throws ConvertRiteException {
		try {
			return new ResponseEntity<>(crCloudTemplateService.getCloudMappingSetNames(), HttpStatus.OK);
		} catch (Exception e) {
			log.error(e.getMessage());
			return new ResponseEntity<>(e.getCause(), HttpStatus.EXPECTATION_FAILED);
		}
	}

	/*
	 * @ApiOperation(value = "This Api Create Cloud Stagging Table")
	 * 
	 * @PostMapping("/createcloudstgtable") public ResponseEntity<?>
	 * createCloudStgTable(@RequestParam("tableName") String tableName,
	 * 
	 * @RequestParam("templateId") Long templateId, HttpServletRequest request)
	 * throws ConvertRiteException { CloudStagingTablePo stagingPo = new
	 * CloudStagingTablePo(); try { return new
	 * ResponseEntity<>(crCloudTemplateService.createCloudStaggingTab(tableName,
	 * templateId, request),HttpStatus.OK); } catch (Exception e) { return new
	 * ResponseEntity<>(e.getCause(), HttpStatus.EXPECTATION_FAILED); } }
	 */

	@ApiOperation(value = "This Api will return List of TableNames and SourceTemplateHeaderNames")
	@GetMapping("/getcloudtablestemplates")
	public ResponseEntity<CloudTablesTemplatesPo> getCloudTablesTemplates(@RequestParam("projectId") Long projectId,
			@RequestParam("objectId") Long objectId, @RequestParam("parentObjectId") Long parentObjectId)
			throws ConvertRiteException {
		CloudTablesTemplatesPo cloudTablesTemplates = new CloudTablesTemplatesPo();
		try {
			if (projectId != null && parentObjectId != null && objectId != null)
				cloudTablesTemplates = crCloudTemplateService.getCloudTablesTemplates(projectId, objectId,
						parentObjectId);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<CloudTablesTemplatesPo>(cloudTablesTemplates, new HttpHeaders(), HttpStatus.OK);
	}
	
	
	/*
	 * Hook Usage APIs
	 */
	@ApiOperation(value = "API to get All Hooks Added to Cloud Template")
	@GetMapping("/getTemplateHooks/{cloudTemplateId}")
	public ResponseEntity<?> getTemplateHooks(@PathVariable Long cloudTemplateId) {
		try {
			return new ResponseEntity<>(crHookUsageService.getHookUsagesByTemplateId(cloudTemplateId), HttpStatus.OK);
		} catch (Exception e) {
			log.error(e.getMessage());
			return new ResponseEntity<>(e.getCause(), HttpStatus.EXPECTATION_FAILED);
		}
	}

	@ApiOperation(value = "API to Save/Update/Delete of Hook Usages of Template")
	@PostMapping("/saveTemplateHooks/{cloudTemplateId}")
	public ResponseEntity<?> saveHookUsages(@RequestBody List<CrHookUsagesCreateReqPo> createReqPos,
			@PathVariable Long cloudTemplateId) {
		try {
			return new ResponseEntity<>(crHookUsageService.saveHookUsage(createReqPos, cloudTemplateId), HttpStatus.OK);
		} catch (Exception e) {
			log.error(e.getMessage());
			return new ResponseEntity<>(e.getCause(), HttpStatus.EXPECTATION_FAILED);
		}
	}

	@ApiOperation(value = "API to get All Hooks Usage By Usage Id")
	@GetMapping("/getHookUsageById/{hookUsageId}")
	public ResponseEntity<?> gethookUsageById(@PathVariable Long hookUsageId) {
		try {
			return new ResponseEntity<>(crHookUsageService.gethookUsageById(hookUsageId), HttpStatus.OK);
		} catch (Exception e) {
			log.error(e.getMessage());
			return new ResponseEntity<>(e.getCause(), HttpStatus.EXPECTATION_FAILED);
		}
	}

	@ApiOperation(value = "API to delete Hooks Usage By Usage Id")
	@DeleteMapping("/deleteHookUsageById/{hookUsageId}")
	public ResponseEntity<BasicResponsePo> deleteHookUsageById(@PathVariable Long hookUsageId) {
		BasicResponsePo respo = new BasicResponsePo();
		try {
			respo = crHookUsageService.deleteHookUsageById(hookUsageId);
			return new ResponseEntity<>(respo, HttpStatus.OK);
		} catch (Exception e) {
			log.error(e.getMessage());
			return new ResponseEntity<>(respo, HttpStatus.EXPECTATION_FAILED);
		}
	}
}


