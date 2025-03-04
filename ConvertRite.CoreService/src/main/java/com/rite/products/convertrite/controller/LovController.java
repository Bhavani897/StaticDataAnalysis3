package com.rite.products.convertrite.controller;

import java.util.ArrayList;
import java.util.List;

import com.rite.products.convertrite.respository.CrLookUpValuesRepo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import com.rite.products.convertrite.exception.ConvertRiteException;
import com.rite.products.convertrite.po.BuLovPo;
import com.rite.products.convertrite.po.CloudMappingSetPo;
import com.rite.products.convertrite.po.CloudTablesTemplatesPo;
import com.rite.products.convertrite.po.CommonLovPo;
import com.rite.products.convertrite.po.LovPo;
import com.rite.products.convertrite.po.LovValuesPo;
import com.rite.products.convertrite.po.ObjectCodeLovPo;
import com.rite.products.convertrite.po.ParentObjectCodeResPo;
import com.rite.products.convertrite.po.PodsPo;
import com.rite.products.convertrite.po.ProjectEntryLovPo;
import com.rite.products.convertrite.po.ProjectsPo;
import com.rite.products.convertrite.service.LovService;
import com.rite.products.convertrite.service.XxrCloudService;

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

@RestController
@RequestMapping(value = "/api/convertritecore")
public class LovController {

	private static final Logger log = LoggerFactory.getLogger(LovController.class);

	@Autowired
	XxrCloudService xxrCloudService;
	@Autowired
	LovService lovService;

	@Autowired
	CrLookUpValuesRepo crLookUpValuesRepo;

	// private int ResponseEntity;

	@ApiOperation(value = "This API will return lov values,pass following values {BU,OBJECTCODE,PARENTOBJECTCODE} in array to fetch values")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/getlovvalues")
	public ResponseEntity<LovPo> getLovValues(@RequestParam("lovValues") String[] lovValues,
			@RequestParam("podId") Long podId, @RequestParam("projectId") Long projectId) throws ConvertRiteException {
		log.info("Start of getLovValues Method in LovController ###");
		LovPo cloudLovPo = new LovPo();
		try {
			if (lovValues.length != 0 && podId != null && projectId != null)
				cloudLovPo = xxrCloudService.getCloudLovValues(lovValues, podId, projectId);
			/*
			 * else throw new ConvertRiteException(
			 * "Pass atleast one Lov Value which you want to fetch or Missing podid or project Id in request"
			 * , HttpStatus.BAD_REQUEST);
			 */
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}

		return new ResponseEntity<LovPo>(cloudLovPo, new HttpHeaders(), HttpStatus.OK);
	}

	@ApiOperation(value = "This API will return lov values,pass following values {BU,OBJECTCODE,PARENTOBJECTCODE} in array to fetch values")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/getalllovvalues")
	public ResponseEntity<LovPo> getAllLovValues(@RequestParam("lovValues") String[] lovValues)
			throws ConvertRiteException {
		log.info("Start of getAllLovValues Method in LovController ###");
		LovPo cloudLovPo = new LovPo();
		try {
			cloudLovPo = xxrCloudService.getAllLovValues(lovValues);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}

		return new ResponseEntity<LovPo>(cloudLovPo, new HttpHeaders(), HttpStatus.OK);
	}

	@ApiOperation(value = "This Api will return list of unique POD Id’s and names")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/getpods")
	public ResponseEntity<List<PodsPo>> getPods() throws ConvertRiteException {
		log.info("Start of getPods Method in LovController ###");
		List<Object> podLovList = new ArrayList<>();
		List<PodsPo> podList = new ArrayList<>();
		try {
			// cloudMasterLookUpSetList = xxrCloudService.getPods();

			podLovList = lovService.getPods();

			podLovList.stream().forEach(x -> {
				PodsPo podsPo = new PodsPo();
				Object[] obj = (Object[]) x;
				podsPo.setPodId((long) obj[0]);
				podsPo.setPodName((String) obj[1]);
				podList.add(podsPo);
			});

		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<List<PodsPo>>(podList, new HttpHeaders(), HttpStatus.OK);
	}

	@ApiOperation(value = "This Api will return list of unique BU Id's and name's")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/getbulov")
	public ResponseEntity<List<BuLovPo>> getBuLov() throws ConvertRiteException {
		log.info("Start of getBuLov Method in LovController ###");
		List<BuLovPo> buLovList = new ArrayList<>();
		try {
			// cloudMasterLookUpSetList = xxrCloudService.getPods();

			buLovList = lovService.getBuLov();

		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<List<BuLovPo>>(buLovList, new HttpHeaders(), HttpStatus.OK);
	}

	@ApiOperation(value = "This Api will return list of unique POD Id’s and names")

	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),

			@ApiResponse(code = 500, message = "Server Side Error") })

	@GetMapping("/getcommonlov")
	public ResponseEntity<CommonLovPo> getCommonLov(@RequestParam("podId") Long podId) throws ConvertRiteException {
		log.info("Start of getCommonLov Method in LovController ###");
		CommonLovPo commonLovPo = new CommonLovPo();
		try {

			commonLovPo = lovService.getCommonLov(podId);

		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<CommonLovPo>(commonLovPo, new HttpHeaders(), HttpStatus.OK);
	}

	@ApiOperation(value = "This Api will return Unique Set of ProjectId and ProjectNames based on podid")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/getprojectsbypod")
	public ResponseEntity<List<LovValuesPo>> getProjectsByPod(@RequestParam("podId") Long podId)
			throws ConvertRiteException {
		log.info("Start of getProjectsByPod Method in LovController ####");
		log.debug("podId#### " + podId);
		List<LovValuesPo> cloudMasterLookUpSetList = new ArrayList<>();
		try {
			if (podId != null)
				cloudMasterLookUpSetList = xxrCloudService.getProjectsByPod(podId);
			/*
			 * else throw new ConvertRiteException("Missing PodId in the Request",
			 * HttpStatus.BAD_REQUEST);
			 */
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<List<LovValuesPo>>(cloudMasterLookUpSetList, new HttpHeaders(), HttpStatus.OK);
	}

	@ApiOperation(value = "This Api will return Unique Set of ProjectId and ProjectNames")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/getprojects")
	public ResponseEntity<List<LovValuesPo>> getProjects() throws ConvertRiteException {
		log.info("Start of getProjects Method in LovController ###");
		List<LovValuesPo> cloudMasterLookUpSetList = new ArrayList<>();
		try {

			cloudMasterLookUpSetList = xxrCloudService.getProjects();

		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<List<LovValuesPo>>(cloudMasterLookUpSetList, new HttpHeaders(), HttpStatus.OK);
	}

	/*@ApiOperation(value = "This Api will return List of TableNames and SourceTemplateHeaderNames")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/getCloudTablesTemplates")
	public ResponseEntity<CloudTablesTemplatesPo> getCloudTablesTemplates(@RequestParam("podId") Long podId,
			@RequestParam("projectId") Long projectId, @RequestParam("objectId") Long objectId,
			@RequestParam("parentObjectId") Long parentObjectId) throws ConvertRiteException {
		log.info("Start of getCloudTablesTemplates Method in LovController ###");
		log.debug("podId#### " + podId + "projectId###" + projectId + "objectId##" + objectId + "parentObjectId##"
				+ parentObjectId);
		CloudTablesTemplatesPo cloudTablesTemplates = new CloudTablesTemplatesPo();
		try {
			if (podId != null && projectId != null && parentObjectId != null && objectId != null)
				cloudTablesTemplates = xxrCloudService.getCloudTablesTemplates(projectId, podId, objectId,
						parentObjectId);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<CloudTablesTemplatesPo>(cloudTablesTemplates, new HttpHeaders(), HttpStatus.OK);
	}*/

	/*
	 * @ApiOperation(value = "This Api will return Mapping Set Names")
	 * 
	 * @ApiResponses(value = { @ApiResponse(code = 200, message =
	 * "Successful Response"),
	 * 
	 * @ApiResponse(code = 500, message = "Server Side Error") })
	 * 
	 * @GetMapping("/getcloudmappingsetnames") public
	 * ResponseEntity<List<CloudMappingSetPo>> getCloudMappingSetNames(
	 * 
	 * @RequestParam(required = false, name = "podId") Long podId,
	 * 
	 * @RequestParam(required = false, name = "projectId") Long
	 * projectId, @RequestParam("objectId") Long objectId,
	 * 
	 * @RequestParam(required = false, name = "bu") Integer bu) throws
	 * ConvertRiteException {
	 * log.info("Start of getCloudMappingSetNames Method in LovController ###");
	 * log.info("podId#### " + podId + "projectId###" + projectId + "objectId##" +
	 * objectId + "bu###" + bu); List<CloudMappingSetPo> cloudMappingSetPo = new
	 * ArrayList<>(); try { if (objectId != null) cloudMappingSetPo =
	 * xxrCloudService.getCloudMappingSetNames(projectId, podId, objectId, bu);
	 * 
	 * else throw new ConvertRiteException("objectId is Mandatory",
	 * HttpStatus.BAD_REQUEST);
	 * 
	 * } catch (Exception e) { log.error(e.getMessage()); throw new
	 * ConvertRiteException(
	 * "Please contact System Administrator there is an error while processing the request"
	 * , HttpStatus.INTERNAL_SERVER_ERROR); } return new
	 * ResponseEntity<List<CloudMappingSetPo>>(cloudMappingSetPo, new HttpHeaders(),
	 * HttpStatus.OK); }
	 */

	/*@ApiOperation(value = "This Api will return Mapping Set Names")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/getcloudmappingsetnames")
	public ResponseEntity<List<CloudMappingSetPo>> getCloudMappingSetNames() throws ConvertRiteException {
		log.info("Start of getCloudMappingSetNames Method in LovController ###");
		List<CloudMappingSetPo> cloudMappingSetPo = new ArrayList<>();
		try {
			cloudMappingSetPo = xxrCloudService.getCloudMappingSetNames();
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<List<CloudMappingSetPo>>(cloudMappingSetPo, new HttpHeaders(), HttpStatus.OK);
	}*/

	@ApiOperation(value = "Pass lovValue as 'Related To' ")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/getrelatedtolov")
	public ResponseEntity<List<LovValuesPo>> getRelatedToLov(@RequestParam("lovValue") String lovValue)
			throws ConvertRiteException {
		log.info("Start of getProjectEntryLovs Method in LovController ###");
		List<LovValuesPo> relatedToList = new ArrayList<>();
		try {
			relatedToList = lovService.getRelatedToLov(lovValue);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}

		return new ResponseEntity<List<LovValuesPo>>(relatedToList, new HttpHeaders(), HttpStatus.OK);
	}

	@ApiOperation(value = "Get actualvalue lov")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/getactualvaluelov")
	public ResponseEntity<List<LovValuesPo>> getActualValueLov(@RequestParam("relatedTo") String relatedTo)
			throws ConvertRiteException {
		log.info("Start of getActualValueLov Method in LovController ###");
		List<LovValuesPo> actualValueList = new ArrayList<>();
		try {
			actualValueList = lovService.getActualValueLov(relatedTo);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}

		return new ResponseEntity<List<LovValuesPo>>(actualValueList, new HttpHeaders(), HttpStatus.OK);
	}

	@ApiOperation(value = "Get Lov based on lookupsetName")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/getlovbylookupsetname")
	public ResponseEntity<List<LovValuesPo>> getLovByLookUpSetName(@RequestParam("lookupSetName") String lookupSetName)
			throws ConvertRiteException {
		log.info("Start of getLovByLookUpSetName Method in LovController ###");
		List<LovValuesPo> lovValueList = new ArrayList<>();
		try {
			//lovValueList = lovService.getLovByLookUpSetName(lookupSetName);
			lovValueList =crLookUpValuesRepo.getLookUpValues(lookupSetName);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}

		return new ResponseEntity<List<LovValuesPo>>(lovValueList, new HttpHeaders(), HttpStatus.OK);
	}

	@ApiOperation(value = "Pass{PROJECT_NAME,PROJECT_MANAGER,PROGRAM_NUM,CLIENT_MANAGER,KPI_AGGREGATION_LEVEL,PROJECT_STATUS,ACCESS_LEVEL,ITERATION_TYPE,PROJECT_TYPE,CLIENT_PROJECT_NUM,TASK_TYPE}")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/getprojectentrylovs")
	public ResponseEntity<ProjectEntryLovPo> getProjectEntryLovs(@RequestParam("lovValues") String[] lovValues)
			throws ConvertRiteException {
		log.info("Start of getProjectEntryLovs Method in LovController ###");
		ProjectEntryLovPo projectEntryLovPo = new ProjectEntryLovPo();
		try {
			projectEntryLovPo = lovService.getProjectEntryLovs(lovValues);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}

		return new ResponseEntity<ProjectEntryLovPo>(projectEntryLovPo, new HttpHeaders(), HttpStatus.OK);
	}

	@ApiOperation(value = "Get Object Code based on ParentObjectId")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/getobjectcodes")
	public ResponseEntity<List<ObjectCodeLovPo>> getObjectCodes(@RequestParam("parentObjectId") Long parentObjectId)
			throws ConvertRiteException {
		log.info("Start of getObjectCodes Method in LovController ###");
		List<ObjectCodeLovPo> objectCodeList = new ArrayList<>();
		try {
			objectCodeList = lovService.getObjectCodes(parentObjectId);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}

		return new ResponseEntity<List<ObjectCodeLovPo>>(objectCodeList, new HttpHeaders(), HttpStatus.OK);
	}

	@ApiOperation(value = "Get all Object Codes independent of parent object code")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/getallobjectcodes")
	public ResponseEntity<List<ObjectCodeLovPo>> getAllObjectCodes() throws ConvertRiteException {
		log.info("Start of getAllObjectCodes Method in LovController ###");
		List<ObjectCodeLovPo> objectCodeList = new ArrayList<>();
		try {
			objectCodeList = lovService.getAllObjectCodes();
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}

		return new ResponseEntity<List<ObjectCodeLovPo>>(objectCodeList, new HttpHeaders(), HttpStatus.OK);
	}

	@ApiOperation(value = "Get ParentObjectCodes based on role,podId and projectId")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/v1/getparentobjectcodebyrole")
	public ResponseEntity<List<ParentObjectCodeResPo>> getParentObjectCodeByRole(@RequestHeader("role") String role,
			@RequestParam("podId") Long podId, @RequestParam("projectId") Long projectId) throws ConvertRiteException {
		log.info("Start of getParentObjectCodeByRole####");
		List<ParentObjectCodeResPo> parentObjectCodeResLi = new ArrayList<>();
		try {
			parentObjectCodeResLi = lovService.getParentObjectCodeByRole(role, podId, projectId);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<List<ParentObjectCodeResPo>>(parentObjectCodeResLi, HttpStatus.OK);
	}

	@ApiOperation(value = "Get ParentObjectCodes based on role and projectId")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/getparentobjectcodebyrole")
	public ResponseEntity<List<ParentObjectCodeResPo>> getParentObjectCodeByRole(@RequestHeader("role") String role,
			@RequestParam("projectId") Long projectId) throws ConvertRiteException {
		log.info("Start of getParentObjectCodeByRole####");
		List<ParentObjectCodeResPo> parentObjectCodeResLi = new ArrayList<>();
		try {
			parentObjectCodeResLi = lovService.getParentObjectCodeByRole(role, projectId);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<List<ParentObjectCodeResPo>>(parentObjectCodeResLi, HttpStatus.OK);
	}

	@ApiOperation(value = "Get ParentObjectCodes based on only role")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/getparentobjectcodes")
	public ResponseEntity<List<ParentObjectCodeResPo>> getParentObjectCodes(@RequestHeader("role") String role)
			throws ConvertRiteException {
		log.info("Start of getParentObjectCodes####");
		List<ParentObjectCodeResPo> parentObjectCodeResLi = new ArrayList<>();
		try {
			parentObjectCodeResLi = lovService.getParentObjectCodes(role);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<List<ParentObjectCodeResPo>>(parentObjectCodeResLi, HttpStatus.OK);
	}

	@ApiOperation(value = "This Api will return list of unique POD Id’s and names by role")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/getpodsbyrole")
	public ResponseEntity<List<PodsPo>> getPodsByRole(@RequestHeader("role") String role) throws ConvertRiteException {
		log.info("Start of getPodsByRole Method in LovController ###");
		List<PodsPo> podList = new ArrayList<>();
		try {
			podList = lovService.getPodsByRole(role);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<List<PodsPo>>(podList, new HttpHeaders(), HttpStatus.OK);
	}

	@ApiOperation(value = "This Api will return list of unique projectId's & ProjectName based on pod & role")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/v1/getprojectsbyrole")
	public ResponseEntity<List<ProjectsPo>> getProjectsByRole(@RequestHeader("role") String role,
			@RequestParam("podId") Long podId) throws ConvertRiteException {
		log.info("Start of getProjectsByRole Method in LovController ###");
		List<ProjectsPo> projectList = new ArrayList<>();
		try {
			projectList = lovService.getProjectsByRole(role, podId);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<List<ProjectsPo>>(projectList, new HttpHeaders(), HttpStatus.OK);
	}

	@ApiOperation(value = "This Api will return list of unique projectId's & ProjectName based on  role")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/getprojectsbyrole")
	public ResponseEntity<List<ProjectsPo>> getProjectsByRole(@RequestHeader("role") String role)
			throws ConvertRiteException {
		log.info("Start of getProjectsByRole Method in LovController ###");
		List<ProjectsPo> projectList = new ArrayList<>();
		try {
			projectList = lovService.getProjectsByRole(role);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<List<ProjectsPo>>(projectList, new HttpHeaders(), HttpStatus.OK);
	}

}
