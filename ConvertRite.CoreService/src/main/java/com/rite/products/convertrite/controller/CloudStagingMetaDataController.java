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
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.rite.products.convertrite.Validations.Validations;
import com.rite.products.convertrite.exception.BadRequestException;
import com.rite.products.convertrite.exception.ConvertRiteException;
import com.rite.products.convertrite.exception.ValidationException;
import com.rite.products.convertrite.model.XxrCldTempHdrsView;
import com.rite.products.convertrite.po.CldSrcTemplateIdsResPo;
import com.rite.products.convertrite.po.CloudSourceColumnsPo;
import com.rite.products.convertrite.po.CloudStagingTablePo;
import com.rite.products.convertrite.po.CopyCloudReqPo;
import com.rite.products.convertrite.po.CreateDynamicViewPo;
import com.rite.products.convertrite.po.SaveCloudTemplateColumnsPo;
import com.rite.products.convertrite.po.SaveCloudTemplateColumnsResPo;
import com.rite.products.convertrite.po.SaveCloudTemplateHeadersPo;
import com.rite.products.convertrite.po.SaveTemplateHeaderResponsePo;
import com.rite.products.convertrite.po.XxrCloudTemplateColumnsResPo;
import com.rite.products.convertrite.service.XxrCloudService;

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

@RestController

public class CloudStagingMetaDataController {
	private static final Logger log = LoggerFactory.getLogger(CloudStagingMetaDataController.class);

	@Autowired
	XxrCloudService xxrCloudService;

	/*
	 * @ApiOperation(value =
	 * "For this Api no need to send any input parameters,Api returns cloudmetadata details"
	 * )
	 * 
	 * @ApiResponses(value = { @ApiResponse(code = 200, message =
	 * "Successful Response"),
	 * 
	 * @ApiResponse(code = 500, message = "Server Side Error") })
	 * 
	 * @GetMapping("/getcloudtemplatemetadata") public
	 * ResponseEntity<XxrCloudTemplatePo> getCloudTemplateMetaData() throws
	 * ConvertRiteException { log.
	 * info("Start of getCloudTemplateMetaData Method in CloudStagingMetaDataController :::"
	 * ); XxrCloudTemplatePo cloudTemplatePo = new XxrCloudTemplatePo(); try {
	 * cloudTemplatePo = xxrCloudService.getAllCloudData(); } catch (Exception e) {
	 * log.error(e.getMessage()); throw new ConvertRiteException(
	 * "Please contact System Administrator there is an error while processing the request"
	 * , HttpStatus.INTERNAL_SERVER_ERROR); } return new
	 * ResponseEntity<XxrCloudTemplatePo>(cloudTemplatePo, new HttpHeaders(),
	 * HttpStatus.OK); }
	 */

	/*@ApiOperation(value = "This Api returns source and cloud cloumns,we has to provide sourcetemplateName and cloudtable names as query params")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error"),
			@ApiResponse(code = 400, message = "Bad Request") })
	@GetMapping("/getcloudsourcecolumns")
	public ResponseEntity<CloudSourceColumnsPo> getCloudSourceColumns(
			@RequestParam(required = false, name = "sourceTemplateName") String sourceTemplateName,
			@RequestParam("cloudTableName") String cloudTableName) throws ConvertRiteException {
		log.info("Start of getCloudSourceColumns Method in Controller ###");
		log.debug("sourceTemplateName:::::::" + sourceTemplateName + "CloudTableName:::::::" + cloudTableName);
		CloudSourceColumnsPo cloudSourceColumnsPo = new CloudSourceColumnsPo();
		try {
			if (!Validations.isNullOrEmpty(cloudTableName))
				cloudSourceColumnsPo = xxrCloudService.getCloudSourceColumns(sourceTemplateName, cloudTableName);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<CloudSourceColumnsPo>(cloudSourceColumnsPo, new HttpHeaders(), HttpStatus.OK);

	}*/

	/*
	 * @ApiOperation(value =
	 * "This Api return already existing cloud templates based on user input data")
	 * 
	 * @ApiResponses(value = { @ApiResponse(code = 200, message =
	 * "Successful Response"),
	 * 
	 * @ApiResponse(code = 500, message = "Server Side Error"),
	 * 
	 * @ApiResponse(code = 400, message = "Bad Request") })
	 * 
	 * @PostMapping("/getcloudtemplates") public
	 * ResponseEntity<List<XxrCloudTemplateHeader>> getCloudTemplates(@RequestBody
	 * CloudTemplatePo cloudTemplatePo) throws ConvertRiteException {
	 * log.info("Start of getCloudTemplates Method in Controller ###");
	 * List<XxrCloudTemplateHeader> cloudTemplateHeaderList = new ArrayList<>(); try
	 * { if (!Validations.isNullOrEmpty(cloudTemplatePo.getObjectCode()))
	 * cloudTemplateHeaderList = xxrCloudService.getCloudTemplate(cloudTemplatePo);
	 * else throw new ConvertRiteException("Missing ObjectCode in Request Object",
	 * HttpStatus.BAD_REQUEST); } catch (Exception e) { log.error(e.getMessage());
	 * throw new ConvertRiteException(
	 * "Please contact System Administrator there is an error while processing the request"
	 * , HttpStatus.INTERNAL_SERVER_ERROR); } return new
	 * ResponseEntity<List<XxrCloudTemplateHeader>>(cloudTemplateHeaderList, new
	 * HttpHeaders(), HttpStatus.OK); }
	 */

	/*@ApiOperation(value = "This Api return all cloud templates")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error"),
			@ApiResponse(code = 400, message = "Bad Request") })
	@GetMapping("/getallcloudtemplates")
	public ResponseEntity<List<XxrCldTempHdrsView>> getAllCloudTemplates(@RequestHeader("roleid") Long roleId) throws ConvertRiteException {
		log.info("Start of getAllCloudTemplates Method in Controller ###");
		List<XxrCldTempHdrsView> cloudTemplateHeaderList = new ArrayList<>();
		try {
			cloudTemplateHeaderList = xxrCloudService.getAllCloudTemplates(roleId);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<List<XxrCldTempHdrsView>>(cloudTemplateHeaderList, new HttpHeaders(), HttpStatus.OK);
	}*/

	/*
	 * @ApiOperation(value = "This Api return all cloud templates")
	 * 
	 * @ApiResponses(value = { @ApiResponse(code = 200, message =
	 * "Successful Response"),
	 * 
	 * @ApiResponse(code = 500, message = "Server Side Error"),
	 * 
	 * @ApiResponse(code = 400, message = "Bad Request") })
	 * 
	 * @GetMapping("/getallcloudtemplates") public
	 * ResponseEntity<List<XxrCloudTemplateHeaderResPo>> getAllCloudTemplates()
	 * throws ConvertRiteException {
	 * log.info("Start of getAllCloudTemplates Method in Controller ###");
	 * List<XxrCloudTemplateHeaderResPo> cloudTemplateHeaderList = new
	 * ArrayList<>(); try { cloudTemplateHeaderList =
	 * xxrCloudService.getAllCloudTemplates(); } catch (Exception e) {
	 * log.error(e.getMessage()); throw new ConvertRiteException(
	 * "Please contact System Administrator there is an error while processing the request"
	 * , HttpStatus.INTERNAL_SERVER_ERROR); } return new
	 * ResponseEntity<List<XxrCloudTemplateHeaderResPo>>(cloudTemplateHeaderList,
	 * new HttpHeaders(), HttpStatus.OK); }
	 */

	/*@ApiOperation(value = "This Api return Existing Cloud Template Columns data based on the TemplateId passed by user")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error"),
			@ApiResponse(code = 400, message = "Bad Request") })
	@GetMapping("/getcloudtemplatecolumns")
	public ResponseEntity<List<XxrCloudTemplateColumnsResPo>> getCloudTemplateColumns(
			@RequestParam("templateId") Long templateId) throws ConvertRiteException {
		log.info("Start of getCloudTemplateColumns Method in Controller ###");
		log.debug("TemplateId:::::" + templateId);
		List<XxrCloudTemplateColumnsResPo> cloudTemplateColumnsList = new ArrayList<>();
		try {
			if (templateId != null)
				cloudTemplateColumnsList = xxrCloudService.getCloudTemplateColumns(templateId);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<List<XxrCloudTemplateColumnsResPo>>(cloudTemplateColumnsList, HttpStatus.OK);
	}

	@ApiOperation(value = "This Api returns source and cloud cloumns,we has to provide sourceTemplateId and CloudTableId names as query params")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error"),
			@ApiResponse(code = 400, message = "Bad Request") })
	@GetMapping("/getcloudsourcecolumnsbyids")
	public ResponseEntity<CloudSourceColumnsPo> getCloudSourceColumnsById(
			@RequestParam(required = false, name = "templateId") Long templateId, @RequestParam("tableId") Long tableId)
			throws ConvertRiteException {

		log.info("Start of getCloudSourceColumnsById Method in Controller ###");
		log.debug("sourceTemplateId:::::::" + templateId + "CloudTableId:::::::" + tableId);
		CloudSourceColumnsPo cloudSourceColumnsPo = new CloudSourceColumnsPo();
		try {
			if (tableId != null)
				cloudSourceColumnsPo = xxrCloudService.getCloudSourceColumnsByIds(templateId, tableId);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<CloudSourceColumnsPo>(cloudSourceColumnsPo, new HttpHeaders(), HttpStatus.OK);

	}

	@ApiOperation(value = "This Api Create Cloud Stagging Table")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error"),
			@ApiResponse(code = 400, message = "Bad Request") })
	@PostMapping("/createcloudstgtable")
	public ResponseEntity<CloudStagingTablePo> createCloudStgTable(@RequestParam("tableName") String tableName,
			@RequestParam("templateId") Long templateId, HttpServletRequest request)
			throws ConvertRiteException {
		log.info("Start of createCloudStgTable Method in Controller ###");
		log.debug("tableName:::::::" + tableName + "templateId:::::::" + templateId );
		CloudStagingTablePo stagingPo = new CloudStagingTablePo();
		try {
			if (!(Validations.isNullOrEmpty(tableName)) && templateId != null)
				stagingPo = xxrCloudService.createCloudStaggingTab(tableName, templateId, request);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<CloudStagingTablePo>(stagingPo, new HttpHeaders(), HttpStatus.OK);
	}*/

	@ApiOperation(value = "This Api Create Dynamic View")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error"),
			@ApiResponse(code = 400, message = "Bad Request") })
	@PostMapping("/createdynamicview")
	public ResponseEntity<CreateDynamicViewPo> createDynamicView(@RequestParam("templateId") Long templateId,
			@RequestParam("stgTableName") String stgTableName, HttpServletRequest request) throws ConvertRiteException {
		log.info("Start of createDynamicView Method in Controller ###");
		log.debug("templateId:::::::" + templateId + "stgTableName:::::::" + stgTableName);
		CreateDynamicViewPo createDynamicViewPo = new CreateDynamicViewPo();
		try {
			if (templateId != null && !Validations.isNullOrEmpty(stgTableName))
				createDynamicViewPo = xxrCloudService.createDynamicView(templateId, stgTableName, request);
			/*
			 * else throw new
			 * ConvertRiteException("Missing templateId or objectCode in the Request",
			 * HttpStatus.BAD_REQUEST);
			 */
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}

		return new ResponseEntity<CreateDynamicViewPo>(createDynamicViewPo, new HttpHeaders(), HttpStatus.OK);

	}

	/*@ApiOperation(value = "This Api is to save cloudtemplateheaders")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error"),
			@ApiResponse(code = 400, message = "Bad Request") })
	@PostMapping("/savecloudtemplateheaders")
	public ResponseEntity<SaveTemplateHeaderResponsePo> saveCloudHeaders(
			@RequestBody List<SaveCloudTemplateHeadersPo> cloudTemplateHeadersPo, HttpServletRequest request) {
		log.info("Start of savecloudheaders Method in Controller ###");

		SaveTemplateHeaderResponsePo saveTemplateHeaderResponsePo = new SaveTemplateHeaderResponsePo();
		try {

			SaveCloudTemplateHeadersPo cloudHeader = cloudTemplateHeadersPo.get(0);
			if (cloudHeader != null) {
				if ( cloudHeader.getProjectId() == null
						|| cloudHeader.getObjectId() == null || cloudHeader.getParentObjectId() == null
						|| cloudHeader.getMetaDataTableId() == null
						|| Validations.isNullOrEmpty(cloudHeader.getTemplateName()))
					throw new BadRequestException(
							"projectId,parentObjectId,objectId,templateName and metaDataTableId are Mandatory fields");
			}
			saveTemplateHeaderResponsePo = xxrCloudService.saveCloudHeaders(cloudTemplateHeadersPo, request);

		} catch (ValidationException e) {
			log.error(e.getMessage() + "ValidationException");
			saveTemplateHeaderResponsePo.setMessage(e.getMessage());
			saveTemplateHeaderResponsePo.setError("Error while saving cloud template");
			return new ResponseEntity<SaveTemplateHeaderResponsePo>(saveTemplateHeaderResponsePo, HttpStatus.OK);
		} catch (BadRequestException e) {
			log.error(e.getMessage() + "BadRequestException");
			saveTemplateHeaderResponsePo.setMessage(e.getMessage());
			saveTemplateHeaderResponsePo.setError("Error while saving cloud template");
			return new ResponseEntity<SaveTemplateHeaderResponsePo>(saveTemplateHeaderResponsePo,
					HttpStatus.BAD_REQUEST);
		} catch (Exception e) {
			log.error(e.getMessage() + "Exception");
			saveTemplateHeaderResponsePo
					.setMessage("Please contact System Administrator there is an error while processing the request");
			saveTemplateHeaderResponsePo.setError(e.getMessage());
			return new ResponseEntity<SaveTemplateHeaderResponsePo>(saveTemplateHeaderResponsePo,
					HttpStatus.INTERNAL_SERVER_ERROR);
		}

		return new ResponseEntity<SaveTemplateHeaderResponsePo>(saveTemplateHeaderResponsePo, new HttpHeaders(),
				HttpStatus.OK);

	}*/

	@ApiOperation(value = "This Api is for copy cloudtemplate")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error"),
			@ApiResponse(code = 400, message = "Bad Request") })
	@PostMapping("/copycloudtemplate")
	public ResponseEntity<SaveTemplateHeaderResponsePo> copyCloudTemplate(@RequestBody CopyCloudReqPo copyCloudReqPo,
			HttpServletRequest request) {
		log.info("Start of copyCloudTemplate Method in Controller ###");
		SaveTemplateHeaderResponsePo saveTemplateHeaderResponsePo = new SaveTemplateHeaderResponsePo();

		try {
			// if (sourceTemplateHeadersPo.get("pod_id")!= null &&
			// sourceTemplateHeadersPo.getPod_id() != null &&
			// sourceTemplateHeadersPo.getBu_specific()!=null)*/
			saveTemplateHeaderResponsePo = xxrCloudService.copyCloudTemplate(copyCloudReqPo, request);
			/*
			 * else return new
			 * ResponseEntity("Missing templateId or objectCode in the Request",HttpStatus.
			 * BAD_REQUEST);
			 */

		} catch (Exception e) {
			saveTemplateHeaderResponsePo
					.setMessage("Please contact System Administrator there is an error while processing the request");
			saveTemplateHeaderResponsePo.setError(e.getMessage());
			log.error(e.getMessage());
			return new ResponseEntity<SaveTemplateHeaderResponsePo>(saveTemplateHeaderResponsePo,
					HttpStatus.INTERNAL_SERVER_ERROR);
		}

		return new ResponseEntity<SaveTemplateHeaderResponsePo>(saveTemplateHeaderResponsePo, new HttpHeaders(),
				HttpStatus.OK);

	}

	/*@ApiOperation(value = "This Api is to save cloudtemplatecolumns")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error"),
			@ApiResponse(code = 400, message = "Bad Request") })
	@PostMapping("/savecloudtemplatecolumns")
	public ResponseEntity<SaveCloudTemplateColumnsResPo> saveCloudTemplateColumns(
			@RequestBody List<SaveCloudTemplateColumnsPo> cloudTemplateColumnsPo, HttpServletRequest request) {
		log.info("Start of saveCloudTemplateColumns Method in Controller ###");

		SaveCloudTemplateColumnsResPo saveCloudTemplateColumnsResPo = new SaveCloudTemplateColumnsResPo();
		try {
			saveCloudTemplateColumnsResPo = xxrCloudService.saveCloudTemplateColumns(cloudTemplateColumnsPo, request);

		} catch (BadRequestException e) {
			log.error(e.getMessage());
			saveCloudTemplateColumnsResPo.setMessage(e.getMessage());
			saveCloudTemplateColumnsResPo.setError("Please contact System Administrator there is an error while processing the request");
			return new ResponseEntity<SaveCloudTemplateColumnsResPo>(saveCloudTemplateColumnsResPo,
					HttpStatus.BAD_REQUEST);
		} catch (Exception e) {
			log.error(e.getMessage());
			saveCloudTemplateColumnsResPo
					.setMessage("Please contact System Administrator there is an error while processing the request");
			saveCloudTemplateColumnsResPo.setError(e.getMessage());
			return new ResponseEntity<SaveCloudTemplateColumnsResPo>(saveCloudTemplateColumnsResPo,
					HttpStatus.INTERNAL_SERVER_ERROR);
		}

		return new ResponseEntity<SaveCloudTemplateColumnsResPo>(saveCloudTemplateColumnsResPo, new HttpHeaders(),
				HttpStatus.OK);

	}*/

	@ApiOperation(value = "This Api is to save cloudtemplatecolumns sequence based on ctl file")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error"),
			@ApiResponse(code = 400, message = "Bad Request") })
	@PostMapping("/generatesequence")
	public ResponseEntity<SaveCloudTemplateColumnsResPo> generateSequence(@RequestParam("templateId") Long templateId,
			@RequestParam("objectId") Long objectId, @RequestParam("version") String version,
			HttpServletRequest request) {
		log.info("Start of generateSequence Method in Controller ###");

		SaveCloudTemplateColumnsResPo saveCloudTemplateColumnsResPo = new SaveCloudTemplateColumnsResPo();
		try {
			saveCloudTemplateColumnsResPo = xxrCloudService.generateSequence(templateId, objectId, version, request);

		} catch (BadRequestException e) {
			log.error(e.getMessage());
			saveCloudTemplateColumnsResPo.setMessage(e.getMessage());
			saveCloudTemplateColumnsResPo.setError("Error while generating sequence");
			return new ResponseEntity<SaveCloudTemplateColumnsResPo>(saveCloudTemplateColumnsResPo,
					HttpStatus.BAD_REQUEST);
		} catch (Exception e) {
			log.error(e.getMessage());
			saveCloudTemplateColumnsResPo
					.setMessage("Please contact System Administrator there is an error while processing the request");
			saveCloudTemplateColumnsResPo.setError(e.getMessage());
			return new ResponseEntity<SaveCloudTemplateColumnsResPo>(saveCloudTemplateColumnsResPo,
					HttpStatus.INTERNAL_SERVER_ERROR);
		}

		return new ResponseEntity<SaveCloudTemplateColumnsResPo>(saveCloudTemplateColumnsResPo, new HttpHeaders(),
				HttpStatus.OK);

	}

	@ApiOperation(value = "This Api is to save cloudtemplatecolumns sequence based on xlsm file")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error"),
			@ApiResponse(code = 400, message = "Bad Request") })
	@PostMapping("/xlsmgeneratesequence")
	public ResponseEntity<SaveCloudTemplateColumnsResPo> xlsmGenerateSequence(
			@RequestParam("templateId") Long templateId, @RequestParam("objectId") Long objectId,
			@RequestParam("version") String version, HttpServletRequest request) {
		log.info("Start of xlsmGenerateSequence Method in Controller ###");

		SaveCloudTemplateColumnsResPo saveCloudTemplateColumnsResPo = new SaveCloudTemplateColumnsResPo();
		try {
			saveCloudTemplateColumnsResPo = xxrCloudService.xlsmGenerateSequence(templateId, objectId, version,
					request);

		} catch (BadRequestException e) {
			log.error(e.getMessage());
			saveCloudTemplateColumnsResPo.setMessage(e.getMessage());
			saveCloudTemplateColumnsResPo.setError("Error while generating sequence");
			return new ResponseEntity<SaveCloudTemplateColumnsResPo>(saveCloudTemplateColumnsResPo,
					HttpStatus.BAD_REQUEST);
		} catch (ValidationException e) {
			log.error(e.getMessage());
			saveCloudTemplateColumnsResPo.setMessage(e.getMessage());
			saveCloudTemplateColumnsResPo
					.setError("Please contact System Administrator there is an error while processing the request");
			return new ResponseEntity<SaveCloudTemplateColumnsResPo>(saveCloudTemplateColumnsResPo, HttpStatus.OK);
		} catch (Exception e) {
			log.error(e.getMessage());
			saveCloudTemplateColumnsResPo
					.setMessage("Please contact System Administrator there is an error while processing the request");
			saveCloudTemplateColumnsResPo.setError(e.getMessage());
			return new ResponseEntity<SaveCloudTemplateColumnsResPo>(saveCloudTemplateColumnsResPo,
					HttpStatus.INTERNAL_SERVER_ERROR);
		}

		return new ResponseEntity<SaveCloudTemplateColumnsResPo>(saveCloudTemplateColumnsResPo, new HttpHeaders(),
				HttpStatus.OK);

	}

	@ApiOperation("This Api fetchs cldTemplateId's and sourcetemplateId's based on objectcode or parentObjectCode")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error"),
			@ApiResponse(code = 400, message = "Bad Request") })
	@GetMapping("/getcldsrctemplateids")
	public ResponseEntity<List<CldSrcTemplateIdsResPo>> getCldSrcTemplateIds(
			@RequestParam("objectCode") String objectCode, @RequestParam("parentObjectCode") String parentObjectCode,@RequestHeader("roleid") Long roleId)
			throws ConvertRiteException {
		log.info("Start of getCldSrcTemplateIds Method ###########");
		List<CldSrcTemplateIdsResPo> cldSrcTemplateIdsResPo = new ArrayList<>();
		try {
			cldSrcTemplateIdsResPo = xxrCloudService.getCldSrcTemplateIds(objectCode, parentObjectCode,roleId);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<List<CldSrcTemplateIdsResPo>>(cldSrcTemplateIdsResPo, HttpStatus.OK);
	}
	
	@ApiOperation("This Api fetchs cldTemplateId's and sourcetemplateId's based on objectcode or parentObjectCode")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error"),
			@ApiResponse(code = 400, message = "Bad Request") })
	@GetMapping("/getdefaultcldsrctemplateids")
	public ResponseEntity<List<CldSrcTemplateIdsResPo>> getDefaultCldSrcTemplateIds(
			@RequestParam("objectCode") String objectCode, @RequestParam("parentObjectCode") String parentObjectCode,@RequestHeader("roleid") Long roleId)
			throws ConvertRiteException {
		log.info("Start of getDefaultCldSrcTemplateIds Method ###########");
		List<CldSrcTemplateIdsResPo> cldSrcTemplateIdsResPo = new ArrayList<>();
		try {
			cldSrcTemplateIdsResPo = xxrCloudService.getDefaultCldSrcTemplateIds(objectCode, parentObjectCode,roleId);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<List<CldSrcTemplateIdsResPo>>(cldSrcTemplateIdsResPo, HttpStatus.OK);
	}
	
	@ApiOperation(value = "This Api is to Save CloudTemplate Header with Jpa")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error"),
			@ApiResponse(code = 400, message = "Bad Request") })
	@PostMapping("/savecloudtemplatehdrswithjpa")
	public ResponseEntity<SaveTemplateHeaderResponsePo> saveCloudTemplateHdrsWithJpa(
			@RequestBody SaveCloudTemplateHeadersPo saveCloudTemplateHeadersPo) {
		log.info("Start of saveCloudTemplateHdrsWithJpa Method in Controller ###");
		SaveTemplateHeaderResponsePo saveTemplateHeaderResponsePo = new SaveTemplateHeaderResponsePo();
		try {
			if ( saveCloudTemplateHeadersPo.getProjectId() == null
					|| saveCloudTemplateHeadersPo.getObjectId() == null
					|| saveCloudTemplateHeadersPo.getParentObjectId() == null
					|| saveCloudTemplateHeadersPo.getMetaDataTableId() == null
					|| Validations.isNullOrEmpty(saveCloudTemplateHeadersPo.getTemplateName()))
				throw new BadRequestException(
						"projectId,parentObjectId,objectId,templateName and metaDataTableId are Mandatory fields");
			saveTemplateHeaderResponsePo = xxrCloudService.saveCloudTemplateHdrsWithJpa(saveCloudTemplateHeadersPo);
		} catch (ValidationException e) {
			log.error(e.getMessage() + "ValidationException");
			saveTemplateHeaderResponsePo.setMessage(e.getMessage());
			saveTemplateHeaderResponsePo
					.setError("Please contact System Administrator there is an error while processing the request");
			return new ResponseEntity<SaveTemplateHeaderResponsePo>(saveTemplateHeaderResponsePo, HttpStatus.OK);
		} catch (BadRequestException e) {
			log.error(e.getMessage() + "BadRequestException");
			saveTemplateHeaderResponsePo.setMessage(e.getMessage());
			saveTemplateHeaderResponsePo
					.setError("Please contact System Administrator there is an error while processing the request");
			return new ResponseEntity<SaveTemplateHeaderResponsePo>(saveTemplateHeaderResponsePo,
					HttpStatus.BAD_REQUEST);
		} catch (Exception e) {
			log.error(e.getMessage() + "Exception");
			saveTemplateHeaderResponsePo
					.setMessage("Please contact System Administrator there is an error while processing the request");
			saveTemplateHeaderResponsePo.setError(e.getMessage());
			return new ResponseEntity<SaveTemplateHeaderResponsePo>(saveTemplateHeaderResponsePo,
					HttpStatus.INTERNAL_SERVER_ERROR);
		}

		return new ResponseEntity<SaveTemplateHeaderResponsePo>(saveTemplateHeaderResponsePo, HttpStatus.OK);
	}
	
	@ApiOperation(value = "This Api is to save cloudtemplatecolumns with jpa")

	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),

			@ApiResponse(code = 500, message = "Server Side Error"),

			@ApiResponse(code = 400, message = "Bad Request") })

	@PostMapping("/savecloudtemplatecolumnswithjpa")
	public ResponseEntity<SaveCloudTemplateColumnsResPo> saveCloudTemplateColumnsWithJpa(

			@RequestBody List<SaveCloudTemplateColumnsPo> cloudTemplateColumnsPo) {
		log.info("Start of saveCloudTemplateColumnsWithJpa ######");
		SaveCloudTemplateColumnsResPo saveCloudTemplateColumnsResPo = new SaveCloudTemplateColumnsResPo();
		try {
			saveCloudTemplateColumnsResPo = xxrCloudService.saveCloudTemplateColumnsWithJpa(cloudTemplateColumnsPo);
		}catch (ValidationException e) {
			log.error(e.getMessage());
			saveCloudTemplateColumnsResPo.setMessage(e.getMessage());
			saveCloudTemplateColumnsResPo.setError("Please contact System Administrator there is an error while processing the request");
			return new ResponseEntity<SaveCloudTemplateColumnsResPo>(saveCloudTemplateColumnsResPo,
					HttpStatus.OK);
		}  catch (BadRequestException e) {
			log.error(e.getMessage());
			saveCloudTemplateColumnsResPo.setMessage(e.getMessage());
			saveCloudTemplateColumnsResPo.setError("Please contact System Administrator there is an error while processing the request");
			return new ResponseEntity<SaveCloudTemplateColumnsResPo>(saveCloudTemplateColumnsResPo,
					HttpStatus.BAD_REQUEST);
		} catch (Exception e) {
			log.error(e.getMessage());
			saveCloudTemplateColumnsResPo.setMessage("Error while saving cloud template cloumns");
			saveCloudTemplateColumnsResPo.setError(e.getMessage());
			return new ResponseEntity<SaveCloudTemplateColumnsResPo>(saveCloudTemplateColumnsResPo,
					HttpStatus.INTERNAL_SERVER_ERROR);
		}

		return new ResponseEntity(saveCloudTemplateColumnsResPo, HttpStatus.OK);
	}

}
