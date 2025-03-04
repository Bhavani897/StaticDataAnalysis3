package com.rite.products.convertrite.controller;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Async;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.rite.products.convertrite.Validations.Validations;
import com.rite.products.convertrite.exception.BadRequestException;
import com.rite.products.convertrite.exception.ConvertRiteException;
import com.rite.products.convertrite.exception.ValidationException;
import com.rite.products.convertrite.model.CallBackReqPo;
import com.rite.products.convertrite.model.XxrCloudDataProcess;
import com.rite.products.convertrite.model.XxrProcessJobs;
import com.rite.products.convertrite.model.XxrProcessRequestsView;
import com.rite.products.convertrite.model.XxrSourceLoadFailRecordsView;
import com.rite.products.convertrite.po.DeleteStagingDataReq;
import com.rite.products.convertrite.po.DeleteStagingDataRes;
import com.rite.products.convertrite.po.GetRecordsPostJobExecution;
import com.rite.products.convertrite.po.ProcessJobPo;
import com.rite.products.convertrite.po.ProcessReqByRequestTypeReq;
import com.rite.products.convertrite.po.ProcessSourceDataResPo;
import com.rite.products.convertrite.po.SourceTemplateStatisticsRes;
import com.rite.products.convertrite.po.SyncCloudInterfaceDataReq;
import com.rite.products.convertrite.po.SyncCloudInterfaceDataResPo;
import com.rite.products.convertrite.po.UpdateFailedRecReqPo;
import com.rite.products.convertrite.po.UpdateFailedRecResp;
import com.rite.products.convertrite.po.XxrSourceFailRecordsReqPo;
import com.rite.products.convertrite.po.XxrSourceLoadFailRecordsResPo;
import com.rite.products.convertrite.service.DataService;

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

@RestController
public class DataController {

	private static final Logger log = LoggerFactory.getLogger(DataController.class);

	@Autowired
	DataService dataService;

	@Value("${file.upload-dir}")
	private String path;

	@ApiOperation(value = "This Api is for processing Job, 'type' Param value should be Validation or Conversion or ReProcesses")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@PostMapping("/processjob")
	public ResponseEntity<ProcessJobPo> processJob(@RequestParam("cloudTemplateName") String cloudTemplateName,
			@RequestParam("type") String type, HttpServletRequest request) throws ConvertRiteException {
		log.info("Start of processJob Method in Controller ###");
		log.debug("cloudTemplateName:::::::" + cloudTemplateName + "type:::::::" + type);
		ProcessJobPo processJobPo = new ProcessJobPo();
		if (type.matches("(?i)Validation|Conversion|ReProcesses")) {
			try {
				if (cloudTemplateName != null && !Validations.isNullOrEmpty(type))
					processJobPo = dataService.processJob(cloudTemplateName, type, request);
				else
					throw new ConvertRiteException("Missing cloudTemplateName or type in the Request",
							HttpStatus.BAD_REQUEST);
			} catch (Exception e) {
				log.error(e.getMessage());
				throw new ConvertRiteException(
						"Please contact System Administrator there is an error while processing the request",
						HttpStatus.INTERNAL_SERVER_ERROR);
			}
		} else {
			throw new ConvertRiteException("type Param value should be Validation or Conversion or ReProcesses",
					HttpStatus.BAD_REQUEST);
		}
		return new ResponseEntity<ProcessJobPo>(processJobPo, new HttpHeaders(), HttpStatus.OK);

	}

	@ApiOperation(value = "This Api returns status of process jobs")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/getprocessjobs")
	public ResponseEntity<List<XxrProcessJobs>> getProcessJobs() throws ConvertRiteException {
		log.info("Start of getProcessJobs Method in Controller ###");
		List<XxrProcessJobs> xxrProcessJobs = new ArrayList<>();
		try {
			xxrProcessJobs = dataService.getProcessJobs();

		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<List<XxrProcessJobs>>(xxrProcessJobs, new HttpHeaders(), HttpStatus.OK);

	}

	@ApiOperation(value = "This Api returns status of process requests")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/getprocessrequestsbytype")
	public ResponseEntity<List<XxrProcessRequestsView>> getPrcRequestsByRequestType(
			ProcessReqByRequestTypeReq processReqByRequestTypeReq) throws ConvertRiteException {
		log.info("Start of getProcessRequests Method in Controller ###");
		List<XxrProcessRequestsView> xxrProcessRequests = new ArrayList<>();
		HttpHeaders httpHeaders = new HttpHeaders();
		try {
			if (Validations.isNullOrEmpty(processReqByRequestTypeReq.getBatchName())
					|| Validations.isNullOrEmpty(processReqByRequestTypeReq.getRequestType()))
				throw new BadRequestException("BatchName & RequestType are Mandatory Parameters");
			xxrProcessRequests = dataService.getPrcRequestsByRequestType(processReqByRequestTypeReq, httpHeaders);

		} catch (BadRequestException e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(e.getMessage(), HttpStatus.BAD_REQUEST);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<List<XxrProcessRequestsView>>(xxrProcessRequests, httpHeaders, HttpStatus.OK);

	}

	/*
	 * @ApiOperation(value = "This Api returns status of process requests")
	 * 
	 * @ApiResponses(value = { @ApiResponse(code = 200, message =
	 * "Successful Response"),
	 * 
	 * @ApiResponse(code = 500, message = "Server Side Error") })
	 * 
	 * @GetMapping("/getprocessrequests") public
	 * ResponseEntity<List<ProcessRequestsPo>> getProcessRequests() throws
	 * ConvertRiteException {
	 * log.info("Start of getProcessRequests Method in Controller ###");
	 * List<ProcessRequestsPo> xxrProcessRequests = new ArrayList<>(); try {
	 * xxrProcessRequests = dataService.getProcessRequests();
	 * 
	 * } catch (Exception e) { log.error(e.getMessage()); throw new
	 * ConvertRiteException(
	 * "Please contact System Administrator there is an error while processing the request"
	 * , HttpStatus.INTERNAL_SERVER_ERROR); } return new
	 * ResponseEntity<List<ProcessRequestsPo>>(xxrProcessRequests, new
	 * HttpHeaders(), HttpStatus.OK);
	 * 
	 * }
	 */

	@ApiOperation(value = "This Api exports failed records as csv or json 'type' field value should be CSV/JSON")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/exportfailedrecords")
	public void exportFailedRecords(@RequestParam("sourceTemplateId") Long sourceTemplateId,
			@RequestParam("Id") Long id, HttpServletResponse response, @RequestParam("type") String type)
			throws ConvertRiteException {
		log.info("Start of exportFailedRecords Method in Controller ###");

		try {
			dataService.exportFailedRecords(sourceTemplateId, id, response, type);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}

	}

	@ApiOperation(value = "This Api is to get status of stage data load")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/getstagedataloadstatus")
	public ResponseEntity<XxrSourceLoadFailRecordsResPo> getStageDataLoadStatus(
			@RequestParam("sourceTemplateId") Long sourceTemplateId) throws ConvertRiteException {
		log.info("Start of getStageDataLoadStatus Method in Controller ###");
		// XxrSourceLoadFailRecords xxrSourceLoadFailRecords = new
		// XxrSourceLoadFailRecords();
		XxrSourceLoadFailRecordsResPo xxrSourceLoadFailRecordsResPo = new XxrSourceLoadFailRecordsResPo();
		try {
			xxrSourceLoadFailRecordsResPo = dataService.getStageDataLoadStatus(sourceTemplateId);

		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<XxrSourceLoadFailRecordsResPo>(xxrSourceLoadFailRecordsResPo, new HttpHeaders(),
				HttpStatus.OK);
	}

	/*
	 * @ApiOperation(value = "This Api is to get status of all stage data load")
	 * 
	 * @ApiResponses(value = { @ApiResponse(code = 200, message =
	 * "Successful Response"),
	 * 
	 * @ApiResponse(code = 500, message = "Server Side Error") })
	 * 
	 * @GetMapping("/getallstagedataloadstatus") public
	 * ResponseEntity<List<XxrSourceLoadFailRecordsResPo>>
	 * getAllStageDataLoadStatus(XxrSourceFailRecordsReqPo
	 * xxrSourceFailRecordsReqPo) throws ConvertRiteException {
	 * log.info("Start of getAllStageDataLoadStatus Method in Controller ###");
	 * HttpHeaders httpHeaders=new HttpHeaders(); // XxrSourceLoadFailRecords
	 * xxrSourceLoadFailRecords = new // XxrSourceLoadFailRecords();
	 * List<XxrSourceLoadFailRecordsResPo> sourceLoadFailRecordsResList = new
	 * ArrayList<>(); // XxrSourceLoadFailRecordsResPo
	 * xxrSourceLoadFailRecordsResPo=new // XxrSourceLoadFailRecordsResPo(); try {
	 * sourceLoadFailRecordsResList =
	 * dataService.getAllStageDataLoadStatus(xxrSourceFailRecordsReqPo,httpHeaders);
	 * 
	 * } catch (Exception e) { log.error(e.getMessage()); throw new
	 * ConvertRiteException(
	 * "Please contact System Administrator there is an error while processing the request"
	 * , HttpStatus.INTERNAL_SERVER_ERROR); } return new
	 * ResponseEntity<List<XxrSourceLoadFailRecordsResPo>>(
	 * sourceLoadFailRecordsResList, new HttpHeaders(), HttpStatus.OK); }
	 */

	@ApiOperation(value = "This Api is to get status of all stage data load")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/getallstagedataloadstatus")
	public ResponseEntity<List<XxrSourceLoadFailRecordsView>> getAllStageDataLoadStatus(
			XxrSourceFailRecordsReqPo xxrSourceFailRecordsReqPo) throws ConvertRiteException {
		log.info("Start of getAllStageDataLoadStatus Method in Controller ###");
		HttpHeaders httpHeaders = new HttpHeaders();
		// XxrSourceLoadFailRecords xxrSourceLoadFailRecords = new
		// XxrSourceLoadFailRecords();
		List<XxrSourceLoadFailRecordsView> sourceLoadFailRecordsResList = new ArrayList<>();
		// XxrSourceLoadFailRecordsResPo xxrSourceLoadFailRecordsResPo=new
		// XxrSourceLoadFailRecordsResPo();
		try {
			sourceLoadFailRecordsResList = dataService.getAllStageDataLoadStatus(xxrSourceFailRecordsReqPo,
					httpHeaders);

		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<List<XxrSourceLoadFailRecordsView>>(sourceLoadFailRecordsResList, httpHeaders,
				HttpStatus.OK);
	}

	/*
	 * @ApiOperation(value =
	 * "This Api returns template statistics filterBy Value should be {POD or PROJECT or OBJECT OR PARENT OBJECT OR TEMPLATE}"
	 * )
	 * 
	 * @ApiResponses(value = { @ApiResponse(code = 200, message =
	 * "Successful Response"),
	 * 
	 * @ApiResponse(code = 500, message = "Server Side Error") })
	 * 
	 * @GetMapping("/gettemplatestatistics") public
	 * ResponseEntity<List<TemplateStatisticsResPo>>
	 * getTemplatesStatistics(@RequestParam("filterBy") String filterBy) throws
	 * ConvertRiteException {
	 * log.info("Start of getTemplatesState Method in Controller ###"); List<Object>
	 * templateStatistics = new ArrayList<>(); List<TemplateStatisticsResPo>
	 * templateStatisticsRes = new ArrayList<>(); try {
	 * 
	 * templateStatistics = dataService.getTemplatesStatistics(filterBy);
	 * 
	 * templateStatistics.stream().forEach(x->{ TemplateStatisticsResPo
	 * templateStatisticsResPo =new TemplateStatisticsResPo(); Object[] obj =
	 * (Object[]) x; BigDecimal id = (BigDecimal) obj[0];
	 * templateStatisticsResPo.setId(id.longValue());
	 * templateStatisticsResPo.setValue((String)obj[1]); BigDecimal success =
	 * (BigDecimal) obj[2]; templateStatisticsResPo.setSuccess(success.intValue());
	 * BigDecimal failed = (BigDecimal) obj[3];
	 * templateStatisticsResPo.setFailed(failed.intValue()); BigDecimal unverified =
	 * (BigDecimal) obj[4];
	 * templateStatisticsResPo.setUnverified(unverified.intValue());
	 * 
	 * templateStatisticsRes.add(templateStatisticsResPo);
	 * 
	 * }); } catch (Exception e) { log.error(e.getMessage()); throw new
	 * ConvertRiteException(
	 * "Please contact System Administrator there is an error while processing the request"
	 * , HttpStatus.INTERNAL_SERVER_ERROR); } return new
	 * ResponseEntity<List<TemplateStatisticsResPo>>(templateStatisticsRes, new
	 * HttpHeaders(), HttpStatus.OK);
	 * 
	 * }
	 */

	@ApiOperation(value = "This Api returns source template statistics")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/getsourcetemplatestatistics")
	public ResponseEntity<List<SourceTemplateStatisticsRes>> getSourceTemplatesStatistics()
			throws ConvertRiteException {
		log.info("Start of getSourceTemplatesStatistics Method in Controller ###");

		List<SourceTemplateStatisticsRes> templateStatistic = new ArrayList<>();
		try {

			templateStatistic = dataService.getSourceTemplatesStatistics();

		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<List<SourceTemplateStatisticsRes>>(templateStatistic, new HttpHeaders(),
				HttpStatus.OK);

	}

	/*
	 * @ApiOperation(value = "This Api is for generating fbdi")
	 * 
	 * @ApiResponses(value = { @ApiResponse(code = 200, message =
	 * "Successful Response"),
	 * 
	 * @ApiResponse(code = 500, message = "Server Side Error") })
	 * 
	 * @GetMapping("/generatefbdi") public void
	 * generateFbdi(@RequestParam("cloudTemplateName") String cloudTemplateName,
	 * HttpServletResponse response) throws ConvertRiteException {
	 * log.info("Start of generateFbdi Method in Controller######"); try {
	 * dataService.generateFbdi(cloudTemplateName, response); } catch (Exception e)
	 * { e.printStackTrace(); throw new ConvertRiteException(
	 * "Please contact System Administrator there is an error while processing the request"
	 * , HttpStatus.INTERNAL_SERVER_ERROR); }
	 * 
	 * }
	 */

	@ApiOperation(value = "This Api is for generating fbdi from lob")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/generatefbdi")
	public void generateFbdiFromLob(@RequestParam("cloudTemplateId") Long cloudTemplateId, HttpServletResponse response)
			throws ConvertRiteException {
		log.info("Start of generateFbdiFromLob Method in Controller######");
		try {
			dataService.generateFbdiFromLob(cloudTemplateId, response);
		} catch (Exception e) {
			e.printStackTrace();
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
	}

	@ApiOperation(value = "This Api is for generating fbdi from lob")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/generatefbdiv1")
	public void generateFbdiFromLobV1(@RequestParam("cloudTemplateId") Long cloudTemplateId,
			@RequestParam("batchName") String batchName, HttpServletResponse response) throws ConvertRiteException {
		log.info("Start of generateFbdiFromLob Method in Controller######");
		try {
			dataService.generateFbdiFromLobV1(cloudTemplateId, batchName, response);
		} catch (Exception e) {
			e.printStackTrace();
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}

	}

	@ApiOperation(value = "This Api is for generating fbdi from lob")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/generatexls")
	public void generateXlsFromLob(@RequestParam("cloudTemplateId") Long cloudTemplateId, HttpServletResponse response)
			throws ConvertRiteException {
		log.info("Start of generateXlsFromLob Method in Controller######");
		try {
			dataService.generateXlsFromLob(cloudTemplateId, response);
		} catch (Exception e) {
			e.printStackTrace();
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}

	}

	@ApiOperation(value = "This Api is for generating transformation report from lob")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/gettransformationreport")
	public void getTransformationReport(@RequestParam("cloudTemplateId") Long cloudTemplateId,
			@RequestParam("batchName") String batchName, HttpServletResponse response) throws ConvertRiteException {
		log.info("Start of getTransformationReport Method in Controller######");
		try {
			dataService.getTransformationReport(cloudTemplateId, batchName, response);
		} catch (Exception e) {
			e.printStackTrace();
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}

	}

	@ApiOperation(value = "This Api is for generating hdl from lob")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/generatehdl")
	public void generateHdlFromLob(@RequestParam("cloudTemplateId") String cloudTemplateId,
			@RequestParam("batchName") String batchName,@RequestParam("isIntialLoad") String isIntialLoad, HttpServletResponse response) throws ConvertRiteException {
		log.info("Start of generateHdlFromLob Method in Controller######");
		try {
			dataService.generateHdlFromLob(cloudTemplateId, batchName,isIntialLoad, response);
		} catch (Exception e) {
			e.printStackTrace();
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}

	}

	@ApiOperation(value = "This Api is for getting records post job execution,Status field value should be VF/VS/ALL,'type field value should be CSV/JSON")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@PostMapping("/getrecordspostjobexecution")
	public void getRecordsPostJobExecution(@RequestBody GetRecordsPostJobExecution getRecordsPostJobExecution, HttpServletResponse response) throws ConvertRiteException {
		log.info("Start of getRecordsPostJobExecution Method in Controller######");
		try {
			dataService.getRecordsPostJobExecution(getRecordsPostJobExecution,response);
		} catch (Exception e) {
			log.error(e.getMessage());
			e.printStackTrace();
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}

	}

	@ApiOperation(value = "This Api helps to load sourcestaginging data Records")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error"),
			@ApiResponse(code = 400, message = "Bad Request") })
	@PostMapping("/processsourcestagingdata")
	public ResponseEntity<ProcessSourceDataResPo> processSourceStagingData(@RequestParam("fileName") String fileName,
			@RequestParam("templateName") String templateName, @RequestParam("batchName") String batchName,
			HttpServletRequest request) throws ConvertRiteException {
		log.info("Start of processSourceStagingData Method in Controller ###");
		log.debug("templateName:::::::" + templateName + "fileName:::::::" + fileName);
		ProcessSourceDataResPo processSourceStagingPo = new ProcessSourceDataResPo();
		String fileExtension = FilenameUtils.getExtension(fileName);
		log.info(fileExtension + "::::::fileExtension");
		if (fileExtension.equalsIgnoreCase("csv")) {
			try {
				processSourceStagingPo = dataService.processSourceStagingData(fileName, templateName, batchName,
						request);
			} catch (Exception e) {
				log.error(e.getMessage());
				throw new ConvertRiteException(
						"Please contact System Administrator there is an error while processing the request",
						HttpStatus.INTERNAL_SERVER_ERROR);
			}
		} else
			throw new ConvertRiteException("File Extension should be csv", HttpStatus.BAD_REQUEST);

		return new ResponseEntity<ProcessSourceDataResPo>(processSourceStagingPo, new HttpHeaders(), HttpStatus.OK);
	}

	@ApiOperation(value = "This Api helps to load sourcestaginging data from csv to table as clob")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error"),
			@ApiResponse(code = 400, message = "Bad Request") })
	@PostMapping("/processdataasclob")
	public void processSourceData(@RequestParam("fileName") String fileName,
			@RequestParam("templateName") String templateName) throws ConvertRiteException {
		log.info("Start of processSourceData Method in Controller ###");
		log.debug("templateName:::::::" + templateName + "fileName:::::::" + fileName + "path" + path);

		long startTime = System.currentTimeMillis();
		try {
			if (!(Validations.isNullOrEmpty(templateName))
					&& !(Validations.isNullOrEmpty(fileName) && !(Validations.isNullOrEmpty(path)))) {
				dataService.processSourceData(fileName, templateName, path);
				long diff = System.currentTimeMillis() - startTime;
				log.info(diff + "diff");
			} else
				throw new ConvertRiteException("Missing templateName or fileName in the Request",
						HttpStatus.BAD_REQUEST);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}

		// return new ResponseEntity<ProcessJobPo>(processSourceStagingPo, new
		// HttpHeaders(), HttpStatus.OK);
	}

	@ApiOperation(value = "This Api updates edited columns of failure records to source staging table ")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error"),
			@ApiResponse(code = 400, message = "Bad Request") })
	@PostMapping("/updateeditedfailedrec")
	public ResponseEntity<UpdateFailedRecResp> updateEditedFailedRecord(
			@RequestBody UpdateFailedRecReqPo updateFailedRecReqPo) throws ConvertRiteException {
		log.info("Start of updateEditedFailedRecords Method in Controller ###");
		UpdateFailedRecResp updateFailedRecResp = new UpdateFailedRecResp();
		try {
			updateFailedRecResp = dataService.updateEditedFailedRecords(updateFailedRecReqPo);
		} catch (Exception e) {
			updateFailedRecResp.setMessage("Something went wrong while updating failed edited records");
			updateFailedRecResp.setError(e.getMessage());
			return new ResponseEntity<UpdateFailedRecResp>(updateFailedRecResp, new HttpHeaders(),
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<UpdateFailedRecResp>(updateFailedRecResp, new HttpHeaders(), HttpStatus.OK);
	}

	@Async
	@PostMapping("/synccloudinterfacedata")
	public ResponseEntity<CompletableFuture<SyncCloudInterfaceDataResPo>> syncCloudInterfaceTbleData(
			@RequestBody SyncCloudInterfaceDataReq syncCloudInterfaceDataReq, HttpServletRequest request) {
		log.info("Start of syncCloudInterfaceTbleData #########");
		SyncCloudInterfaceDataResPo syncCloudInterfaceDataResPo = new SyncCloudInterfaceDataResPo();
		CompletableFuture<SyncCloudInterfaceDataResPo> completedFuture = new CompletableFuture<SyncCloudInterfaceDataResPo>();
		try {
			syncCloudInterfaceDataResPo = dataService.syncCloudInterfaceTbleData(syncCloudInterfaceDataReq, request);

			completedFuture = CompletableFuture.completedFuture(syncCloudInterfaceDataResPo);
		} catch (Exception e) {
			e.printStackTrace();
			log.error(e.getMessage());
			syncCloudInterfaceDataResPo
					.setMessage("Please contact System Administrator there is an error while processing the request");
			syncCloudInterfaceDataResPo.setError(e.getMessage());
			completedFuture = CompletableFuture.completedFuture(syncCloudInterfaceDataResPo);
			return new ResponseEntity<CompletableFuture<SyncCloudInterfaceDataResPo>>(completedFuture,
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<CompletableFuture<SyncCloudInterfaceDataResPo>>(completedFuture, HttpStatus.OK);
	}

	@ApiOperation(value = "This Api saves select query to fetch origtransref in cloudIntegrator process table ")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error"),
			@ApiResponse(code = 400, message = "Bad Request") })
	@PostMapping("/processreconcile")
	public ResponseEntity<List<XxrCloudDataProcess>> processReconcile(
			@RequestParam("cloudTemplateId") Long cloudTemplateId, HttpServletRequest request)
			throws ConvertRiteException {
		log.info("Start of processReconcile Method in Controller ###");
		log.debug("cloudTemplateId:::::::" + cloudTemplateId);
		List<XxrCloudDataProcess> xxrCloudDataProcessLi = new ArrayList<>();
		try {
			xxrCloudDataProcessLi = dataService.processReconcile(cloudTemplateId, request);
			// log.info("Request submitted to process reconcile for CloudTemplateId:::::::"
			// + cloudTemplateId);
		} catch (Exception e) {
			e.printStackTrace();
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		log.info("Returning Request submitted to process reconcile for CloudTemplateId:::::::" + cloudTemplateId);
		return new ResponseEntity<List<XxrCloudDataProcess>>(xxrCloudDataProcessLi, new HttpHeaders(), HttpStatus.OK);
	}

	@ApiOperation(value = "This api get reconcilation report")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error"),
			@ApiResponse(code = 400, message = "Bad Request") })
	@GetMapping("/processreconcilereport")
	public void processReconcileReport(@RequestParam("cloudTemplateId") Long cloudTemplateId,
			@RequestParam("requestId") List<String> requestId, HttpServletResponse response)
			throws ConvertRiteException {
		log.info("Start of processReconcileReport Method in Controller ###");
		log.debug("cloudTemplateId:::::::" + cloudTemplateId);
		try {
			dataService.processReconcileReport(cloudTemplateId, requestId, response);
		} catch (ValidationException e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(e.getMessage(), HttpStatus.OK);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
	}

	@ApiOperation(value = "This Api is for callback")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error"),
			@ApiResponse(code = 400, message = "Bad Request") })
	@PostMapping("/callback")
	public void onFallbackmethod(@RequestBody CallBackReqPo request) {
		log.info("Start of onFallbackmethod##########::::::::");
		log.debug("request##########::::::::" + request);
		try {
			dataService.onFallbackmethod(request);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@ApiOperation(value = "This Api is for callback11")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error"),
			@ApiResponse(code = 400, message = "Bad Request") })
	@PostMapping(value = "/fallback")
	public void onFallbackmethod(@RequestBody Object request) {
		log.info("Start of onFallbackmethod11111##########::::::::");
		log.info("request##########::::::::" + request);

	}

	@ApiOperation(value = "This api is for deleting data from  cloud & source staging tables")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error"),
			@ApiResponse(code = 400, message = "Bad Request") })
	@PostMapping("/deletestagingdata")
	public ResponseEntity<DeleteStagingDataRes> deleteStagingData(
			@RequestBody DeleteStagingDataReq deleteStagingDataReq) throws ConvertRiteException {
		log.info("Start of deleteStagingData Method in Controller ###");
		DeleteStagingDataRes deleteStagingDataRes = new DeleteStagingDataRes();
		try {
			deleteStagingDataRes = dataService.deleteStagingData(deleteStagingDataReq);
		} catch (ValidationException e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(e.getMessage(), HttpStatus.OK);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<DeleteStagingDataRes>(deleteStagingDataRes, HttpStatus.OK);
	}

	@ApiOperation(value = "This api is for fetching distinct batch names")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error"),
			@ApiResponse(code = 400, message = "Bad Request") })
	@GetMapping("/getbatchnames")
	public ResponseEntity<List<String>> getBatchNames(@RequestParam("cldTemplateId") Long templateId) throws Exception {
		log.info("Start of getBatchNames in controller #####");
		List<String> batchNamesLi = new ArrayList<>();
		try {
			batchNamesLi = dataService.getBatchNames(templateId);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<List<String>>(batchNamesLi, HttpStatus.OK);
	}

}
