package com.rite.products.convertrite.service;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.RichTextString;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import com.jcraft.jsch.ChannelSftp;
import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.rite.products.convertrite.Validations.Validations;
import com.rite.products.convertrite.exception.ValidationException;
import com.rite.products.convertrite.model.CallBackReqPo;
import com.rite.products.convertrite.model.SourceTemplateHeaders;
import com.rite.products.convertrite.model.XxrBlobConvertrite;
import com.rite.products.convertrite.model.XxrCloudConfig;
import com.rite.products.convertrite.model.XxrCloudDataProcess;
import com.rite.products.convertrite.model.XxrCloudTemplateColumns;
import com.rite.products.convertrite.model.XxrCloudTemplateHeader;
import com.rite.products.convertrite.model.XxrErpIntegration;
import com.rite.products.convertrite.model.XxrFileDataLoad;
import com.rite.products.convertrite.model.XxrProcessJobs;
import com.rite.products.convertrite.model.XxrProcessRequestsView;
import com.rite.products.convertrite.model.XxrSourceLoadFailRecords;
import com.rite.products.convertrite.model.XxrSourceLoadFailRecordsView;
import com.rite.products.convertrite.model.XxrTemplateRelation;
import com.rite.products.convertrite.po.DeleteStagingDataReq;
import com.rite.products.convertrite.po.DeleteStagingDataRes;
import com.rite.products.convertrite.po.GetRecordsPostJobExecution;
import com.rite.products.convertrite.po.MetaDataColumnsPo;
import com.rite.products.convertrite.po.ProcessJobPo;
import com.rite.products.convertrite.po.ProcessReqByRequestTypeReq;
import com.rite.products.convertrite.po.ProcessSourceDataResPo;
import com.rite.products.convertrite.po.RecordsPostJobExcecutionPo;
import com.rite.products.convertrite.po.SourceTemplateStatisticsRes;
import com.rite.products.convertrite.po.SourceTemplateStatisticsResPo;
import com.rite.products.convertrite.po.SyncCloudInterfaceDataReq;
import com.rite.products.convertrite.po.SyncCloudInterfaceDataResPo;
import com.rite.products.convertrite.po.UpdateFailedRecReqPo;
import com.rite.products.convertrite.po.UpdateFailedRecResp;
import com.rite.products.convertrite.po.XxrSourceFailRecordsReqPo;
import com.rite.products.convertrite.po.XxrSourceLoadFailRecordsResPo;
import com.rite.products.convertrite.respository.CloudTemplateHeaderDaoImpl;
import com.rite.products.convertrite.respository.CreateCloudStagTableDaoImpl;
import com.rite.products.convertrite.respository.ProcessJobDaoImpl;
import com.rite.products.convertrite.respository.ProcessSourceStagingDataDaoImpl;
import com.rite.products.convertrite.respository.SourceTemplateHeadersDaoImpl;
import com.rite.products.convertrite.respository.SourceTemplateHeadersRepository;
import com.rite.products.convertrite.respository.XxrBlobConvertriteRepository;
import com.rite.products.convertrite.respository.XxrCldTempHdrsViewRepository;
import com.rite.products.convertrite.respository.XxrCloudConfigRepository;
import com.rite.products.convertrite.respository.XxrCloudDataProcessRepository;
import com.rite.products.convertrite.respository.XxrCloudTableRepository;
import com.rite.products.convertrite.respository.XxrCloudTemplateColumnsRepository;
import com.rite.products.convertrite.respository.XxrCloudTemplateHeadersRepository;
import com.rite.products.convertrite.respository.XxrErpIntegrationRepository;
import com.rite.products.convertrite.respository.XxrFileDataLoadRepository;
import com.rite.products.convertrite.respository.XxrLookUpValuesRepository;
import com.rite.products.convertrite.respository.XxrObjectCodeGroupingLinesRepository;
import com.rite.products.convertrite.respository.XxrProcessJobsRepository;
import com.rite.products.convertrite.respository.XxrProcessRequestsRepository;
import com.rite.products.convertrite.respository.XxrProcessRequestsViewRepository;
import com.rite.products.convertrite.respository.XxrSourceLoadFailRecordsRepository;
import com.rite.products.convertrite.respository.XxrSourceLoadFailRecordsViewRepository;
import com.rite.products.convertrite.respository.XxrSrcTempHdrsViewRepository;
import com.rite.products.convertrite.respository.XxrTemplateRelationRepository;
import com.rite.products.convertrite.respository.XxrTemplateStateRepository;
import com.rite.products.convertrite.respository.XxrTemplateStatisticsRepository;
import com.rite.products.convertrite.respository.XxrTemplateStatisticsViewRepository;
import com.rite.products.convertrite.utils.DataSourceUtil;
import com.rite.products.convertrite.utils.Utils;

@Service
public class DataServiceImpl implements DataService {
	private static final Logger log = LoggerFactory.getLogger(DataServiceImpl.class);

	/*
	 * @Value("${datasource.hostname}") private String datasourceHostName;
	 * 
	 * @Value("${datasource.port}") private int datasourcePort;
	 * 
	 * @Value("${spring.datasource.username}") private String datasourceUserName;
	 * 
	 * @Value("${spring.datasource.password}") private String datasourcePassword;
	 * 
	 * @Value("${datasource.name}") private String dataSourceName;
	 * 
	 * @Value("${file.upload-dir}") private String remoteDir;
	 */

	@Autowired
	XxrBlobConvertriteRepository xxBlobConvertriteRepository;
	@Autowired
	XxrCloudTemplateHeadersRepository xxrCloudTemplateHeadersRepository;
	@Autowired
	XxrCloudTemplateColumnsRepository xxrCloudTemplateColumnsRepository;
	@Autowired
	CloudTemplateHeaderDaoImpl cloudTemplateHeaderDaoImpl;
	@Autowired
	ProcessJobDaoImpl processJobDaoImpl;
	@Autowired
	XxrProcessJobsRepository xxrProcessJobsRepository;
	@Autowired
	XxrProcessRequestsRepository xxrProcessRequestsRepository;
	@Autowired
	XxrTemplateStateRepository xxrTemplateStateRepository;
	@Autowired
	SourceTemplateHeadersDaoImpl sourceTemplateHeadersDaoImpl;
	@Autowired
	ProcessSourceStagingDataDaoImpl processSourceStagingDataDaoImpl;
	@Autowired
	XxrLookUpValuesRepository xxrLookUpValuesRepository;
	@Autowired
	XxrTemplateStatisticsRepository xxrTemplateStatisticsRepository;
	@Autowired
	XxrFileDataLoadRepository xxrFileDataLoadRepository;
	@Autowired
	XxrSourceLoadFailRecordsRepository xxrSourceLoadFailRecordsRepository;
	@Autowired
	CreateCloudStagTableDaoImpl createCloudStagTableDaoImpl;
	@Autowired
	XxrCloudTableRepository xxrCloudTableRepository;
	@Autowired
	SourceTemplateHeadersRepository sourceTemplateHeadersRepository;
	@Autowired
	XxrCloudDataProcessRepository xxrCloudDataProcessRepository;
	@Autowired
	Utils utils;
	@Autowired
	DataSourceUtil dataSourceUtil;
	@Autowired
	RestTemplate restTemplate;
	@Autowired
	XxrCloudConfigRepository xxrCloudConfigRepository;
	@Autowired
	XxrObjectCodeGroupingLinesRepository xxrObjectCodeGroupingLinesRepository;
	@Autowired
	XxrTemplateRelationRepository xxrTemplateRelationRepository;
	@Autowired
	XxrErpIntegrationRepository xxrErpIntegrationRepository;
	@Autowired
	XxrSourceLoadFailRecordsViewRepository xxrSourceLoadFailRecordsViewRepository;
	@Autowired
	XxrTemplateStatisticsViewRepository xxrTemplateStatisticsViewRepository;
	@Autowired
	XxrProcessRequestsViewRepository xxrProcessRequestsViewRepository;
	@Autowired
	XxrCldTempHdrsViewRepository xxrCldTempHdrsViewRepository;
	@Autowired
	XxrSrcTempHdrsViewRepository xxrSrcTempHdrsViewRepository;

	@Value("${sftp.client.host}")
	private String remoteHost;

	@Value("${sftp.client.username}")
	private String username;

	/*
	 * @Value("${sftp.client.password}") private String password;
	 */

	@Value("${clouddataprocess-url}")
	private String url;
//	@Value("${syncsaasdata-url}")
//	private String syncSaasDataUrl;

	@Override
	public String generateFbdi(String cloudTemplateName, HttpServletResponse response) throws Exception {
		log.info("Start Of Method generateFbdi ####");
		String tableName = "";
		try {
			tableName = xxrCloudTemplateHeadersRepository.getTableName(cloudTemplateName);
			response.setContentType("text/csv");
			if (tableName != null) {
				response.setHeader(HttpHeaders.CONTENT_DISPOSITION,
						"attachment; filename=\"" + tableName + ".csv" + "\"");
				cloudTemplateHeaderDaoImpl.generateFbdi(tableName, response.getWriter());
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e.getMessage());
		}

		return tableName;

	}

	@Override
	public ProcessJobPo processJob(String cloudTemplateName, String type, HttpServletRequest request) throws Exception {
		log.info("Start Of Method processJob ####");
		ProcessJobPo processJobPo = new ProcessJobPo();
		try {
			processJobPo = processJobDaoImpl.processJob(cloudTemplateName, type, request);
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return processJobPo;
	}

	@Override
	public List<XxrProcessJobs> getProcessJobs() throws Exception {
		log.info("Start of getProcessJobs Method in Service ###");
		List<XxrProcessJobs> xxrProcessJobs = new ArrayList<>();
		try {
			xxrProcessJobs = xxrProcessJobsRepository.findAll();
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return xxrProcessJobs;
	}

	/*
	 * @Override public List<ProcessRequestsPo> getProcessRequests() throws
	 * Exception { log.info("Start of getProcessRequests Method in Service ###");
	 * List<ProcessRequestsPo> xxrProcessRequests = new ArrayList<>(); try {
	 * xxrProcessRequests = xxrProcessRequestsRepository.getProcessRequests();
	 * xxrProcessRequests.stream().forEach(x -> { XxrProcessRequests
	 * xxrProcessRequest = x.getXxrProcessRequests(); String parentObjectCode = "";
	 * int y = 0; if (xxrProcessRequest.getParentObjectId() != null)
	 * parentObjectCode =
	 * xxrLookUpValuesRepository.getValueById(xxrProcessRequest.getParentObjectId())
	 * ;
	 * 
	 * Integer successRecords = x.getSuccessRecords(); Integer totalRecords =
	 * xxrProcessRequest.getTotalRecords(); if (successRecords != null &&
	 * totalRecords != null && totalRecords != 0) { y = Math.round((((float)
	 * successRecords / (float) totalRecords) * 100)); }
	 * xxrProcessRequest.setPercentage(y);
	 * x.setXxrProcessRequests(xxrProcessRequest); if
	 * (!Validations.isNullOrEmpty(parentObjectCode))
	 * x.setParentObjectCode(parentObjectCode); }); } catch (Exception e) { throw
	 * new Exception(e.getMessage()); } return xxrProcessRequests; }
	 */

	@Override
	public void getRecordsPostJobExecution(GetRecordsPostJobExecution getRecordsPostJobExecution,
			HttpServletResponse response) throws Exception {
		log.info("Start Of  Method getRecordsPostJobExecution ####");
		try {
			RecordsPostJobExcecutionPo recordsPostJobExce = xxrCloudTemplateHeadersRepository
					.getRecordsPostJobExecution(getRecordsPostJobExecution.getCloudTemplateName());
			if (recordsPostJobExce.getSourceTableName() != null)
				sourceTemplateHeadersDaoImpl.getRecordsPostJobExecution(getRecordsPostJobExecution,
						recordsPostJobExce.getSourceTableName(), response);

		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e.getMessage());
		}

	}

	@Override
	public ProcessSourceDataResPo processSourceStagingData(String fileName, String templateName, String batchName,
			HttpServletRequest request) throws Exception {
		log.info("Start of processSourceStagingData Method in Service ###");
		ProcessSourceDataResPo processSourceStagingPo = new ProcessSourceDataResPo();
		try {
			processSourceStagingPo = processSourceStagingDataDaoImpl.processSourceStagingData(fileName, templateName,
					batchName, request);
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return processSourceStagingPo;
	}

	@Override
	public void processSourceData(String fileName, String templateName, String path) throws Exception {
		log.info("Start of processSourceData Method in Service ###");
		XxrFileDataLoad xxrFileDataLoad = new XxrFileDataLoad();
		String line = "";
		StringBuffer sb = new StringBuffer();
		try {

			ChannelSftp channelSftp = utils.setupJsch();
			channelSftp.connect();
			channelSftp.cd(path);
			InputStream in = channelSftp.get(fileName);

			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			// lines= br.lines().toString();

			while ((line = br.readLine()) != null) // returns a Boolean value
			{
				sb.append(line);
				sb.append("\n");
			}
//			log.info("lines ::::::" + sb);

			Clob myClob = new javax.sql.rowset.serial.SerialClob(sb.toString().toCharArray());

//			log.info(myClob + ":::::::clob");

			// processSourceStagingDataDaoImpl.processSourceData(myClob, templateName);

			xxrFileDataLoad.setCreationDate(Calendar.getInstance().getTime());
			xxrFileDataLoad.setDataLob(sb.toString());
			xxrFileDataLoad.setFileName(fileName);
			xxrFileDataLoad.setTemplateName(templateName);

			xxrFileDataLoadRepository.save(xxrFileDataLoad);

		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}

	}

	/*
	 * private ChannelSftp setupJsch() throws JSchException { JSch jsch = new
	 * JSch(); Session jschSession = jsch.getSession(username, remoteHost);
	 * jschSession.setPassword(password); // jschSession.setPort(port);
	 * java.util.Properties config = new java.util.Properties();
	 * config.put("StrictHostKeyChecking", "no"); jschSession.setConfig(config);
	 * jschSession.setTimeout(6000); jschSession.connect(); return (ChannelSftp)
	 * jschSession.openChannel("sftp"); }
	 */

	@Override
	public void exportFailedRecords(Long sourceTemplateId, Long id, HttpServletResponse response, String type)
			throws Exception {
		log.info("Start of exportFailedRecordsAsCsv Method in Service ###");
		XxrSourceLoadFailRecords xxrSourceLoadFailRecords = new XxrSourceLoadFailRecords();
		// Path target = null;
		PrintWriter writer = response.getWriter();

		/*
		 * Connection con=null; ResultSet rs=null;
		 */

		try {
			xxrSourceLoadFailRecords = xxrSourceLoadFailRecordsRepository.findBytemplateIdAndId(sourceTemplateId, id);
			String failedClob = xxrSourceLoadFailRecords.getFailedClob();
			String csvFileName = xxrSourceLoadFailRecords.getFileName();

			/*
			 * String
			 * sql="select file_name,failed_clob from xxr_source_load_fail_records where template_id="
			 * +sourceTemplateId+"and Id="+id+""; con = dataSourceUtil.createConnection();
			 * PreparedStatement stmt = con.prepareStatement(sql); // step4 execute query
			 * rs= stmt.executeQuery(); rs.next(); String
			 * csvFileName=rs.getString("file_name"); String
			 * failedClob=Utils.clobToString(rs.getClob("failed_clob"));
			 */

			log.info(failedClob + " :::::::failedClob" + "csvFileName:::" + csvFileName);
			if ("CSV".equalsIgnoreCase(type)) {
				response.setHeader(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=" + csvFileName);
				CSVWriter csvWriter = new CSVWriter(writer, CSVWriter.DEFAULT_SEPARATOR, CSVWriter.NO_QUOTE_CHARACTER,
						CSVWriter.NO_ESCAPE_CHARACTER, CSVWriter.DEFAULT_LINE_END);
				if (!Validations.isNullOrEmpty(failedClob))
					csvWriter.writeNext(failedClob.split(","));
				csvWriter.flush();
				csvWriter.close();
			} else if ("JSON".equalsIgnoreCase(type)) {
				response.setContentType("application/json");
				response.setCharacterEncoding("UTF-8");
				JSONArray jsonArr = createJsonResponse(csvFileName, failedClob);
				writer.print(jsonArr);
				writer.flush();
			}

		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e.getMessage());
		} /*
			 * finally { if (con != null) con.close(); }
			 */

	}

	@Override
	public XxrSourceLoadFailRecordsResPo getStageDataLoadStatus(Long sourceTemplateId) throws Exception {
		log.info("Start of getStageDataLoadStatus Method in Service ###");
		XxrSourceLoadFailRecords xxrSourceLoadFailRecords = new XxrSourceLoadFailRecords();
		XxrSourceLoadFailRecordsResPo xxrSourceLoadFailRecordsResPo = new XxrSourceLoadFailRecordsResPo();
		List<SourceTemplateHeaders> sourceTemplateHdrList = new ArrayList<>();
		String parentObjectCode = "";
		SourceTemplateHeaders sourceTemplateHeader = new SourceTemplateHeaders();
		try {
			xxrSourceLoadFailRecords = xxrSourceLoadFailRecordsRepository.findBytemplateId(sourceTemplateId);
			sourceTemplateHdrList = sourceTemplateHeadersRepository
					.findByTemplateId(xxrSourceLoadFailRecords.getTemplateId());
			sourceTemplateHeader = sourceTemplateHdrList.get(0);
			if (sourceTemplateHeader != null) {
				parentObjectCode = xxrLookUpValuesRepository.getValueById(sourceTemplateHeader.getParentObjectId());
				xxrSourceLoadFailRecordsResPo.setParentObjectCode(parentObjectCode);
				xxrSourceLoadFailRecordsResPo.setTemplateName(sourceTemplateHeader.getTemplateName());
			}
			xxrSourceLoadFailRecordsResPo.setTemplateId(xxrSourceLoadFailRecords.getTemplateId());
			xxrSourceLoadFailRecordsResPo.setTotalFailedRec(xxrSourceLoadFailRecords.getFailed());
			xxrSourceLoadFailRecordsResPo.setTotalSuccessRec(xxrSourceLoadFailRecords.getSuccess());
			xxrSourceLoadFailRecordsResPo
					.setTotalRecords(xxrSourceLoadFailRecords.getFailed() + xxrSourceLoadFailRecords.getSuccess());
			xxrSourceLoadFailRecordsResPo.setFileName(xxrSourceLoadFailRecords.getFileName());
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return xxrSourceLoadFailRecordsResPo;
	}

	@Override
	public void generateFbdiFromLob(Long cloudTemplateId, HttpServletResponse response) throws Exception {
		log.info("Start Of generateFbdiFromLob in service###");
		// List<XxrCloudTemplateHeader> cloudTemplateHeaderList = new ArrayList<>();
		Long metaDataTableId = null;
		String metaDataTableName = "";
		try {
			response.setContentType("text/csv");
			XxrCloudTemplateHeader xxrCloudTemplateHeader = xxrCloudTemplateHeadersRepository
					.getCloudTemplateById(cloudTemplateId);

			// XxrCloudTemplateHeader xxrCloudTemplateHeader =
			// cloudTemplateHeaderList.get(0);
			if (xxrCloudTemplateHeader != null)
				metaDataTableId = xxrCloudTemplateHeader.getMetaDataTableId();
			if (metaDataTableId != null) {
				metaDataTableName = xxrCloudTableRepository.getMetaDataTableName(metaDataTableId);
			}
			response.setHeader(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=" + metaDataTableName + ".csv");
			response.setHeader("cldfbdifilename", metaDataTableName + ".csv");
			cloudTemplateHeaderDaoImpl.generateFbdiFromLob(cloudTemplateId, response.getWriter());
			// cloudTemplateHeaderDaoImpl.generateFailedRec(cloudTemplateId,
			// response.getWriter());
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}

	}

	@Override
	public List<XxrSourceLoadFailRecordsView> getAllStageDataLoadStatus(
			XxrSourceFailRecordsReqPo xxrSourceFailRecordsReqPo, HttpHeaders httpHeaders) throws Exception {
		log.info("Start of getAllStageDataLoadStatus Method in Service ###");
		// XxrSourceLoadFailRecords xxrSourceLoadFailRecords = new
		// XxrSourceLoadFailRecords();
		List<XxrSourceLoadFailRecordsView> xxrSourceLoadFailRecordsList = new ArrayList<>();
		// List<XxrSourceLoadFailRecordsResPo> xxrSourceLoadFailRecordsResList = new
		// ArrayList<>();

		try {
			Pageable pageable = PageRequest.of(xxrSourceFailRecordsReqPo.getPageNo(),
					xxrSourceFailRecordsReqPo.getPageSize(),
					Sort.by(xxrSourceFailRecordsReqPo.getSortDirection(), xxrSourceFailRecordsReqPo.getSortBy()));
			Page<XxrSourceLoadFailRecordsView> page = xxrSourceLoadFailRecordsViewRepository.findAll(pageable);

			httpHeaders.set("pagecount", String.valueOf(page.getTotalPages()));
			httpHeaders.set("totalcount", String.valueOf(page.getTotalElements()));

			if (page.hasContent())
				xxrSourceLoadFailRecordsList = page.getContent();

			/*
			 * xxrSourceLoadFailRecordsList.stream().forEach(x -> { if (x != null) {
			 * XxrSourceLoadFailRecordsResPo xxrSourceLoadFailRecordsResPo = new
			 * XxrSourceLoadFailRecordsResPo(); List<SourceTemplateHeaders>
			 * sourceTemplateHdrList = new ArrayList<>(); String parentObjectCode = "";
			 * SourceTemplateHeaders sourceTemplateHeader = new SourceTemplateHeaders();
			 * System.out.println("x.getTemplateId():::::" + x.getTemplateId());
			 * sourceTemplateHdrList =
			 * sourceTemplateHeadersRepository.findByTemplateId(x.getTemplateId());
			 * 
			 * if (sourceTemplateHdrList != null && !sourceTemplateHdrList.isEmpty())
			 * sourceTemplateHeader = sourceTemplateHdrList.get(0); if (sourceTemplateHeader
			 * != null) { parentObjectCode = xxrLookUpValuesRepository
			 * .getValueById(sourceTemplateHeader.getParentObjectId());
			 * xxrSourceLoadFailRecordsResPo.setParentObjectCode(parentObjectCode);
			 * xxrSourceLoadFailRecordsResPo.setTemplateName(sourceTemplateHeader.
			 * getTemplateName()); }
			 * xxrSourceLoadFailRecordsResPo.setTemplateId(x.getTemplateId());
			 * xxrSourceLoadFailRecordsResPo.setTotalFailedRec(x.getFailed()); //
			 * System.out.println(x.getSuccess());
			 * xxrSourceLoadFailRecordsResPo.setTotalSuccessRec(x.getSuccess());
			 * xxrSourceLoadFailRecordsResPo.setTotalRecords(x.getFailed() +
			 * x.getSuccess()); xxrSourceLoadFailRecordsResPo.setFileName(x.getFileName());
			 * xxrSourceLoadFailRecordsResPo.setCreatedBy(x.getCreatedBy());
			 * xxrSourceLoadFailRecordsResPo.setCreationDate(x.getCreationDate());
			 * xxrSourceLoadFailRecordsResPo.setId(x.getId());
			 * 
			 * xxrSourceLoadFailRecordsResList.add(xxrSourceLoadFailRecordsResPo); } });
			 */

		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e.getMessage());
		}
		return xxrSourceLoadFailRecordsList;
	}

	@Override
	public List<SourceTemplateStatisticsRes> getSourceTemplatesStatistics() throws Exception {
		log.info("Start of getSourceTemplatesStatistics Method in Service ###");
		List<Object> templateStatistics = new ArrayList<>();
		List<SourceTemplateStatisticsResPo> templateStatisticsRes = new ArrayList<>();
		List<SourceTemplateStatisticsRes> templateStatistic = new ArrayList<>();
		try {
			templateStatistics = processSourceStagingDataDaoImpl.getSourceTemplateStatistics();
			templateStatistics.stream().forEach(x -> {
				SourceTemplateStatisticsResPo templateStatisticsResPo = new SourceTemplateStatisticsResPo();
				Object[] obj = (Object[]) x;
				BigDecimal id = (BigDecimal) obj[0];
				templateStatisticsResPo.setCriteriaId(id.longValue());
				templateStatisticsResPo.setCriteriaName((String) obj[1]);
				templateStatisticsResPo.setCriteriaType((String) obj[2]);
				BigDecimal success = (BigDecimal) obj[3];
				templateStatisticsResPo.setSuccess(success.intValue());
				BigDecimal failed = (BigDecimal) obj[4];
				templateStatisticsResPo.setFailed(failed.intValue());

				templateStatisticsRes.add(templateStatisticsResPo);

			});

			Map<String, List<SourceTemplateStatisticsResPo>> dataByTemplate = templateStatisticsRes.stream()
					.collect(Collectors.groupingBy(SourceTemplateStatisticsResPo::getCriteriaType));

			SourceTemplateStatisticsRes templateRes = new SourceTemplateStatisticsRes();
			templateRes.setCriteriaType("TEMPLATE_NAME");
			templateRes.setData(dataByTemplate.get("TEMPLATE_NAME"));
			SourceTemplateStatisticsRes podRes = new SourceTemplateStatisticsRes();
			podRes.setData(dataByTemplate.get("POD"));
			podRes.setCriteriaType("POD");
			SourceTemplateStatisticsRes projectRes = new SourceTemplateStatisticsRes();
			projectRes.setData(dataByTemplate.get("PROJECT_ID"));
			projectRes.setCriteriaType("PROJECT_ID");
			SourceTemplateStatisticsRes objectRes = new SourceTemplateStatisticsRes();
			objectRes.setData(dataByTemplate.get("OBJECT_CODE"));
			objectRes.setCriteriaType("OBJECT_CODE");
			SourceTemplateStatisticsRes parentObjectRes = new SourceTemplateStatisticsRes();
			parentObjectRes.setData(dataByTemplate.get("PARENT_OBJECT_CODE"));
			parentObjectRes.setCriteriaType("PARENT_OBJECT_CODE");

			templateStatistic.add(templateRes);
			templateStatistic.add(podRes);
			templateStatistic.add(projectRes);
			templateStatistic.add(objectRes);
			templateStatistic.add(parentObjectRes);
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return templateStatistic;
	}

	private JSONArray createJsonResponse(String csvFileName, String failedClob) throws Exception {
		Path target = null;
		JSONArray jsonArr = new JSONArray();
		try {
			target = Files.createTempDirectory("");
			log.info("target:::::" + target);
			File file = new File(target + File.separator + csvFileName);
			FileWriter outputfile = new FileWriter(file);
			// create CSVWriter with ',' as separator
			CSVWriter writerObj = new CSVWriter(outputfile, CSVWriter.DEFAULT_SEPARATOR, CSVWriter.NO_QUOTE_CHARACTER,
					CSVWriter.NO_ESCAPE_CHARACTER, CSVWriter.DEFAULT_LINE_END);
			if (!Validations.isNullOrEmpty(failedClob))
				writerObj.writeNext(failedClob.split(","));
			writerObj.flush();
			writerObj.close();

			CSVReader reader = new CSVReader(new FileReader(file));
			List<String[]> csvBody = reader.readAll();
			String[] headers = csvBody.get(0);
			// System.out.println(headers.length+":::::headers Length");
			for (int i = 1; i < csvBody.size(); i++) {
				String[] values = csvBody.get(i);
				// System.out.println(values.length+"::::::::::Values Length");
				// Map<String,String> map=new LinkedHashMap<>();
				JSONObject jsonObj = new JSONObject() {
					/**
					 * changes the value of JSONObject.map to a LinkedHashMap in order to maintain
					 * order of keys.
					 */
					@Override
					public JSONObject put(String key, Object value) throws JSONException {
						try {
							Field map = JSONObject.class.getDeclaredField("map");
							map.setAccessible(true);
							Object mapValue = map.get(this);
							if (!(mapValue instanceof LinkedHashMap)) {
								map.set(this, new LinkedHashMap<>());
							}
						} catch (NoSuchFieldException | IllegalAccessException e) {
							throw new RuntimeException(e);
						}
						return super.put(key, value);
					}
				};

				for (int j = 0; j < headers.length; j++) {
					try {
						// System.out.println("HEADER:::::::"+headers[j]+"values[j]:::::::::::::"+values[j]);
						jsonObj.put(headers[j], values[j]);
					} catch (Exception e) {
						jsonObj.put(headers[j], "Missed Value");
					}
				}
				jsonArr.put(jsonObj);
			}
			reader.close();
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e.getMessage());
		}
		return jsonArr;
	}

	@Override
	public List<XxrCloudDataProcess> processReconcile(Long cloudTemplateId, HttpServletRequest request)
			throws Exception {
		log.info("Start of processReconcile ########");
		// List<XxrCloudTemplateHeader> cloudTempHdrsLi = new ArrayList<>();
		// String cldStgTableName = "";
		String objectCode = "";
		Long objectId = null;
		List<XxrCloudDataProcess> cloudDataProcessLi = new ArrayList<>();
		String applicationTableName = "";
		try {
			XxrCloudTemplateHeader cloudTempHdr = xxrCloudTemplateHeadersRepository
					.getCloudTemplateById(cloudTemplateId);
			// XxrCloudTemplateHeader cloudTempHdr = cloudTempHdrsLi.get(0);
			objectId = cloudTempHdr.getObjectId();
			// cldStgTableName = cloudTempHdr.getStagingTableName();
			List<XxrCloudTemplateColumns> cloudTemplateCols = xxrCloudTemplateColumnsRepository
					.findByTemplateId(cloudTemplateId);
			XxrCloudTemplateColumns cldTempOrigTransRefCol = cloudTemplateCols.stream()
					.filter(x -> ("Y").equalsIgnoreCase(x.getOrigTransRef())).findFirst().get();

			XxrCloudTemplateColumns cldTempBatchIdCol = cloudTemplateCols.stream()
					.filter(x -> x.getColumnName().equalsIgnoreCase("BATCH_ID")).findFirst().get();
			String origTranRefColumnName = cldTempOrigTransRefCol.getColumnName();
			String batchId = cldTempBatchIdCol.getMappingValue1();

			if (objectId != null)
				objectCode = xxrLookUpValuesRepository.getValueById(objectId);
			// String applicationTableName = utils.getApplicationTableName(objectCode);
			applicationTableName = utils.getInterfaceTableName(objectCode);
			String sqlQuery = "";
			String rejectionSqlQuery = "";
			if (objectCode.equalsIgnoreCase("supplier") || objectCode.equalsIgnoreCase("sites")
					|| objectCode.equalsIgnoreCase("site assignments")) {
				sqlQuery = "select " + origTranRefColumnName + "  from " + applicationTableName
						+ " where status='PROCESSED'  and batch_id='" + batchId + "'";

				rejectionSqlQuery = "select distinct rej.rejection_id,rej.parent_table,rej.reject_lookup_code,rej.ATTRIBUTE,rej.value,rej.parent_id,int.attribute2  from "
						+ applicationTableName
						+ " int, POZ_SUPPLIER_INT_REJECTIONS rej where int.request_id = rej.request_id and int.batch_id='"
						+ batchId + "' and int.status='REJECTED' order by rej.rejection_id";
			} else {
				sqlQuery = "select " + origTranRefColumnName + "  from " + applicationTableName
						+ " where IMPORT_STATUS='PROCESSED'  and batch_id='" + batchId + "'";

				rejectionSqlQuery = "select distinct rej.rejection_id,rej.parent_table,rej.reject_lookup_code,rej.ATTRIBUTE,rej.value,rej.parent_id,int.attribute2  "
						+ applicationTableName
						+ " int, POZ_SUPPLIER_INT_REJECTIONS rej where int.request_id = rej.request_id and int.batch_id='"
						+ batchId + "' and int.IMPORT_STATUS='REJECTED' order by rej.rejection_id";
			}
			// fetching origTransRef of process Records
			XxrCloudDataProcess processedRec = utils.cldDataProcess(sqlQuery, cloudTempHdr, "Fetching origTransRef",
					request);
			cloudDataProcessLi.add(processedRec);

			XxrCloudDataProcess rejectionRec = utils.cldDataProcess(rejectionSqlQuery, cloudTempHdr,
					"Fetching RejectionRecords", request);
			// log.info("End of process reconcile in Service layer");
			cloudDataProcessLi.add(rejectionRec);
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e.getMessage());
		}

		// log.info("Returning End of process reconcile in Service layer");
		return cloudDataProcessLi;
	}

	@Override
	public void processReconcileReport(Long cloudTemplateId, List<String> requestIds, HttpServletResponse response)
			throws Exception {
		log.info("Start of processReconcileReport Method#####");
		String processRecStatus = "";
		String rejectionRecStatus = "";
		// String json = "";
		String cldStgTableName = "";
		String origTranRefColumnName = "";
		String updateQuery = "";
		// List<XxrCloudTemplateHeader> cloudTempHdrsLi = new ArrayList<>();
		String srcStgTblName = "";
		Long sourceTemplateId = null;
		List<String> rejectOrigTransRefLi = new ArrayList<>();
		XxrCloudDataProcess origTransRefProcessRec = null;
		XxrCloudDataProcess rejectionRecordsDesc = null;
		try {
			for (String requestId : requestIds) {
				XxrCloudDataProcess cloudDataProcess = xxrCloudDataProcessRepository.findAll().stream()
						.filter(x -> x.getRequestId().equalsIgnoreCase(requestId)).findFirst().get();
				if (cloudDataProcess.getDescription().equalsIgnoreCase("Fetching origTransRef"))
					origTransRefProcessRec = cloudDataProcess;
				if (cloudDataProcess.getDescription().equalsIgnoreCase("Fetching RejectionRecords"))
					rejectionRecordsDesc = cloudDataProcess;
			}
			if (origTransRefProcessRec != null && rejectionRecordsDesc != null) {
				processRecStatus = origTransRefProcessRec.getStatus();
				rejectionRecStatus = rejectionRecordsDesc.getStatus();
				if (processRecStatus.equalsIgnoreCase("processing") || processRecStatus.equalsIgnoreCase("starting")
						|| rejectionRecStatus.equalsIgnoreCase("starting")
						|| rejectionRecStatus.equalsIgnoreCase("processing"))
					throw new ValidationException("Reconcile is still in process please wait for sometime");
				else if (processRecStatus.equalsIgnoreCase("error") || rejectionRecStatus.equalsIgnoreCase("error"))
					throw new ValidationException(
							"Something went wrong while Reconcile,Please contact system administrator");
				else if (processRecStatus.equalsIgnoreCase("completed")
						&& rejectionRecStatus.equalsIgnoreCase("completed")) {
					String statu = "";
					cldStgTableName = getCldStagingTblName(cloudTemplateId);
					origTranRefColumnName = getOrigTranRefCldClmnName(cloudTemplateId);
					List<String> cldStgTblOrigTransLi = getOrigTransRefCldStgTable(cloudTemplateId);
					XxrBlobConvertrite res = new XxrBlobConvertrite();
					/*
					 * ChannelSftp channelSftp = utils.setupJsch(); channelSftp.connect();
					 * channelSftp.cd(remoteDir); CSVReader reader = new CSVReader( new
					 * InputStreamReader(channelSftp.get(origTransRefProcessRec.getRequestId() +
					 * ".csv"))); List<String[]> readr = reader.readAll();
					 */
					List<String[]> readr = new ArrayList<>();
					Optional<XxrBlobConvertrite> lobResp = xxBlobConvertriteRepository
							.findById(origTransRefProcessRec.getRequestId());
					if (lobResp.isPresent()) {
						res = lobResp.get();
						String lob = res.getBlob();
						String[] lineArr = lob.split("\n");
						for (String line : lineArr) {
							readr.add(line.split(","));
						}
					}
					if (readr.size() == 1) {
						updateQuery = "Update " + cldStgTableName + " set rec_status='REJECTED' where cld_template_id="
								+ cloudTemplateId;
						//updateReconcilationStatus(updateQuery);
						rejectOrigTransRefLi.addAll(cldStgTblOrigTransLi);

					} else {
						for (String cldStgTbleOrigTrans : cldStgTblOrigTransLi) {

							for (int i = 1; i < readr.size(); i++) {
								String[] str = readr.get(i);
								log.info("origTranRef of Interface Table::::" + str[0]);
								if (cldStgTbleOrigTrans.equals(str[0])) {

									updateQuery = "Update " + cldStgTableName + " set rec_status='PROCESSED' where "
											+ origTranRefColumnName + "='" + cldStgTbleOrigTrans
											+ "' and cld_template_id=" + cloudTemplateId;
									statu = "PROCESSED";
									break;
								} else {
									updateQuery = "Update " + cldStgTableName + " set rec_status='REJECTED' where "
											+ origTranRefColumnName + "='" + cldStgTbleOrigTrans
											+ "' and cld_template_id=" + cloudTemplateId;
									statu = "REJECTED";
								}
							}
							if (statu.equals("REJECTED"))
								rejectOrigTransRefLi.add(cldStgTbleOrigTrans);

							//updateReconcilationStatus(updateQuery);
						}
					}
					// String fetchRejRecQuery = "select * from " + cldStgTableName + " where
					// rec_status='REJECTED'";
					XxrCloudTemplateHeader cloudTempHdr = xxrCloudTemplateHeadersRepository
							.getCloudTemplateById(cloudTemplateId);
					// XxrCloudTemplateHeader cloudTempHdr = cloudTempHdrsLi.get(0);

					RecordsPostJobExcecutionPo recordsPostJobExcecutionPo = xxrCloudTemplateHeadersRepository
							.getRecordsPostJobExecution(cloudTempHdr.getTemplateName());
					srcStgTblName = recordsPostJobExcecutionPo.getSourceTableName();
					sourceTemplateId = recordsPostJobExcecutionPo.getSourceTemplateId();
					// ExportRejectedRecords to CSV File
					response.setContentType("application/octet-stream");
					// response.setHeader("Content-Disposition", "attachment;
					// filename=customers.xlsx");
					// response.setContentType("text/csv");
					response.setHeader(HttpHeaders.CONTENT_DISPOSITION,
							"attachment; filename=" + cloudTempHdr.getTemplateName() + ".xlsx");

					exportRejectRecToXlsx(srcStgTblName, cldStgTableName, rejectOrigTransRefLi, response,
							sourceTemplateId, cloudTemplateId, rejectionRecordsDesc.getRequestId());
					// exportRejectRecToCsv(fetchRejRecQuery,response);

				}
			}
		} catch (ValidationException e) {
			throw new ValidationException(e.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e.getMessage());
		}

	}

	private void exportRejectRecToXlsx(String srcStgTblName, String cldStgTableName, List<String> rejectOrigTransRefLi,
			HttpServletResponse response, Long sourceTemplateId, Long cloudTemplateId, String rejectionRequestId)
			throws Exception {
		Connection con = null;
		PreparedStatement srcStgTblStmnt = null;
		PreparedStatement cldStgTblStmnt = null;
		PreparedStatement srcStgStmt = null;
		ResultSet cldStgRs = null;
		ResultSet srcStgRs = null;
		ResultSet srcStagingRs = null;
		// List<String[]> csvArr = new ArrayList<>();

		try (Workbook workbook = new XSSFWorkbook()) {
			// create cloudstagingtable sheet
			Sheet cldStgsheet = workbook.createSheet(cldStgTableName + "(cloud)");

			String cldStgsqlQuery = "Select * from " + cldStgTableName
					+ "  where rec_status='REJECTED' and cld_template_id=" + cloudTemplateId;
			con = dataSourceUtil.createConnection();
			cldStgTblStmnt = con.prepareStatement(cldStgsqlQuery);
			cldStgRs = cldStgTblStmnt.executeQuery();
			ResultSetMetaData rsmd = cldStgRs.getMetaData();
			// Writing header line
			cldStgsheet = createSheetHeader(cldStgsheet, rsmd);
			int rowNum = 1;
			// Writing data line to sheet
			writeDataLines(cldStgRs, cldStgsheet, rsmd, rowNum);

			String srcStgQuery = "Select * from " + srcStgTblName;
			srcStgStmt = con.prepareStatement(srcStgQuery);
			srcStagingRs = srcStgStmt.executeQuery();
			ResultSetMetaData srcstgrsmd = srcStagingRs.getMetaData();

			// create sourcestagingtable sheet
			Sheet srcStgsheet = workbook.createSheet(srcStgTblName + "(source)");
			// Writing header line
			srcStgsheet = createSheetHeader(srcStgsheet, srcstgrsmd);
			int srcrowNum = 1;
			for (String origTransRef : rejectOrigTransRefLi) {

//				log.info("origTransRef####" + origTransRef + "srcrowNum#####" + srcrowNum);
				String srcStgsqlQuery = "Select * from " + srcStgTblName + "  where orig_trans_id=? and template_id=?";
				srcStgTblStmnt = con.prepareStatement(srcStgsqlQuery);
				srcStgTblStmnt.setString(1, origTransRef);
				srcStgTblStmnt.setLong(2, sourceTemplateId);
				srcStgRs = srcStgTblStmnt.executeQuery();
				ResultSetMetaData srcrsmd = srcStgRs.getMetaData();
				// Writing data line to sheet

				writeDataLines(srcStgRs, srcStgsheet, srcrsmd, srcrowNum);
				srcrowNum++;

			}

			// creation rejection reason sheet
			createRejectionDescSheet(workbook, rejectionRequestId);

			ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
			workbook.write(outputStream);
			IOUtils.copy(new ByteArrayInputStream(outputStream.toByteArray()), response.getOutputStream());

		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e.getMessage());
		} finally {
			if (srcStgRs != null)
				srcStgRs.close();
			if (srcStgTblStmnt != null)
				srcStgTblStmnt.close();
			if (cldStgRs != null)
				cldStgRs.close();
			if (cldStgTblStmnt != null)
				cldStgTblStmnt.close();
			if (srcStagingRs != null)
				srcStagingRs.close();
			if (srcStgStmt != null)
				srcStgStmt.close();
			if (con != null)
				con.close();
		}
	}

	private void createRejectionDescSheet(Workbook workbook, String rejectionRequestId) throws Exception {
		try {
			Sheet rejRessheet = workbook.createSheet("REJECTION_REASON");
			/*
			 * ChannelSftp channelSftp = utils.setupJsch(); channelSftp.connect();
			 * channelSftp.cd(remoteDir); CSVReader reader = new CSVReader(new
			 * InputStreamReader(channelSftp.get(rejectionRequestId + ".csv")));
			 * List<String[]> readr = reader.readAll();
			 */
			XxrBlobConvertrite res = new XxrBlobConvertrite();
			List<String[]> readr = new ArrayList<>();
			Optional<XxrBlobConvertrite> lobResp = xxBlobConvertriteRepository.findById(rejectionRequestId);
			if (lobResp.isPresent()) {
				res = lobResp.get();
				String lob = res.getBlob();
				String[] lineArr = lob.split("\n");
				for (String line : lineArr) {
					readr.add(line.split(","));
				}
			}

			for (int i = 0; i < readr.size(); i++) {
				Row row = rejRessheet.createRow(i);
				String[] rowValues = readr.get(i);
				for (int j = 0; j < rowValues.length; j++) {
					Cell cell = row.createCell(j);
					cell.setCellValue(rowValues[j]);
				}
			}
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
	}

	private void writeDataLines(ResultSet srcStgRs, Sheet sheet, ResultSetMetaData rsmd, int rowNum) throws Exception {
		try {
			while (srcStgRs.next()) {

				Row row = sheet.createRow(rowNum);
				int numColumns = rsmd.getColumnCount();
				for (int i = 1; i < numColumns + 1; i++) {
					Cell cell = row.createCell(i - 1);
					String column_name = rsmd.getColumnName(i);

					if (rsmd.getColumnType(i) == java.sql.Types.ARRAY) {
						cell.setCellValue(srcStgRs.getArray(column_name).toString());
					} else if (rsmd.getColumnType(i) == java.sql.Types.BIGINT) {
						cell.setCellValue(srcStgRs.getInt(column_name));
					} else if (rsmd.getColumnType(i) == java.sql.Types.BOOLEAN) {
						cell.setCellValue(srcStgRs.getBoolean(column_name));
					} else if (rsmd.getColumnType(i) == java.sql.Types.BLOB) {
						cell.setCellValue((RichTextString) srcStgRs.getBlob(column_name));
					} else if (rsmd.getColumnType(i) == java.sql.Types.DOUBLE) {
						cell.setCellValue(srcStgRs.getDouble(column_name));
					} else if (rsmd.getColumnType(i) == java.sql.Types.FLOAT) {
						cell.setCellValue(srcStgRs.getFloat(column_name));
					} else if (rsmd.getColumnType(i) == java.sql.Types.INTEGER) {
						cell.setCellValue(srcStgRs.getInt(column_name));
					} else if (rsmd.getColumnType(i) == java.sql.Types.NVARCHAR) {
						cell.setCellValue(srcStgRs.getNString(column_name));
					} else if (rsmd.getColumnType(i) == java.sql.Types.VARCHAR) {
						cell.setCellValue(srcStgRs.getString(column_name));
					} else if (rsmd.getColumnType(i) == java.sql.Types.TINYINT) {
						cell.setCellValue(srcStgRs.getInt(column_name));
					} else if (rsmd.getColumnType(i) == java.sql.Types.SMALLINT) {
						cell.setCellValue(srcStgRs.getInt(column_name));
					} else if (rsmd.getColumnType(i) == java.sql.Types.DATE) {
						cell.setCellValue(srcStgRs.getDate(column_name));
					} else if (rsmd.getColumnType(i) == java.sql.Types.TIMESTAMP) {
						cell.setCellValue(srcStgRs.getTimestamp(column_name));
					} else if (rsmd.getColumnType(i) == java.sql.Types.NUMERIC) {
						if (srcStgRs.getBigDecimal(column_name) != null) {
							cell.setCellValue(srcStgRs.getBigDecimal(column_name).longValue());
						}
					}
				}
				rowNum++;
			}

		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}

	}

	private Sheet createSheetHeader(Sheet sheet, ResultSetMetaData rsmd) throws Exception {

		try {

			Row row = sheet.createRow(0);
			int numColumns = rsmd.getColumnCount();
			for (int i = 0; i < numColumns; i++) {
				Cell cell = row.createCell(i);
				cell.setCellValue(rsmd.getColumnName(i + 1));
			}

		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return sheet;
	}

	/*
	 * private void exportRejectRecToCsv(String fetchRejRecQuery,HttpServletResponse
	 * response) throws Exception { Connection con = null; PreparedStatement stmnt =
	 * null; ResultSet rs = null; try { con = dataSourceUtil.createConnection();
	 * stmnt = con.prepareStatement(fetchRejRecQuery); rs = stmnt.executeQuery();
	 * 
	 * PrintWriter writer=response.getWriter(); CSVWriter csvWriter = new
	 * CSVWriter(writer, CSVWriter.DEFAULT_SEPARATOR, CSVWriter.NO_QUOTE_CHARACTER,
	 * CSVWriter.NO_ESCAPE_CHARACTER, CSVWriter.DEFAULT_LINE_END);
	 * csvWriter.writeAll(rs, true); csvWriter.flush(); csvWriter.close(); } catch
	 * (Exception e) { throw new Exception(e.getMessage()); } finally { try { if (rs
	 * != null) rs.close(); if (stmnt != null) stmnt.close(); if (con != null)
	 * con.close(); } catch (SQLException ex) { ex.getMessage(); } }
	 * 
	 * }
	 */
	/*
	 * private int updateReconcilationStatus(String updateQuery) throws Exception {
	 * Connection con = null; PreparedStatement stmnt = null; int count = 0; try {
	 * con = dataSourceUtil.createConnection(); stmnt =
	 * con.prepareStatement(updateQuery); count = stmnt.executeUpdate();
	 * 
	 * } catch (Exception e) { throw new Exception(e.getMessage()); } finally { try
	 * { if (stmnt != null) stmnt.close(); if (con != null) con.close(); } catch
	 * (SQLException ex) { ex.getMessage(); } } return count; }
	 */

	private String getCldStagingTblName(Long cloudTemplateId) throws Exception {
		// List<XxrCloudTemplateHeader> cloudTempHdrsLi = new ArrayList<>();
		String cldStgTableName = "";
		try {
			XxrCloudTemplateHeader cloudTempHdr = xxrCloudTemplateHeadersRepository
					.getCloudTemplateById(cloudTemplateId);
			// XxrCloudTemplateHeader cloudTempHdr = cloudTempHdrsLi.get(0);
			cldStgTableName = cloudTempHdr.getStagingTableName();
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return cldStgTableName;
	}

	private String getOrigTranRefCldClmnName(Long cloudTemplateId) throws Exception {
		String origTranRefColumnName = "";
		try {

			List<XxrCloudTemplateColumns> cloudTemplateCols = xxrCloudTemplateColumnsRepository
					.findByTemplateId(cloudTemplateId);
			XxrCloudTemplateColumns cldTempOrigTransRefCol = cloudTemplateCols.stream()
					.filter(x -> x.getOrigTransRef().equalsIgnoreCase("Y")).findFirst().get();
			origTranRefColumnName = cldTempOrigTransRefCol.getColumnName();
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return origTranRefColumnName;
	}

	private List<String> getOrigTransRefCldStgTable(Long cloudTemplateId) throws Exception {

		Connection con = null;
		PreparedStatement stmnt = null;
		ResultSet rs = null;
		List<String> origTransLi = new ArrayList<>();
		String cldStgTableName = "";
		String origTranRefColumnName = "";
		try {

			cldStgTableName = getCldStagingTblName(cloudTemplateId);
			origTranRefColumnName = getOrigTranRefCldClmnName(cloudTemplateId);
			String query = "select " + origTranRefColumnName + " from " + cldStgTableName + "  where cld_template_id="
					+ cloudTemplateId;

			con = dataSourceUtil.createConnection();
			stmnt = con.prepareStatement(query);
			rs = stmnt.executeQuery();

			while (rs.next()) {
				origTransLi.add(rs.getString(origTranRefColumnName));
			}
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		} finally {
			try {
				if (rs != null)
					rs.close();
				if (stmnt != null)
					stmnt.close();
				if (con != null)
					con.close();
			} catch (SQLException ex) {
				ex.getMessage();
			}

		}

		return origTransLi;
	}

	@Override
	public void generateHdlFromLob(String cloudTemplateId, String batchName, String isIntialLoad, HttpServletResponse response)
			throws Exception {
		log.info("Start Of generateHdlFromLob in service###");
		// List<XxrCloudTemplateHeader> cloudTemplateHeaderList = new ArrayList<>();
		Long parentObjectId = null;
		String parentObjectCode = "";
		try {
			response.setContentType("text/dat");
			XxrCloudTemplateHeader xxrCloudTemplateHeader = xxrCloudTemplateHeadersRepository
					.getCloudTemplateById(Long.valueOf(cloudTemplateId));
			// XxrCloudTemplateHeader xxrCloudTemplateHeader =
			// cloudTemplateHeaderList.get(0);
			if (xxrCloudTemplateHeader != null)
				parentObjectId = xxrCloudTemplateHeader.getParentObjectId();
			if (parentObjectId != null)
				parentObjectCode = xxrLookUpValuesRepository.getValueById(parentObjectId);
			response.setHeader(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=" + parentObjectCode + ".dat");
			cloudTemplateHeaderDaoImpl.generateHdlFromLob(cloudTemplateId, batchName,   isIntialLoad, response.getWriter());

		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
	}
	@Override
	public UpdateFailedRecResp updateEditedFailedRecords(UpdateFailedRecReqPo updateFailedRecReqPo) throws Exception {
		UpdateFailedRecResp updateFailedRecResp = new UpdateFailedRecResp();
		List<String> sqlQueryLi = new ArrayList<>();
		RecordsPostJobExcecutionPo recordsPostJobExce = xxrCloudTemplateHeadersRepository
				.getRecordsPostJobExecution(updateFailedRecReqPo.getCloudTemplateName());
		try {
			for (String editedFailedRec : updateFailedRecReqPo.getEditedFailedRecs()) {
				String[] column = editedFailedRec.split(":");
				String[] columnNameArr = column[0].split(",");
				String[] columnValueArr = column[1].split(",");
				StringBuffer sb = new StringBuffer();
				sb.append("update " + recordsPostJobExce.getSourceTableName() + " set ");
				for (int i = 1; i < columnNameArr.length; i++) {
					sb.append(columnNameArr[i] + "='" + columnValueArr[i] + "'");
					if (i != columnNameArr.length - 1)
						sb.append(",");
				}
				sb.append(" where " + columnNameArr[0] + "='" + columnValueArr[0] + "'");
				sqlQueryLi.add(sb.toString());
			}
			updateFailedRecResp = cloudTemplateHeaderDaoImpl.updateFailedRec(sqlQueryLi);
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return updateFailedRecResp;
	}

	@Override
	public void getTransformationReport(Long cloudTemplateId, String batchName, HttpServletResponse response)
			throws Exception {
		log.info("Start Of getTransformationReport in service###");
		try {

			response.setHeader(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=TransformationReport.csv");
			cloudTemplateHeaderDaoImpl.getTransformationReport(cloudTemplateId, batchName, response.getWriter());

		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
	}

	@Override
	public DeleteStagingDataRes deleteStagingData(DeleteStagingDataReq deleteStagingDataReq) throws Exception {
		log.info("Start Of deleteStagingData in service###");
		DeleteStagingDataRes deleteStagingDataRes = new DeleteStagingDataRes();
		try {
			deleteStagingDataRes = cloudTemplateHeaderDaoImpl.deleteStagingData(deleteStagingDataReq);
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return deleteStagingDataRes;
	}

	@Override
	public void generateXlsFromLob(Long cloudTemplateId, HttpServletResponse response) throws Exception {
		log.info("Start of generateXlsFromLob#######");
		// List<XxrCloudTemplateHeader> cloudTemplateHeaderList = new ArrayList<>();
		Long objectId = null;
		Long groupId = null;
		String parentObjectCode = "";
		// String metaDataTableName = "";
		XxrCloudTemplateHeader xxrCloudTemplateHeader = xxrCloudTemplateHeadersRepository
				.getCloudTemplateById(cloudTemplateId);
		// XxrCloudTemplateHeader xxrCloudTemplateHeader =
		// cloudTemplateHeaderList.get(0);
		if (xxrCloudTemplateHeader != null) {
			objectId = xxrCloudTemplateHeader.getObjectId();

			parentObjectCode = xxrLookUpValuesRepository.getValueById(xxrCloudTemplateHeader.getParentObjectId());

		}

		groupId = xxrObjectCodeGroupingLinesRepository.getGroupIdbyObjectId(objectId);

		InputStream inputStream = this.getClass().getClassLoader()
				.getResourceAsStream("SAP_S4_HANA_Template_File.xlsx");
		XSSFWorkbook wb = new XSSFWorkbook(inputStream);
		if (groupId != null) {
			genrateXlsWorkBook(cloudTemplateId, groupId, wb);
		}
		response.setContentType("application/octet-stream");
		response.setHeader(HttpHeaders.CONTENT_DISPOSITION,
				"attachment; filename=" + parentObjectCode + "_Template.xlsx");
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		wb.write(outputStream);
		IOUtils.copy(new ByteArrayInputStream(outputStream.toByteArray()), response.getOutputStream());

	}

	private void genrateXlsWorkBook(Long cloudTemplateId, Long groupId, XSSFWorkbook wb) throws Exception {
		Long objectId = null;
		XxrTemplateRelation xxrTemplateRelation = null;
		Predicate<XxrTemplateRelation> filterTemplateGrouping = x -> {
			String[] strArr = x.getTemplateIds().split(",");
			boolean flag = false;
			for (String s : strArr) {
				long templateId = Long.parseLong(s);
				if (templateId == cloudTemplateId) {
					flag = true;
					break;
				}
			}
			return flag;
		};
		List<XxrTemplateRelation> xxrTemplateRelationLi = xxrTemplateRelationRepository.getTemplatRelations(groupId);
		if (xxrTemplateRelationLi.stream().filter(filterTemplateGrouping).count() <= 0)
			throw new ValidationException(
					"This Template is not part of template grouping,Please add into template grouping");
		xxrTemplateRelation = xxrTemplateRelationLi.stream().filter(filterTemplateGrouping).findFirst().get();
		if (xxrTemplateRelation != null) {
			String[] templateArr = xxrTemplateRelation.getTemplateIds().split(",");
			for (String tempId : templateArr) {
				long templateId = Long.parseLong(tempId);
				XxrCloudTemplateHeader cloudTemplateHeader = xxrCloudTemplateHeadersRepository
						.getCloudTemplateById(templateId);
				// XxrCloudTemplateHeader cloudTemplateHeader = cloudTemplateHeaderLi.get(0);
				objectId = cloudTemplateHeader.getObjectId();
				String objectCode = xxrLookUpValuesRepository.getValueById(objectId);

				/*
				 * XSSFSheet sheet = null;
				 * 
				 * if ("Contacts".equalsIgnoreCase(objectCode)) sheet =
				 * wb.getSheet("General Data"); else sheet = wb.getSheet("BP roles");
				 */

				XSSFSheet sheet = wb.getSheet(objectCode);
				int rowNum = 8;
				String clobString = cloudTemplateHeaderDaoImpl.generateXlsFromLob(templateId);
				String[] clobStringArr = clobString.split("\\n");
				for (int i = 0; i < clobStringArr.length; i++) {
					Row row = sheet.createRow(rowNum);

					String[] arr = clobStringArr[i].split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
					for (int j = 0; j < arr.length; j++) {
						Cell cell = row.createCell(j);
//						log.info(arr[j]);
						cell.setCellValue(arr[j].replaceAll("\"", ""));
					}
					rowNum++;
				}
			}
		}

	}

	@Override
	public SyncCloudInterfaceDataResPo syncCloudInterfaceTbleData(SyncCloudInterfaceDataReq syncCloudInterfaceDataReq,
			HttpServletRequest request) throws Exception {
		log.info("Start of syncCloudInterfaceTbleData in service####");
		// List<XxrCloudTemplateHeader> cloudTempHdrsLi = new ArrayList<>();
		// String cldStgTableName = "";
		String objectCode = "";
		Long objectId = null;
		List<XxrCloudDataProcess> cloudDataProcessLi = new ArrayList<>();
		String applicationTableName = "";
		String rejectionTableName = "";
		Connection con = null;
		int count = 0;
		SyncCloudInterfaceDataResPo syncCloudInterfaceDataResPo = new SyncCloudInterfaceDataResPo();
		try {
			log.info(syncCloudInterfaceDataReq.getCloudTemplateId() + ":::TemplateId");
			XxrCloudTemplateHeader cloudTempHdr = xxrCloudTemplateHeadersRepository
					.getCloudTemplateById(syncCloudInterfaceDataReq.getCloudTemplateId());
			// XxrCloudTemplateHeader cloudTempHdr = cloudTempHdrsLi.get(0);
			objectId = cloudTempHdr.getObjectId();
			Long metaDataTableId = cloudTempHdr.getMetaDataTableId();
			MetaDataColumnsPo metaDataColumnsPo = utils.getMetaDataAndColumnNames(metaDataTableId);
			if (objectId != null)
				objectCode = xxrLookUpValuesRepository.getValueById(objectId);
			// applicationTableName = utils.getInterfaceTableName(objectCode);
			XxrCloudConfig xxrCloudConfig = xxrCloudConfigRepository.findByObjectCode(objectCode);
			applicationTableName = xxrCloudConfig.getInterfaceTableName();
			rejectionTableName = xxrCloudConfig.getRejectionTableName();
			String sqlQuery = "Select " + metaDataColumnsPo.getColumnNames() + " from " + applicationTableName
					+ " where LOAD_REQUEST_ID=" + syncCloudInterfaceDataReq.getLoadRequestId();
			XxrCloudDataProcess val = null;
			val = utils.cldDataProcess(sqlQuery, "Sync Interface table", request,
					cloudTempHdr.getProjectId());
			/*
			 * XxrCloudDataProcess cloudDataProce =
			 * xxrCloudDataProcessRepository.findAll().stream() .filter(x ->
			 * x.getStatus().equalsIgnoreCase("Starting"))
			 * .max(Comparator.comparing(XxrCloudDataProcess::getPriority)).orElse(null);
			 * 
			 * CloudDataProcessingReqPo cloudDataProcessingReqPo = new
			 * CloudDataProcessingReqPo();
			 * cloudDataProcessingReqPo.setDescription("ConvertRite");
			 * cloudDataProcessingReqPo.setLookUpFlag("Y");
			 * cloudDataProcessingReqPo.setScheduledJobCall("N");
			 * cloudDataProcessingReqPo.setPodId(cloudTempHdr.getPodId());
			 * cloudDataProcessingReqPo.setProjectId(cloudTempHdr.getProjectId());
			 * cloudDataProcessingReqPo.setSqlQuery(sqlQuery); if (cloudDataProce != null)
			 * cloudDataProcessingReqPo.setPriority(cloudDataProce.getPriority() + 1); else
			 * cloudDataProcessingReqPo.setPriority(1);
			 * 
			 * log.info("priority::::::" + cloudDataProcessingReqPo.getPriority());
			 * 
			 * HttpHeaders headers = new HttpHeaders(); headers.set("Accept",
			 * MediaType.APPLICATION_JSON_VALUE);
			 * headers.setContentType(MediaType.APPLICATION_JSON); //
			 * headers.set("Authorization", request.getHeader("Authorization"));
			 * HttpEntity<CloudDataProcessingReqPo> requestEntity = new
			 * HttpEntity<>(cloudDataProcessingReqPo, headers); //
			 * System.out.println(String.format(url, parametrizedArgs));
			 * 
			 * ResponseEntity<?> cloudDataApiResponse = restTemplate.exchange(url,
			 * HttpMethod.POST, requestEntity, XxrCloudDataProcess.class);
			 */
			// create connection
			con = dataSourceUtil.createConnection();
			if (val != null) {
				String requestId = val.getRequestId();
//				log.info(requestId + "::::::::requestId");
				Utils.getStatus(con, requestId);
				count = insertIntoInterfaceTable(requestId, con, applicationTableName, metaDataColumnsPo);
			}

			if (!Validations.isNullOrEmpty(rejectionTableName))
				syncRejectionTableData(syncCloudInterfaceDataReq, con, applicationTableName, rejectionTableName,
						request);

			syncCloudInterfaceDataResPo.setCount(count);
			syncCloudInterfaceDataResPo.setMessage("Successfully Synced Data from Cloud Interface Table");
		} finally {
			if (con != null)
				con.close();
		}
		return syncCloudInterfaceDataResPo;
	}

	private void syncRejectionTableData(SyncCloudInterfaceDataReq syncCloudInterfaceDataReq, Connection con,
			String applicationTableName, String rejectionTableName, HttpServletRequest request) throws Exception {
		log.info("Start of syncRejectionTableData #########");
		int count = 0;
		Long tableId = null;
		PreparedStatement stmnt = con.prepareStatement("select distinct request_id from " + applicationTableName
				+ " where load_request_id=" + syncCloudInterfaceDataReq.getLoadRequestId());
		ResultSet rs = stmnt.executeQuery();
		Long requestId = null;
		if (rs.next()) {
			requestId = rs.getLong("request_id");
		}
		if (requestId != null) {
			XxrCloudTemplateHeader cloudTempHdr = xxrCloudTemplateHeadersRepository
					.getCloudTemplateById(syncCloudInterfaceDataReq.getCloudTemplateId());
			String sqlQuery = "Select * from " + rejectionTableName + "  where REQUEST_ID=" + requestId;
			XxrCloudDataProcess val = null;
			val = utils.cldDataProcess(sqlQuery, "Sync rejection table", request,
					cloudTempHdr.getProjectId());
			if (val != null) {
				String cldRequestId = val.getRequestId();
				log.info(cldRequestId + "::::::::cldRequestId");
				Utils.getStatus(con, cldRequestId);
				tableId = xxrCloudTableRepository.getTableId(rejectionTableName);
				if (tableId == null)
					throw new ValidationException("Please load metadata of rejection table");
				MetaDataColumnsPo metaDataColumnsPo = utils.getMetaDataAndColumnNames(tableId);
				count = insertIntoInterfaceTable(cldRequestId, con, rejectionTableName, metaDataColumnsPo);
				log.info("No of Records Inserted to rejection table" + count);
			}
		} else {
			log.info("No records in Interface table with this load request id:::"
					+ syncCloudInterfaceDataReq.getLoadRequestId());
		}

	}

	private int insertIntoInterfaceTable(String requestId, Connection con, String applicationTableName,
			MetaDataColumnsPo metaDataColumnsPo) throws Exception {
		log.info("Start of insertIntoInterfaceTable ######");
		PreparedStatement insertColumnsStmnt = null;
		PreparedStatement defineOffStmnt = null;
		int count = 0;
		XxrBlobConvertrite res = new XxrBlobConvertrite();
		// List<String[]> readr = new ArrayList<>();
		String[] lineArr = null;

		DatabaseMetaData dbm = con.getMetaData();
		// check if Master table is already there
		ResultSet tables = dbm.getTables(null, null, applicationTableName, null);
		if (tables.next()) {
			// Table exists
			log.info("ApplicationTableName Already Exists");
		} else {
			// Master Table Creation
			String masterSql = "CREATE TABLE " + applicationTableName + "  (" + metaDataColumnsPo.getMetaDataStr()
					+ " )";
			PreparedStatement stmnt = con.prepareStatement(masterSql);
			int masterCount = stmnt.executeUpdate();
			log.info(masterCount + "########count");
			stmnt.close();
		}

		Optional<XxrBlobConvertrite> lobResp = xxBlobConvertriteRepository.findById(requestId);
		if (lobResp.isPresent()) {
			res = lobResp.get();
			String lob = res.getBlob();
			lineArr = lob.split("\n");
			/*
			 * for (String line : lineArr) { readr.add(line.split(",")); }
			 */
		}

		if (lineArr.length <= 1) {
			count = 0;
		} else {
			// check if error table already exists or not
			ResultSet errTables = dbm.getTables(null, null, "ERR$_" + applicationTableName, null);
			if (errTables.next()) {
				// Table exists
				log.info("ERR$_" + applicationTableName + " Already Exists");
			} else {

				CallableStatement callblestmnt = con.prepareCall("{call DBMS_ERRLOG.CREATE_ERROR_LOG (?)}");
				callblestmnt.setString(1, applicationTableName);
				callblestmnt.execute();
			}
			/*
			 * defineOffStmnt=con.
			 * prepareStatement("exec DBMS_ERRLOG.CREATE_ERROR_LOG (applicationTableName)");
			 * defineOffStmnt.execute();
			 */

			StringBuffer sb = new StringBuffer(
					"insert into " + applicationTableName + " (  " + metaDataColumnsPo.getColumnNames() + " ) ");
			for (int i = 1; i < lineArr.length; i++) {
				String[] metaData = metaDataColumnsPo.getMetaDataStr().split(",");
				String[] values = lineArr[i].split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
				//log.info(values.length + "::::::Values length");
				//StringBuffer columnNamesSb = new StringBuffer();
				StringBuffer valuesSb = new StringBuffer();
				for (int j = 0; j < metaData.length; j++) {
					// log.info(metaData[j]);
					String[] metaDataArr = metaData[j].split("\\s+");
					// columnNamesSb.append(metaDataArr[0]);

					if (metaDataArr[1].equals("NUMBER")) {
						if (values[j].isEmpty() || values[j].isBlank() || values[j] == null)
							valuesSb.append("null");
						else
							valuesSb.append(values[j]);
					} else if (metaDataArr[1].contains("VARCHAR2")) {
						if (!Validations.isNullOrEmpty(values[j])) {
							if (values[j].contains(",")) {
								valuesSb.append("q'{" + values[j].substring(1, values[j].length() - 1) + "}'");
							} else
								valuesSb.append("q'{" + values[j] + "}'");
						} else
							valuesSb.append("null");
					}
					if (j != metaData.length - 1) {
						// columnNamesSb.append(",");
						valuesSb.append(",");
					}

				}

				sb.append("select " + valuesSb + " from dual ");

				if (i != lineArr.length - 1) {
					// columnNamesSb.append(",");
					sb.append("\n");
					sb.append("union all ");
					sb.append("\n");
				} else {
					sb.append("LOG ERRORS INTO err$_" + applicationTableName + " REJECT LIMIT unlimited");
				}
			}

			/*
			 * StringBuffer sb = new StringBuffer("insert all "); for (int i = 1; i <
			 * lineArr.length; i++) { String[] metaData =
			 * metaDataColumnsPo.getMetaDataStr().split(","); String[] values =
			 * lineArr[i].split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
			 * log.info(values.length + "::::::Values length"); StringBuffer columnNamesSb =
			 * new StringBuffer(); StringBuffer valuesSb = new StringBuffer(); for (int j =
			 * 0; j < metaData.length; j++) { log.info(metaData[j]); String[] metaDataArr =
			 * metaData[j].split("\\s+"); columnNamesSb.append(metaDataArr[0]); if
			 * (metaDataArr[1].equals("NUMBER")) { if (values[j].isEmpty() ||
			 * values[j].isBlank() || values[j] == null) valuesSb.append("null"); else
			 * valuesSb.append(values[j]); } else if (metaDataArr[1].contains("VARCHAR2")) {
			 * if(!Validations.isNullOrEmpty(values[j])) valuesSb.append("q'{" + values[j] +
			 * "}'"); else valuesSb.append("null"); } if (j != metaData.length - 1) {
			 * columnNamesSb.append(","); valuesSb.append(","); }
			 * 
			 * }
			 * 
			 * sb.append(" into " + applicationTableName + "(" + columnNamesSb.toString() +
			 * ") VALUES (" + valuesSb.toString() + ")"); }
			 * sb.append(" SELECT 1 FROM DUAL");
			 */
//			log.info("sqlquery:::" + sb);
			insertColumnsStmnt = con.prepareStatement(sb.toString());
			count = insertColumnsStmnt.executeUpdate();

		}
		log.info("No of rows synced from saas::::" + count);
		return count;
	}

	@Override
	public void onFallbackmethod(CallBackReqPo request) throws ValidationException {
		log.info("Start of onFallbackmethod in service ####");
		// List<XxrCloudTemplateHeader> cloudTemplateHeaderList = new ArrayList<>();
		Long objectId = null;
		Long groupId = null;
		Long requestId = request.getHeader().getRelatesTo();
//		log.info(requestId + ":::::::requestId");
		String jobStatus = request.getBody().getOnJobCompletion().getState();

		XxrErpIntegration xxrErpIntegrationRes = xxrErpIntegrationRepository.findByResult(requestId);
		xxrErpIntegrationRes.setJobStatus(jobStatus);
		XxrErpIntegration saveXxrErpIntegrationRes = xxrErpIntegrationRepository.save(xxrErpIntegrationRes);

		Long cloudTemplateId = saveXxrErpIntegrationRes.getCloudTemplateId();
		log.info("cloudTemplateId:::::::" + cloudTemplateId);
		XxrCloudTemplateHeader xxrCloudTemplateHeader = xxrCloudTemplateHeadersRepository
				.getCloudTemplateById(cloudTemplateId);
		// XxrCloudTemplateHeader xxrCloudTemplateHeader =
		// cloudTemplateHeaderList.get(0);
		objectId = xxrCloudTemplateHeader.getObjectId();
		groupId = xxrObjectCodeGroupingLinesRepository.getGroupIdbyObjectId(objectId);

		if (groupId != null) {
			XxrTemplateRelation xxrTemplateRelation = null;
			Predicate<XxrTemplateRelation> filterTemplateGrouping = x -> {
				String[] strArr = x.getTemplateIds().split(",");
				boolean flag = false;
				for (String s : strArr) {
					long templateId = Long.parseLong(s);
					if (templateId == cloudTemplateId) {
						flag = true;
						break;
					}
				}
				return flag;
			};
			List<XxrTemplateRelation> xxrTemplateRelationLi = xxrTemplateRelationRepository
					.findByGroupIdOrderByIdDesc(groupId);
			if (xxrTemplateRelationLi.stream().filter(filterTemplateGrouping).count() <= 0)
				throw new ValidationException(
						"This Template is not part of template grouping,Please add into template grouping");
			xxrTemplateRelation = xxrTemplateRelationLi.stream().filter(filterTemplateGrouping).findFirst().get();
			if (xxrTemplateRelation != null) {
				String[] templateArr = xxrTemplateRelation.getTemplateIds().split(",");
				for (String templateId : templateArr) {

					SyncCloudInterfaceDataReq syncCloudInterfaceDataReq = new SyncCloudInterfaceDataReq();
					syncCloudInterfaceDataReq.setCloudTemplateId(Long.parseLong(templateId));
					syncCloudInterfaceDataReq.setLoadRequestId(requestId);
					HttpHeaders headers = new HttpHeaders();
					headers.set("Accept", MediaType.APPLICATION_JSON_VALUE);
					headers.setContentType(MediaType.APPLICATION_JSON);
					HttpEntity<SyncCloudInterfaceDataReq> requestEntity = new HttpEntity<>(syncCloudInterfaceDataReq,
							headers);
//					ResponseEntity<?> syncCloudDataResponse = restTemplate.exchange(syncSaasDataUrl, HttpMethod.POST,
//							requestEntity, SyncCloudInterfaceDataResPo.class);
				}

			}
		} else {
			SyncCloudInterfaceDataReq syncCloudInterfaceDataReq = new SyncCloudInterfaceDataReq();
			log.info("else cloudTemplateId::::" + cloudTemplateId);
			syncCloudInterfaceDataReq.setCloudTemplateId(cloudTemplateId);
			syncCloudInterfaceDataReq.setLoadRequestId(requestId);
			HttpHeaders headers = new HttpHeaders();
			headers.set("Accept", MediaType.APPLICATION_JSON_VALUE);
			headers.setContentType(MediaType.APPLICATION_JSON);
			HttpEntity<SyncCloudInterfaceDataReq> requestEntity = new HttpEntity<>(syncCloudInterfaceDataReq, headers);

//			ResponseEntity<?> syncCloudDataResponse = restTemplate.exchange(syncSaasDataUrl, HttpMethod.POST,
//					requestEntity, SyncCloudInterfaceDataResPo.class);
		}

	}

	@Override
	public List<String> getBatchNames(Long templateId) throws Exception {
		log.info("Start of  getBatchNames in service ######");
		Connection con = null;
		PreparedStatement stmnt = null;
		List<String> li = new ArrayList<>();
		ResultSet rs = null;
		try {
			/*
			 * XxrCldTempHdrsView xxrCldTempHdrsView =
			 * xxrCldTempHdrsViewRepository.findByTemplateId(templateId); XxrSrcTempHdrsView
			 * xxrSrcTempHdrsView = xxrSrcTempHdrsViewRepository
			 * .findByTemplateId(xxrCldTempHdrsView.getSourceTemplateId());
			 */
			String srcStagingTableName = xxrCldTempHdrsViewRepository.getSrcStagingTableName(templateId);
			if (srcStagingTableName != null) {
				con = dataSourceUtil.createConnection();

				stmnt = con.prepareStatement("select distinct xxr_batch_name from " + srcStagingTableName);
				rs = stmnt.executeQuery();

				while (rs.next()) {
					li.add(rs.getString("xxr_batch_name"));
				}
			}
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		} finally {
			if (rs != null)
				rs.close();
			if (con != null)
				con.close();
		}
		return li;
	}

	@Override
	public void generateFbdiFromLobV1(Long cloudTemplateId, String batchName, HttpServletResponse response)
			throws Exception {
		log.info("Start of generateFbdiFromLobV1####");

		Long metaDataTableId = null;
		String metaDataTableName = "";
		try {
			response.setContentType("text/csv");
			XxrCloudTemplateHeader xxrCloudTemplateHeader = xxrCloudTemplateHeadersRepository
					.getCloudTemplateById(cloudTemplateId);

			// XxrCloudTemplateHeader xxrCloudTemplateHeader =
			// cloudTemplateHeaderList.get(0);
			if (xxrCloudTemplateHeader != null)
				metaDataTableId = xxrCloudTemplateHeader.getMetaDataTableId();
			if (metaDataTableId != null) {
				metaDataTableName = xxrCloudTableRepository.getMetaDataTableName(metaDataTableId);
			}
			response.setHeader(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=" + metaDataTableName + ".csv");
			response.setHeader("cldfbdifilename", metaDataTableName + ".csv");
			cloudTemplateHeaderDaoImpl.generateFbdiFromLobV1(cloudTemplateId, batchName, response.getWriter());
			// cloudTemplateHeaderDaoImpl.generateFailedRec(cloudTemplateId,
			// response.getWriter());
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}

	}

	@Override
	public List<XxrProcessRequestsView> getPrcRequestsByRequestType(
			ProcessReqByRequestTypeReq processReqByRequestTypeReq, HttpHeaders httpHeaders) {
		log.info("Start of getPrcRequestsByRequestType#####");
		List<XxrProcessRequestsView> processRequestLi = new ArrayList<>();
		Page<XxrProcessRequestsView> page = null;
		Pageable pageable = PageRequest.of(processReqByRequestTypeReq.getPageNo(),
				processReqByRequestTypeReq.getPageSize(),
				Sort.by(processReqByRequestTypeReq.getSortDirection(), processReqByRequestTypeReq.getSortBy()));
		if (!Validations.isNullOrEmpty(processReqByRequestTypeReq.getBatchName()))
			page = xxrProcessRequestsViewRepository.findAllByXxrBatchNameAndRequestType(
					processReqByRequestTypeReq.getBatchName(), processReqByRequestTypeReq.getRequestType(), pageable);

		httpHeaders.set("pagecount", String.valueOf(page.getTotalPages()));
		httpHeaders.set("totalcount", String.valueOf(page.getTotalElements()));

		if (page.hasContent())
			processRequestLi = page.getContent();

		return processRequestLi;
	}

}
