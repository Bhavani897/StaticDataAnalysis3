package com.rite.products.convertrite.service;

import java.io.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletRequest;
import javax.transaction.Transactional;

import com.rite.products.convertrite.model.*;
import com.rite.products.convertrite.respository.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.multipart.MultipartFile;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.Session;
import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.rite.products.convertrite.exception.ValidationException;
import com.rite.products.convertrite.po.CloudDataProcessingReqPo;
import com.rite.products.convertrite.po.LoadMetaDataFromCloudRes;
import com.rite.products.convertrite.utils.DataSourceUtil;
import com.rite.products.convertrite.utils.Utils;

@Service
public class LoadMetaDataServiceImpl implements LoadMetaDataService {

	private static final Logger log = LoggerFactory.getLogger(LoadMetaDataServiceImpl.class);

	@Value("${file.local-dir}")
	private String localDir;

	@Value("${file.upload-dir}")
	private String remoteDir;

	@Value("${sftp.client.host}")
	private String remoteHost;

	@Value("${sftp.client.username}")
	private String username;
	@Value("${file-transfer}")
	private String fileTransfer;

	/*
	 * @Value("${sftp.client.password}") private String password;
	 */

	@Value("${clouddataprocess-url}")
	private String url;

	@Autowired
	XxrCloudDataProcessConfigRepository xxrCloudDataProcessConfigRepository;
	@Autowired
	ProcessSourceMetaDataRecordsDaoImpl createSourceRecordsDaoImpl;
	@Autowired
	ProcessCloudMetaDataRecordsDaoImpl processCloudMetaDataRecordsDaoImpl;
	@Autowired
	XxrLookUpValuesRepository xxrLookUpValuesRepository;
	@Autowired
	XxrLookupSetsRepository xxrLookupSetsRepository;
	@Autowired
	XxrBlobConvertriteRepository xxrBlobConvertriteRepository;

	@Autowired
	CrFileDetailsRepo crFileDetailsRepo;
	@Autowired
	Utils utils;
	@Autowired
	RestTemplate restTemplate;
	@Autowired
	DataSourceUtil dataSourceUtil;
	@Autowired
	XxrCloudTableRepository xxrCloudTableRepository;
	@Autowired
	XxrCloudDataProcessRepository xxrCloudDataProcessRepository;
	@Autowired
	XxrCloudConfigRepository xxrCloudConfigRepository;
	@Autowired
	XxrBlobConvertriteRepository xxBlobConvertriteRepository;

	@Override
	public String processSourceRecords(String fileName, String metaDataFileType, HttpServletRequest request)
			throws Exception {
		log.info("Start of processSourceRecords Method in Service class######");
		String status = "";
		try {
			status = createSourceRecordsDaoImpl.processSourceRecords(fileName, metaDataFileType, request);
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return status;
	}

	public void downloadSourceFile(PrintWriter writer, String objectCode, String metaDataFileType, String environment) throws Exception {
		log.info("Start of downloadSourceFile Method in Service class######");
		try {
			if (("TABLE").equalsIgnoreCase(metaDataFileType)) {
				InputStream inputStream = this.getClass().getClassLoader()
						.getResourceAsStream("Rite_Source_Table_Temp.csv");

				CSVReader reader = new CSVReader(new InputStreamReader(inputStream));
				List<String[]> csvBody = reader.readAll();
				// get CSV row column and replace with by using row and column
				for (int i = 1; i < csvBody.size(); i++) {
					csvBody.get(i)[0] = objectCode; // Target replacement
				}
				reader.close();

				// Write to CSV file which is open
				// ByteArrayOutputStream stream = new ByteArrayOutputStream();
				CSVWriter csvWriter = new CSVWriter(writer, CSVWriter.DEFAULT_SEPARATOR, CSVWriter.NO_QUOTE_CHARACTER,
						CSVWriter.DEFAULT_ESCAPE_CHARACTER, CSVWriter.DEFAULT_LINE_END);

				csvWriter.writeAll(csvBody);
				csvWriter.flush();
				csvWriter.close();

			} else if (("COLUMN").equalsIgnoreCase(metaDataFileType)) {
				InputStream inputStream =null;
				if(environment.equalsIgnoreCase("SOURCE")){
					 inputStream = this.getClass().getClassLoader()
							.getResourceAsStream("Rite_Source_MetaData_Template.csv");
				}else if(environment.equalsIgnoreCase("CLOUD")){
					 inputStream = this.getClass().getClassLoader()
							.getResourceAsStream("Rite_Cloud_MetaData_Template.csv");
				}


				CSVReader reader = new CSVReader(new InputStreamReader(inputStream));
				List<String[]> csvBody = reader.readAll();
				reader.close();

				// Write to CSV file which is open
				// ByteArrayOutputStream stream = new ByteArrayOutputStream();
				CSVWriter csvWriter = new CSVWriter(writer, CSVWriter.DEFAULT_SEPARATOR, CSVWriter.NO_QUOTE_CHARACTER,
						CSVWriter.DEFAULT_ESCAPE_CHARACTER, CSVWriter.DEFAULT_LINE_END);

				csvWriter.writeAll(csvBody);
				csvWriter.flush();
				csvWriter.close();

			}
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}

	}

	@Override
	public void downloadCloudFile(PrintWriter writer, String objectCode, String metaDataFileType) throws Exception {
		log.info("Start of downloadCloudFile Method in Service class######");
		InputStream inputStream = null;
		List<String[]> csvBody = new ArrayList<>();
		try {
			if (("TABLE").equalsIgnoreCase(metaDataFileType)) {
				inputStream = this.getClass().getClassLoader().getResourceAsStream("Rite_Cloud_Table_Template.csv");
				CSVReader reader = new CSVReader(new InputStreamReader(inputStream));
				csvBody = reader.readAll();
				// get CSV row column and replace with by using row and column
				for (int i = 1; i < csvBody.size(); i++) {
					csvBody.get(i)[0] = objectCode; // Target replacement
				}
				reader.close();

			} else if (("COLUMN").equalsIgnoreCase(metaDataFileType)) {
				inputStream = this.getClass().getClassLoader().getResourceAsStream("Rite_Cloud_MetaData_Template.csv");
				CSVReader reader = new CSVReader(new InputStreamReader(inputStream));
				csvBody = reader.readAll();
				// get CSV row column and replace with by using row and column
				for (int i = 1; i < csvBody.size(); i++) {
					csvBody.get(i)[1] = objectCode; // Target replacement
				}
				reader.close();

			}

			// Write to CSV file which is open
			// ByteArrayOutputStream stream = new ByteArrayOutputStream();
			CSVWriter csvWriter = new CSVWriter(writer, CSVWriter.DEFAULT_SEPARATOR, CSVWriter.NO_QUOTE_CHARACTER,
					CSVWriter.DEFAULT_ESCAPE_CHARACTER, CSVWriter.DEFAULT_LINE_END);

			csvWriter.writeAll(csvBody);
			csvWriter.flush();
			csvWriter.close();
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e.getMessage());
		}

	}

	@Override
	public String processCloudRecords(String fileName, String metaDataFileType, HttpServletRequest request)
			throws Exception {
		log.info("Start of processCloudRecords Method in Service class######");
		String status = "";
		try {
			status = processCloudMetaDataRecordsDaoImpl.processCloudRecords(fileName, metaDataFileType, request);
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return status;

	}

	public String uploadFile(MultipartFile file) throws Exception {
		log.info("Start of uploadFile Method in Service class######");
		//XxrBlobConvertrite xxrBlobConvertrite = new XxrBlobConvertrite();
		String fileName =null;
		try {

		CrFileDetails crFileDetails=new CrFileDetails();
		UUID uuid = UUID.randomUUID();
		 fileName = org.apache.commons.io.FilenameUtils.getName(file.getOriginalFilename());
		String[] fileNameArr = fileName.split("\\.");
		String result = new BufferedReader(new InputStreamReader(file.getInputStream())).lines()
				.collect(Collectors.joining("\n"));
		//xxrBlobConvertrite = xxrBlobConvertriteRepository.findByBlobName(fileName);
		crFileDetails=crFileDetailsRepo.findByFileName(fileName);
		System.out.println(fileName+"crFileDetails-->"+crFileDetails);

		if (crFileDetails != null) {
			System.out.println("if-->"+fileName);
			crFileDetails.setFileContent(result);
			crFileDetailsRepo.save(crFileDetails);
		} else {
			System.out.println("else-->"+fileName);
			CrFileDetails crFile=new CrFileDetails();
			crFile.setFileContent(result);
			crFile.setFileName(fileName);
			crFile.setFileType(fileNameArr[1]);
			crFile.setCreatedBy("ConvertRiteAdmin");
			crFile.setCreateDate(new java.sql.Date(new java.util.Date().getTime()));
			crFileDetailsRepo.save(crFile);
		}

		}catch (Exception e){
			e.printStackTrace();
		}

		return fileName;
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
	public LoadMetaDataFromCloudRes loadMetaDataFromCloud(String objectCode, String metaDataTableName, Long podId,
			Long projectId, HttpServletRequest request) throws ValidationException, Exception {
		log.info("Start of loadMetaDataFromCloud Method in Service class######");
		Connection con = null;
		Long metaDataTableId = null;
		LoadMetaDataFromCloudRes loadMetaDataFromCloudRes = new LoadMetaDataFromCloudRes();
		String applicationTableName = "";
		try {
			// String applicationTableName = utils.getApplicationTableName(objectCode);

			applicationTableName = utils.getInterfaceTableName(objectCode);

			Optional<XxrCloudDataProcessConfig> cloudAcessConfig = xxrCloudDataProcessConfigRepository
					.findByPodIdAndProjectId(podId, projectId);
			if (!cloudAcessConfig.isPresent())
				throw new ValidationException("Please define CloudAccessConfig details for pod and project fields");
			String sqlQuery = "select fc.COLUMN_NAME,fc.PHYSICAL_COLUMN_NAME,fc.USER_COLUMN_NAME,fc.STATUS,fc.SHORT_NAME,fc.COLUMN_SEQUENCE,fc.COLUMN_TYPE,fc.WIDTH,fc.NULL_ALLOWED_FLAG,fc.TRANSLATE_FLAG,fc.PRECISION,fc.SCALE,fc.DOMAIN_CODE,fc.DENORM_PATH,fc.ROUTING_MODE,fc.VERSION_COLUMN,fc.ELIGIBLE_TO_BE_SECURED,fc.SECURITY_CLASSIFICATION,fc.SEC_CLASSIFICATION_OVERRIDE,fc.DESCRIPTION from fnd_columns fc,fnd_tables ft where ft.table_id=fc.table_id and ft.table_name='"
					+ applicationTableName + "'";

			XxrCloudDataProcess cloudDataProce = xxrCloudDataProcessRepository.findAll().stream()
					.filter(x -> x.getStatus().equalsIgnoreCase("Starting"))
					.max(Comparator.comparing(XxrCloudDataProcess::getPriority)).orElse(null);

			CloudDataProcessingReqPo cloudDataProcessingReqPo = new CloudDataProcessingReqPo();
			cloudDataProcessingReqPo.setDescription("ConvertRite");
			cloudDataProcessingReqPo.setLookUpFlag("N");
			cloudDataProcessingReqPo.setScheduledJobCall("N");
			cloudDataProcessingReqPo.setPodId(podId);
			cloudDataProcessingReqPo.setProjectId(projectId);
			cloudDataProcessingReqPo.setSqlQuery(sqlQuery);
			if (cloudDataProce != null)
				cloudDataProcessingReqPo.setPriority(cloudDataProce.getPriority() + 1);
			else
				cloudDataProcessingReqPo.setPriority(1);

//			log.info("priority::::::" + cloudDataProcessingReqPo.getPriority());

			HttpHeaders headers = new HttpHeaders();
			headers.set("Accept", MediaType.APPLICATION_JSON_VALUE);
			headers.setContentType(MediaType.APPLICATION_JSON);
			headers.set("Authorization", request.getHeader("Authorization"));
			HttpEntity<CloudDataProcessingReqPo> requestEntity = new HttpEntity<>(cloudDataProcessingReqPo, headers);
			// System.out.println(String.format(url, parametrizedArgs));

			ResponseEntity<?> cloudDataApiResponse = restTemplate.exchange(url, HttpMethod.POST, requestEntity,
					XxrCloudDataProcess.class);
			// create connection
			con = dataSourceUtil.createConnection();
			XxrCloudDataProcess val = null;
			if (cloudDataApiResponse.getStatusCode() == HttpStatus.OK) {
				val = (XxrCloudDataProcess) cloudDataApiResponse.getBody();
			}
			if (val != null) {
				String requestId = val.getRequestId();
				log.info(requestId + "::::::::requestId");
				getStatus(con, requestId);
				metaDataTableId = xxrCloudTableRepository.getTableId(metaDataTableName);
				if (metaDataTableId != null)
					throw new ValidationException("This MetaDataTableName Already exists,Please give different name");
				// Inserting MetaData into xxr_cloud_tables
				metaDataTableName = insertTableMetaData(objectCode, con, metaDataTableName,"T");
				log.debug("metaDataTableName::" + metaDataTableName);
				metaDataTableId = xxrCloudTableRepository.getTableId(metaDataTableName);
				log.info("metaDataTableId:::::" + metaDataTableId);
				insertColumnMetaData(requestId, metaDataTableId, con, objectCode);
				loadMetaDataFromCloudRes.setMessage("Successfully loaded MetaData from cloud");
				loadMetaDataFromCloudRes.setTableName(metaDataTableName);
				loadMetaDataFromCloudRes.setTableId(metaDataTableId);
			}
		} catch (ValidationException e) {
			throw new ValidationException(e.getMessage());
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		} finally {
			if (con != null)
				con.close();
		}
		return loadMetaDataFromCloudRes;
	}

	/*
	 * @Override public LoadMetaDataFromCloudRes loadMetaDataFromCloud(String
	 * objectCode, String metaDataTableName, String requestId) throws Exception { //
	 * TODO Auto-generated method stub Connection con = null; Long metaDataTableId =
	 * null; LoadMetaDataFromCloudRes loadMetaDataFromCloudRes = new
	 * LoadMetaDataFromCloudRes(); try {
	 * 
	 * // Create Connection con = dataSourceUtil.createConnection(); // To insert
	 * metadata of table into xxr_source_tables metaDataTableName =
	 * insertTableMetaData(objectCode, con, metaDataTableName);
	 * log.debug("metaDataTableName::" + metaDataTableName); metaDataTableId =
	 * xxrCloudTableRepository.getTableId(metaDataTableName);
	 * log.info("metaDataTableId:::::" + metaDataTableId);
	 * insertColumnMetaData(requestId, metaDataTableId, con, objectCode);
	 * loadMetaDataFromCloudRes.setMessage("Successfully loaded MetaData from cloud"
	 * ); loadMetaDataFromCloudRes.setTableName(metaDataTableName);
	 * loadMetaDataFromCloudRes.setTableId(metaDataTableId); } catch (Exception e) {
	 * throw new Exception(e.getMessage()); } return loadMetaDataFromCloudRes; }
	 */

	private String insertTableMetaData(String objectCode, Connection con, String metaDataTableName,String tableType) throws Exception {
		PreparedStatement insertStmnt = null;
		int count = 0;
		// String metaDataTable = "";
		try {
			// metaDataTable = sourceMetaDataTableName(objectCode);
			Long lookupSetId = xxrLookupSetsRepository.getLookupSetId("Object Code");
			Long objectId = xxrLookUpValuesRepository.getIdByValuesetId(objectCode, lookupSetId);
			log.info("objectId:::::::" + objectId);
			StringBuffer sb = new StringBuffer(
					"insert into xxr_cloud_tables(table_id,table_name,physical_table_name,user_table_name,description,application_short_name,table_type,hosted_support_style,logical,mls_support_model,status,deploy_to,extension_of_table,short_name,shared_object,conflict_resolution,tablespace_type,select_allowed,insert_allowed,update_allowed,delete_allowed,truncate_allowed,maintain_partition,exchange_partition,maintain_index,flashback_allowed,enable_audit,ora_edition_context,object_id,attribute1,attribute2,attribute3,attribute4,attribute5,last_update_date,last_updated_by,creation_date,created_by) values (xxr_cld_table_id_s.nextval,");
			sb.append("'" + metaDataTableName + "','" + metaDataTableName + "','" + metaDataTableName + "','"
					+ objectCode
					+ "','CONVRITE','"+tableType+"',null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,"
					+ objectId + ",null,null,null,null,null,sysdate,'CONVRITE',sysdate,'CONVRITE')");
//			log.info("sqlquery:::::::" + sb.toString());
			insertStmnt = con.prepareStatement(sb.toString());
			count = insertStmnt.executeUpdate();
			log.info("count:::::" + count);
			insertStmnt.close();
		} catch (Exception e) {
			// e.printStackTrace();
			throw new Exception(e.getMessage());
		}
		return metaDataTableName;
	}

	private void getStatus(Connection con, String requestId) throws Exception {
		String query = "SELECT * FROM XXR_CLOUD_DATA_PROCESS C WHERE C.request_id ='" + requestId + "'";
		while (true) {
			ResultSet rs = null;
			PreparedStatement selectStmnt = null;
			try {
				selectStmnt = con.prepareStatement(query);
				rs = selectStmnt.executeQuery();
				String status = "";
				if (rs.next())
					status = rs.getString("STATUS");
				// XxrCloudDataProcess cloudDataProcessReq =
				// xxrCloudDataProcessRepository.findByrequestId(requestId);

				log.info(status + "########status" + "priority::::" + rs.getString("PRIORITY"));
				// if (status.equalsIgnoreCase("processing") ||
				// status.equalsIgnoreCase("starting"))
				// throw new ValidationException("Reconcile is still in process please wait for
				// sometime");
				if (status.equalsIgnoreCase("error"))
					throw new ValidationException(
							"Something went wrong while loading cloud metadata,Please contact system administrator");
				else if (status.equalsIgnoreCase("completed")) {
//					log.info("entering:::::::");
					break;
				}
			} catch (Exception e) {
				throw new Exception(e.getMessage());
			} finally {
				if (rs != null)
					rs.close();
				if (selectStmnt != null)
					selectStmnt.close();
			}
		}
	}

	private void insertColumnMetaData(String requestId, Long metaDataTableId, Connection con, String objectCode)
			throws Exception {
		PreparedStatement insertColumnsStmnt = null;
		int count = 0;
		XxrBlobConvertrite res = new XxrBlobConvertrite();
		List<String[]> readr = new ArrayList<>();
		try {
			StringBuffer sb = new StringBuffer("insert all ");
			Optional<XxrBlobConvertrite> lobResp = xxBlobConvertriteRepository.findById(requestId);
			if (lobResp.isPresent()) {
				res = lobResp.get();
				String lob = res.getBlob();
				String[] lineArr = lob.split("\n");
				for (String line : lineArr) {
					readr.add(line.split(","));
				}
			}
			Long lookupSetId = xxrLookupSetsRepository.getLookupSetId("Object Code");
			Long objectId = xxrLookUpValuesRepository.getIdByValuesetId(objectCode, lookupSetId);
			log.info("objectId:::::::" + objectId);
			for (int i = 1; i < readr.size(); i++) {
				String[] str = readr.get(i);
				str[19] = str[19].replaceAll("\\W", " ");
				sb.append(
						" into xxr_cloud_columns (column_id,column_name,physical_column_name,user_column_name,description,table_id,status,short_name,ora_edition_context,column_sequence,column_type,width,null_allowed_flag,translate_flag,precision,scale,domain_code,denorm_path,routing_mode,version_column,eligible_to_be_secured,security_classification,sec_classification_override,object_id,attribute1,attribute2,attribute3,attribute4,attribute5,last_update_date,last_updated_by,creation_date,created_by) values ("
								+ i + ",'" + str[0] + "','" + str[0] + "','" + str[2] + "','" + str[19] + "',"
								+ metaDataTableId + ",'" + str[3] + "','" + str[4] + "',null,'" + str[5] + "','"
								+ str[6] + "','" + str[7] + "','" + str[8] + "','" + str[9] + "','" + str[10] + "','"
								+ str[11] + "','" + str[12] + "','" + str[13] + "','" + str[14] + "','" + str[15]
								+ "','" + str[16] + "','" + str[17] + "','" + str[18] + "'," + objectId
								+ ",null,null,null,null,null,sysdate,'CONVRITE',sysdate,'CONVRITE')");

			}
			sb.append("SELECT 1 FROM DUAL");
//			log.info("sqlquery:::" + sb.toString());
			insertColumnsStmnt = con.prepareStatement(sb.toString());
			count = insertColumnsStmnt.executeUpdate();
			log.info("No of meta data columns inserted::" + count);
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e.getMessage());
		}
	}

	private void insertColumnMetaDataFromHdl(String fileName, Long metaDataTableId, Connection con, String objectCode)
			throws Exception {
		PreparedStatement insertColumnsStmnt = null;
		int count = 0;
		try {
			StringBuffer sb = new StringBuffer("insert all ");

			/*
			 * ChannelSftp channelSftp = utils.setupJsch(); channelSftp.connect();
			 * channelSftp.cd(remoteDir); BufferedReader br = new BufferedReader(new
			 * InputStreamReader(channelSftp.get(fileName)));
			 */

			XxrBlobConvertrite lobResp = xxBlobConvertriteRepository.findByBlobName(fileName);

			String lob = lobResp.getBlob();
			String[] lineArr = lob.split("\n");

			String columnString = "";
			String objectCde = objectCode.replaceAll("\\s+", "");
			for (String line : lineArr) {
				if (line.contains("METADATA|" + objectCde)) {
					columnString = line;
					break;
				}
			}
//			log.info(columnString + "#####columnString");
			String[] columnNamesArr = columnString.split("\\|");
			Long lookupSetId = xxrLookupSetsRepository.getLookupSetId("Object Code");
			Long objectId = xxrLookUpValuesRepository.getIdByValuesetId(objectCode, lookupSetId);
			log.info("objectId:::::::" + objectId);
			int j = 1;
			for (int i = 0; i < columnNamesArr.length; i++) {
				/*
				 * if(i==1) continue;
				 */
				String columnType = "";
				int columnWidth = 0;
				String columnName = columnNamesArr[i];

				if (columnName.contains("(")) {
					int index = columnName.indexOf("(");
					columnName = columnName.substring(0, index);
				} else if (columnName.contains("=<")) {
					int index = columnName.indexOf("=<");
					columnName = columnName.substring(0, index);
				} else if (columnName.contains("FLEX:")) {
					columnName = columnName.substring(5);
				}
				if (columnName.substring(0, 1).equalsIgnoreCase("_"))
					columnName = columnName.substring(1);
				log.info(columnName + "#####columnName");
				if (columnName.toLowerCase().contains("date")) {
//					log.info("#####columnName enterinngiifff");
					columnType = "D";
				} else {
					columnType = "V";
					columnWidth = 2000;
				}
				columnName = columnName.toUpperCase();
				sb.append(
						" into xxr_cloud_columns (column_id,column_name,physical_column_name,user_column_name,description,table_id,status,short_name,ora_edition_context,column_sequence,column_type,width,null_allowed_flag,translate_flag,precision,scale,domain_code,denorm_path,routing_mode,version_column,eligible_to_be_secured,security_classification,sec_classification_override,object_id,attribute1,attribute2,attribute3,attribute4,attribute5,last_update_date,last_updated_by,creation_date,created_by) values ("
								+ j + ",'" + columnName + "','" + columnName + "','" + columnNamesArr[i] + "','"
								+ columnName + "'," + metaDataTableId + ",null,'INTERNAL',null," + j + ",'" + columnType
								+ "'," + columnWidth + ",'Y','N',null,null,null,null,null,null,null,null,null,"
								+ objectId + ",null,null,null,null,null,sysdate,'CONVRITE',sysdate,'CONVRITE')");
				j++;
			}
			sb.append("SELECT 1 FROM DUAL");
//			log.info("sqlquery:::" + sb.toString());
			insertColumnsStmnt = con.prepareStatement(sb.toString());
			count = insertColumnsStmnt.executeUpdate();
			log.info("No of meta data columns inserted::" + count);
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e.getMessage());
		}
	}

	@Override
	public LoadMetaDataFromCloudRes loadHdlCloudMetaData(String objectCode, String metaDataTableName,
			MultipartFile file) throws Exception {
		log.info("Start of loadHdlCloudMetaData Method in Service###");
		Connection con = null;
		Long metaDataTableId = null;
		String fileName = "";
		LoadMetaDataFromCloudRes loadMetaDataFromCloudRes = new LoadMetaDataFromCloudRes();
		try {
			// upload file to ftp path
			fileName = uploadFile(file);
			// Create Connection
			con = dataSourceUtil.createConnection();
			metaDataTableId = xxrCloudTableRepository.getTableId(metaDataTableName);
			if (metaDataTableId != null)
				throw new ValidationException("This MetaDataTableName Already exists,Please give different name");
			// Inserting MetaData into xxr_cloud_tables
			metaDataTableName = insertTableMetaData(objectCode, con, metaDataTableName,"T");
			log.debug("metaDataTableName::" + metaDataTableName);
			metaDataTableId = xxrCloudTableRepository.getTableId(metaDataTableName);
			log.info("metaDataTableId:::::" + metaDataTableId);
			// Inserting MetaData columns into xxr_cloud_columns
			insertColumnMetaDataFromHdl(fileName, metaDataTableId, con, objectCode);
			loadMetaDataFromCloudRes.setTableId(metaDataTableId);
			loadMetaDataFromCloudRes.setTableName(metaDataTableName);
			loadMetaDataFromCloudRes.setMessage("Successfully loaded hdl cloud metadata");

		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return loadMetaDataFromCloudRes;
	}

	@Override
	public String uploadFileToFtp(MultipartFile file) throws Exception {
		log.info("Start of uploadFileToFtp in service ####");
		String fileName = StringUtils.cleanPath(Objects.requireNonNull(file.getOriginalFilename()));
		ChannelSftp channelSftp = null;
		Session jschSession = null;
		try {
			if("SFTP".equalsIgnoreCase(fileTransfer)) {
				jschSession = utils.setupJschSession();
				channelSftp = (ChannelSftp) jschSession.openChannel("sftp");
				channelSftp.connect();
				channelSftp.cd(remoteDir);
				channelSftp.put(file.getInputStream(), fileName);
			}
			else if("NFS".equalsIgnoreCase(fileTransfer)){
				log.info(remoteDir + fileName);
				File destinationFile = new File(remoteDir + fileName);
				// Copy the file to the server folder
				file.transferTo(destinationFile);
			}
		}catch(Exception e){
			log.error(e.getMessage());
		}
			finally {
			if (channelSftp != null) {
				channelSftp.exit();
				channelSftp.disconnect();
			}
			if (jschSession != null)
				jschSession.disconnect();
		}
//		log.info("End of uploadFileToFtp in service ####");
		return fileName;
	}

	@Transactional
	@Override
	public LoadMetaDataFromCloudRes loadRejectionMetaDataFromCloud(String objectCode, Long podId, Long projectId,
			HttpServletRequest request,String metaDataTableName) throws Exception {
		log.info("Start of loadRejectionMetaDataFromCloud Method in Service class######");
		Connection con = null;
		Long metaDataTableId = null;
		LoadMetaDataFromCloudRes loadMetaDataFromCloudRes = new LoadMetaDataFromCloudRes();
		try {
			// String applicationTableName = utils.getApplicationTableName(objectCode);

			XxrCloudConfig xxrCloudConfig = xxrCloudConfigRepository.findByObjectCode(objectCode);
			if (xxrCloudConfig == null)
				throw new ValidationException(
						"For " + objectCode + " Cloud Configuration is not defined,Please help to define");
			
			String rejectionTableName=xxrCloudConfig.getRejectionTableName();

			Optional<XxrCloudDataProcessConfig> cloudAcessConfig = xxrCloudDataProcessConfigRepository
					.findByPodIdAndProjectId(podId, projectId);
			if (!cloudAcessConfig.isPresent())
				throw new ValidationException("Please define CloudAccessConfig details for pod and project fields");
			String sqlQuery = "select fc.COLUMN_NAME,fc.PHYSICAL_COLUMN_NAME,fc.USER_COLUMN_NAME,fc.STATUS,fc.SHORT_NAME,fc.COLUMN_SEQUENCE,fc.COLUMN_TYPE,fc.WIDTH,fc.NULL_ALLOWED_FLAG,fc.TRANSLATE_FLAG,fc.PRECISION,fc.SCALE,fc.DOMAIN_CODE,fc.DENORM_PATH,fc.ROUTING_MODE,fc.VERSION_COLUMN,fc.ELIGIBLE_TO_BE_SECURED,fc.SECURITY_CLASSIFICATION,fc.SEC_CLASSIFICATION_OVERRIDE,fc.DESCRIPTION from fnd_columns fc,fnd_tables ft where ft.table_id=fc.table_id and ft.table_name='"
					+ rejectionTableName + "'";

			XxrCloudDataProcess cloudDataProce = xxrCloudDataProcessRepository.findAll().stream()
					.filter(x -> x.getStatus().equalsIgnoreCase("Starting"))
					.max(Comparator.comparing(XxrCloudDataProcess::getPriority)).orElse(null);

			CloudDataProcessingReqPo cloudDataProcessingReqPo = new CloudDataProcessingReqPo();
			cloudDataProcessingReqPo.setDescription("ConvertRite");
			cloudDataProcessingReqPo.setLookUpFlag("N");
			cloudDataProcessingReqPo.setScheduledJobCall("N");
			cloudDataProcessingReqPo.setPodId(podId);
			cloudDataProcessingReqPo.setProjectId(projectId);
			cloudDataProcessingReqPo.setSqlQuery(sqlQuery);
			if (cloudDataProce != null)
				cloudDataProcessingReqPo.setPriority(cloudDataProce.getPriority() + 1);
			else
				cloudDataProcessingReqPo.setPriority(1);

//			log.info("priority::::::" + cloudDataProcessingReqPo.getPriority());

			HttpHeaders headers = new HttpHeaders();
			headers.set("Accept", MediaType.APPLICATION_JSON_VALUE);
			headers.setContentType(MediaType.APPLICATION_JSON);
			headers.set("Authorization", request.getHeader("Authorization"));
			HttpEntity<CloudDataProcessingReqPo> requestEntity = new HttpEntity<>(cloudDataProcessingReqPo, headers);
			// System.out.println(String.format(url, parametrizedArgs));

			ResponseEntity<?> cloudDataApiResponse = restTemplate.exchange(url, HttpMethod.POST, requestEntity,
					XxrCloudDataProcess.class);
			// create connection
			con = dataSourceUtil.createConnection();
			XxrCloudDataProcess val = null;
			if (cloudDataApiResponse.getStatusCode() == HttpStatus.OK) {
				val = (XxrCloudDataProcess) cloudDataApiResponse.getBody();
			}
			if (val != null) {
				String requestId = val.getRequestId();
				log.info(requestId + "::::::::requestId");
				getStatus(con, requestId);
				metaDataTableId = xxrCloudTableRepository.getTableId(metaDataTableName);
				if (metaDataTableId != null)
					throw new ValidationException("This MetaDataTableName Already exists,Please give different name");
				// Inserting MetaData into xxr_cloud_tables
				metaDataTableName = insertTableMetaData(objectCode, con, metaDataTableName,"R");
				log.debug("metaDataTableName::" + metaDataTableName);
				metaDataTableId = xxrCloudTableRepository.getTableId(metaDataTableName);
				log.info("metaDataTableId:::::" + metaDataTableId);
				insertColumnMetaData(requestId, metaDataTableId, con, objectCode);
				loadMetaDataFromCloudRes.setMessage("Successfully loaded MetaData from cloud");
				loadMetaDataFromCloudRes.setTableName(metaDataTableName);
				loadMetaDataFromCloudRes.setTableId(metaDataTableId);
			}
		} catch (ValidationException e) {
			throw new ValidationException(e.getMessage());
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		} finally {
			if (con != null)
				con.close();
		}
		return loadMetaDataFromCloudRes;
	}
		
	

}
