package com.rite.products.convertrite.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.opencsv.CSVWriter;
import com.rite.products.convertrite.exception.ValidationException;
import com.rite.products.convertrite.hcm.stubs.GenericSoapServiceStub;
import com.rite.products.convertrite.hcm.stubs.HCMDataLoaderStub;
import com.rite.products.convertrite.model.*;
import com.rite.products.convertrite.multitenancy.config.tenant.hibernate.DynamicDataSourceBasedMultiTenantConnectionProvider;
import com.rite.products.convertrite.po.CloudDataProcessingReqPo;
import com.rite.products.convertrite.po.CrObjectInformationPo;
import com.rite.products.convertrite.po.CrCloudDataProcessResPo;
import com.rite.products.convertrite.po.CrCloudReqPo;
import com.rite.products.convertrite.po.MetaDataColumnsPo;
import com.rite.products.convertrite.respository.XxrCloudColumnsRepository;
import com.rite.products.convertrite.respository.XxrCloudConfigRepository;
import com.rite.products.convertrite.respository.XxrCloudDataProcessConfigRepository;
import org.apache.axis2.client.ServiceClient;
import org.apache.axis2.client.Stub;
import org.apache.axis2.transport.http.impl.httpclient4.HttpTransportPropertiesImpl;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import javax.servlet.http.HttpServletRequest;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.util.*;
import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import javax.persistence.Column;
@Component
public class Utils {

	@Value("${cloud-status-check}")
	private String statusUrl;
	@Value("${clouddataprocess-url}")
	private String cloudDataProcessUrl;
	@Value("${file.local-dir}")
	private String localDir;

	@Value("${file.upload-dir}")
	private String remoteDir;

	@Value("${sftp.client.host}")
	private String remoteHost;

	@Value("${sftp.client.username}")
	private String username;
	//@Value("${sftp.privatekey.path}")
	private String sftpPrivateKeyPath;

	@Value("${convertrite-admin-host}")
	String convertriteAdminHost;

	@Value("${sftp.client.password}")
	private String password;


	@Value("${clouddataprocess-url}")
	private String url;

	@Value("${hcm_soapurl}")
	private String hcmUrl;
	@Value("${generic_soapurl}")
	private String genericSoapUrl;
	@Autowired
	DataSourceUtil dataSourceUtil;
	@Autowired
	XxrCloudConfigRepository xxrCloudConfigRepository;
	@Autowired
	RestTemplate restTemplate;
	@Autowired
	XxrCloudColumnsRepository xxrCloudColumnsRepository;
	@Autowired
	XxrCloudDataProcessConfigRepository xxrCloudDataProcessConfigRepository;

	@Autowired
	DynamicDataSourceBasedMultiTenantConnectionProvider dynamicDataSourceBasedMultiTenantConnectionProvider;
	private static final Logger log = LoggerFactory.getLogger(Utils.class);

	// private static final String xxrSourceColumnsRepository = null;

	public static void zipFile(String filePath, String fileName, String csvName) throws Exception {
		log.info("Start of zipFile Method######");
		String path = filePath + File.separator + csvName;
		try {
			File file = new File(path);
			log.info("path::::" + path);
			String zipPath = filePath + File.separator + fileName + ".zip";
			log.info("zipPath---->" + zipPath);
			FileOutputStream fos = new FileOutputStream(zipPath);
			ZipOutputStream zos = new ZipOutputStream(fos);

			zos.putNextEntry(new ZipEntry(file.getName()));

			byte[] bytes = Files.readAllBytes(Paths.get(path));

			log.debug(bytes.length + "#######Length");
			zos.write(bytes, 0, bytes.length);
			zos.closeEntry();
			zos.close();
			// fis.close();
		} catch (FileNotFoundException ex) {
			log.error("The file %s does not exist", filePath);
			throw new Exception(ex.getMessage());
		} catch (IOException ex) {
			log.error("I/O error: " + ex);
			throw new Exception(ex.getMessage());
		} finally {
			Files.deleteIfExists(Paths.get(path));

		}
	}

	public static String zipMultipleFiles(String filePath, List<String> fileNameLi, Map<String, String> map)
			throws Exception {
		ZipOutputStream zos = null;
		String zipPath = null;
		try {
			zipPath = filePath + File.separator + fileNameLi.get(0) + ".zip";
			log.info(zipPath);
			FileOutputStream fos = new FileOutputStream(zipPath);
			zos = new ZipOutputStream(fos);
			Set<String> set=map.keySet();

			for(String key:set){
				String csvFileName =  key + ".csv";
				ZipEntry entry = new ZipEntry(csvFileName); // create a zip entry and add it to ZipOutputStream
				zos.putNextEntry(entry);
				CSVWriter writer = new CSVWriter(new OutputStreamWriter(zos), CSVWriter.DEFAULT_SEPARATOR,
						CSVWriter.NO_QUOTE_CHARACTER, CSVWriter.NO_ESCAPE_CHARACTER, CSVWriter.DEFAULT_LINE_END);

				writer.writeNext(map.get(key).split(",")); // write the contents
				writer.flush(); // flush the writer. Very important!
				zos.closeEntry(); // close the entry. Note : we are not closing the zos just yet as we need to add
				// more files to our ZIP
			}
		} catch (Exception e) {
			e.printStackTrace();
			log.error("Error in zipMultipleFiles--->"+e.getMessage());
			throw new Exception(e.getMessage());
		} finally {
			zos.close();
		}
		return zipPath;
	}

	public static String clobToString(Clob data) throws Exception {
		log.info("Start of clobToString Method######");
		StringBuilder sb = new StringBuilder();
		try {
			Reader reader = data.getCharacterStream();
			BufferedReader br = new BufferedReader(reader);

			String line;
			while (null != (line = br.readLine())) {
				log.debug(line + "::::::line");
				sb.append(line);
				sb.append("\n");
			}
			br.close();
		} catch (SQLException e) {
			throw new Exception(e.getMessage());
		} catch (IOException e) {
			throw new Exception(e.getMessage());
		}
		return sb.toString();
	}

	public static String removeSpace(String objectName) {
		String object = objectName.replaceAll("\\s","");
		return object;
	}

	public static String replaceLastComma(String str) {
		int lastIndex = str.lastIndexOf(",");
		if (lastIndex != -1) {
			return str.substring(0, lastIndex) + str.substring(lastIndex + 1);
		}
		return str;
	}

	public XxrCloudDataProcess cldDataProcess(String sqlQuery, XxrCloudTemplateHeader cloudTempHdr, String desc,
											  HttpServletRequest request) throws Exception {
		XxrCloudDataProcess xxrCloudDataProcess = null;
		try {
			CloudDataProcessingReqPo cloudDataProcessingReqPo = new CloudDataProcessingReqPo();
			cloudDataProcessingReqPo.setDescription(desc);
			cloudDataProcessingReqPo.setLookUpFlag("N");
			cloudDataProcessingReqPo.setScheduledJobCall("N");
			cloudDataProcessingReqPo.setSqlQuery(sqlQuery);
			// cloudDataProcessingReqPo.setPodId(cloudTempHdr.getPodId());
			cloudDataProcessingReqPo.setProjectId(cloudTempHdr.getProjectId());

			HttpHeaders headers = new HttpHeaders();
			headers.set("Accept", MediaType.APPLICATION_JSON_VALUE);
			headers.setContentType(MediaType.APPLICATION_JSON);
			headers.set("Authorization", request.getHeader("Authorization"));
			HttpEntity<CloudDataProcessingReqPo> requestEntity = new HttpEntity<>(cloudDataProcessingReqPo, headers);

			ResponseEntity<?> cloudDataApiResponse = restTemplate.exchange(url, HttpMethod.POST, requestEntity,
					XxrCloudDataProcess.class);
			if (cloudDataApiResponse.getStatusCode() == HttpStatus.OK)
				xxrCloudDataProcess = (XxrCloudDataProcess) cloudDataApiResponse.getBody();
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return xxrCloudDataProcess;
	}

	public XxrCloudDataProcess cldDataProcess(String sqlQuery, String desc, HttpServletRequest request, Long projectId)
			throws Exception {
		XxrCloudDataProcess xxrCloudDataProcess = null;
		try {
			CloudDataProcessingReqPo cloudDataProcessingReqPo = new CloudDataProcessingReqPo();
			cloudDataProcessingReqPo.setDescription(desc);
			cloudDataProcessingReqPo.setLookUpFlag("N");
			cloudDataProcessingReqPo.setScheduledJobCall("N");
			cloudDataProcessingReqPo.setSqlQuery(sqlQuery);
			// cloudDataProcessingReqPo.setPodId(podId);
			cloudDataProcessingReqPo.setProjectId(projectId);

			HttpHeaders headers = new HttpHeaders();
			headers.set("Accept", MediaType.APPLICATION_JSON_VALUE);
			headers.setContentType(MediaType.APPLICATION_JSON);
			headers.set("Authorization", request.getHeader("Authorization"));
			HttpEntity<CloudDataProcessingReqPo> requestEntity = new HttpEntity<>(cloudDataProcessingReqPo, headers);

			ResponseEntity<?> cloudDataApiResponse = restTemplate.exchange(url, HttpMethod.POST, requestEntity,
					XxrCloudDataProcess.class);
			if (cloudDataApiResponse.getStatusCode() == HttpStatus.OK)
				xxrCloudDataProcess = (XxrCloudDataProcess) cloudDataApiResponse.getBody();
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return xxrCloudDataProcess;
	}

	public String getCloudUrl(XxrCldTempHdrsView xxrCldTempHdrsView) throws Exception {

		String cloudUrl = "";
		try {
//xxrCloudDataProcessConfigRepository -- CrPodCloudConfig
			XxrCloudDataProcessConfig cloudAcessConfig = xxrCloudDataProcessConfigRepository
					.findFirstByOrderByCreationDateDesc();

			cloudUrl = cloudAcessConfig.getCloudUrl();

		} catch (Exception e) {
			// e.printStackTrace();
			throw new Exception(e.getMessage());
		}
		return cloudUrl;
	}

	public void basicAuthentication(ServiceClient client, XxrCldTempHdrsView crCldTempHdrsView) {
	}

	public void basicAuthentication1(ServiceClient client, CrCloudTemplateHeadersView crCldTempHdrsView)
			throws ValidationException, Exception {
		log.info("Start of basicAuthentication Method######");
		try {
			String userName = "";
			String password = "";
			String cloudUrl = "";
			//xxrCloudDataProcessConfigRepository-- CrPodCloudConfig
			XxrCloudDataProcessConfig cloudDataProcessConfig = xxrCloudDataProcessConfigRepository
					.findFirstByOrderByCreationDateDesc();

			cloudUrl = cloudDataProcessConfig.getCloudUrl();
			userName = cloudDataProcessConfig.getUserName();
			password = cloudDataProcessConfig.getPassword();

			HttpTransportPropertiesImpl.Authenticator auth = new HttpTransportPropertiesImpl.Authenticator();
			auth.setUsername(userName);//lisa.jones
			auth.setPassword(password);
			client.getOptions().setProperty(org.apache.axis2.transport.http.HTTPConstants.AUTHENTICATE, auth);
			client.getOptions().setProperty(org.apache.axis2.transport.http.HTTPConstants.CHUNKED, false);

		} catch (Exception e) {
			// e.printStackTrace();
			throw new Exception(e.getMessage());
		}
	}

	public String getInterfaceTableName(String objectCode) throws ValidationException {
		String applicationTableName = "";
		XxrCloudConfig xxrCloudConfig = xxrCloudConfigRepository.findByObjectCode(objectCode);
		if (xxrCloudConfig == null)
			throw new ValidationException(
					"For " + objectCode + " Cloud Configuration is not defined,Please help to define");
		else
			applicationTableName = xxrCloudConfig.getInterfaceTableName();
		return applicationTableName;
	}

	public String getApplicationTableName(String objectCode) {

		String applicationTableName = "";
		Map<String, String> objectCodeApplicationTableMap = new HashMap<>();

		objectCodeApplicationTableMap.put("supplier", "POZ_SUPPLIERS_INT");
		objectCodeApplicationTableMap.put("sites", "POZ_SUPPLIER_SITES_INT");
		objectCodeApplicationTableMap.put("site assignments", "POZ_SITE_ASSIGNMENTS_INT");
		objectCodeApplicationTableMap.put("product and services", "POZ_SUP_PROD_SERV_INT");
		objectCodeApplicationTableMap.put("contacts", "POZ_SUP_CONTACTS_INT");
		objectCodeApplicationTableMap.put("contact address", "POZ_SUP_CONTACT_ADDRESSES_INT");
		objectCodeApplicationTableMap.put("business classifications", "POZ_SUP_BUS_CLASS_INT");
		objectCodeApplicationTableMap.put("attachments", "POZ_SUP_ATTACHMENTS_INT");
		objectCodeApplicationTableMap.put("site attachment", "POZ_SUP_ATTACHMENTS_INT");
		objectCodeApplicationTableMap.put("business classification attachment", "POZ_SUP_ATTACHMENTS_INT");
		objectCodeApplicationTableMap.put("address", "POZ_SUP_ADDRESSES_INT");
		objectCodeApplicationTableMap.put("third party", "POZ_SUP_THIRDPARTY_INT");
		objectCodeApplicationTableMap.put("iby temp ext bank accts", "IBY_TEMP_EXT_BANK_ACCTS");
		objectCodeApplicationTableMap.put("iby temp ext payees", "IBY_TEMP_EXT_PAYEES");
		objectCodeApplicationTableMap.put("iby temp pmt instr uses", "IBY_TEMP_PMT_INSTR_USES");
		objectCodeApplicationTableMap.put("invoice header", "AP_INVOICES_INTERFACE");
		objectCodeApplicationTableMap.put("invoice lines", "AP_INVOICE_LINES_INTERFACE");
		objectCodeApplicationTableMap.put("blanket agreement item attributes", "PO_ATTR_VALUES_INTERFACE");
		objectCodeApplicationTableMap.put("blanket agreement item attribute translations",
				"PO_ATTR_VALUES_TLP_INTERFACE");
		objectCodeApplicationTableMap.put("blanket agreement bu assignments", "PO_GA_ORG_ASSIGN_INTERFACE");
		objectCodeApplicationTableMap.put("blanket agreement headers", "PO_HEADERS_INTERFACE");
		objectCodeApplicationTableMap.put("blanket agreement lines", "PO_LINES_INTERFACE");
		objectCodeApplicationTableMap.put("blanket agreement price breaks", "PO_LINE_LOCATIONS_INTERFACE");
		objectCodeApplicationTableMap.put("purchase order distributions", "PO_DISTRIBUTIONS_INTERFACE");
		objectCodeApplicationTableMap.put("purchase order headers", "PO_HEADERS_INTERFACE");
		objectCodeApplicationTableMap.put("purchase order lines", "PO_LINES_INTERFACE");
		objectCodeApplicationTableMap.put("purchase order schedules", "PO_LINE_LOCATIONS_INTERFACE");

		applicationTableName = objectCodeApplicationTableMap.get(objectCode.toLowerCase());

		return applicationTableName;
	}

	public String getCtlFileName(String objectCode) throws ValidationException {
		String ctlFileName = "";
		//xxrCloudConfigRepository -- crObjects
		XxrCloudConfig cloudConfig = xxrCloudConfigRepository.findByObjectCode(objectCode);
		if (cloudConfig == null)
			throw new ValidationException(
					"For " + objectCode + " Cloud Configuration is not defined,Please help to define");
		else
			ctlFileName = cloudConfig.getCtlFileName();
		return ctlFileName;
	}

	public String getFileName(String objectCode) {
		String fileName = "";
		Map<String, String> objectCodeFileNameMap = new HashMap<>();

		objectCodeFileNameMap.put("supplier", "PozSuppliersInt");
		objectCodeFileNameMap.put("sites", "PozSupplierSitesInt");
		objectCodeFileNameMap.put("site assignments", "PozSiteAssignmentsInt");
		objectCodeFileNameMap.put("product and services", "PozSupProdServInt");
		objectCodeFileNameMap.put("contacts", "PozSupContactsInt");
		objectCodeFileNameMap.put("contact address", "PozSupContactAddressesInt");
		objectCodeFileNameMap.put("business classifications", "PozSupBusClassInt");
		objectCodeFileNameMap.put("attachments", "PozSupAttachmentsInt");
		objectCodeFileNameMap.put("site attachment", "PozSupSiteAttachmentsInt");
		objectCodeFileNameMap.put("business classification attachment", "PozSupBusClassAttachmentsInt");
		objectCodeFileNameMap.put("address", "PozSupAddressesInt");
		objectCodeFileNameMap.put("third party", "PozSupThirdPartyInt");

		fileName = objectCodeFileNameMap.get(objectCode.toLowerCase());

		return fileName;
	}

	public String getXlsmFileName(String objectCode) {
		String fileName = "";
		Map<String, String> objectCodeFileNameMap = new HashMap<>();

		objectCodeFileNameMap.put("supplier", "SupplierImportTemplate");
		objectCodeFileNameMap.put("sites", "SupplierSiteImportTemplate");
		objectCodeFileNameMap.put("site assignments", "SupplierSiteAssignmentImportTemplate");
		objectCodeFileNameMap.put("product and services", "SupplierProductsandServicesCategoryImportTemplate");
		objectCodeFileNameMap.put("contacts", "SupplierContactImportTemplate");
		objectCodeFileNameMap.put("contact address", "SupplierContactImportTemplate");
		objectCodeFileNameMap.put("business classifications", "SupplierBusinessClassificationImportTemplate");
		objectCodeFileNameMap.put("attachments", "SupplierAttachmentImportTemplate");
		objectCodeFileNameMap.put("site attachment", "SupplierAttachmentImportTemplate");
		objectCodeFileNameMap.put("business classification attachment", "SupplierAttachmentImportTemplate");
		objectCodeFileNameMap.put("address", "SupplierAddressImportTemplate");
		objectCodeFileNameMap.put("third party", "SupplierSiteImportTemplate");

		fileName = objectCodeFileNameMap.get(objectCode.toLowerCase());

		return fileName;
	}

	public String getXlsmSheetName(String objectCode) {
		String sheetName = "";
		Map<String, String> objectCodeSheetNameMap = new HashMap<>();

		objectCodeSheetNameMap.put("supplier", "POZ_SUPPLIERS_INT");
		objectCodeSheetNameMap.put("sites", "POZ_SUPPLIER_SITES_INT");
		objectCodeSheetNameMap.put("site assignments", "POZ_SITE_ASSIGNMENTS_INT");
		objectCodeSheetNameMap.put("product and services", "POZ_SUP_PROD_SERV_INT");
		objectCodeSheetNameMap.put("contacts", "POZ_SUP_CONTACTS");
		objectCodeSheetNameMap.put("contact address", "POZ_SUPP_CONTACT_ADDRESSES_INT");
		objectCodeSheetNameMap.put("business classifications", "POZ_SUP_BUS_CLASS_INT");
		objectCodeSheetNameMap.put("attachments", "Supplier_Profile_Attachments");
		objectCodeSheetNameMap.put("site attachment", "Supplier_Site_Attachments");
		objectCodeSheetNameMap.put("business classification attachment", "Business_Class_Attachments");
		objectCodeSheetNameMap.put("address", "POZ_SUPPLIER_ADDRESSES_INT");
		objectCodeSheetNameMap.put("third party", "Third_Party_Pay_Relationships");

		sheetName = objectCodeSheetNameMap.get(objectCode.toLowerCase());

		return sheetName;
	}

	/*
	 * public ChannelSftp setupJsch() throws JSchException { JSch jsch = new JSch();
	 * Session jschSession = jsch.getSession(username, remoteHost);
	 * jschSession.setPassword(password); // jschSession.setPort(port);
	 * java.util.Properties config = new java.util.Properties();
	 * config.put("StrictHostKeyChecking", "no"); jschSession.setConfig(config);
	 * jschSession.setTimeout(6000); jschSession.connect(); return (ChannelSftp)
	 * jschSession.openChannel("sftp"); }
	 */

	/*
	 * public Session setupJschSession() throws JSchException { JSch jsch = new
	 * JSch(); Session jschSession = jsch.getSession(username, remoteHost);
	 * jschSession.setPassword(password); // jschSession.setPort(port);
	 * java.util.Properties config = new java.util.Properties();
	 * config.put("StrictHostKeyChecking", "no"); jschSession.setConfig(config);
	 * jschSession.setTimeout(60000); jschSession.connect(); return jschSession; }
	 */

	public Session setupJschSession() throws JSchException {
		JSch jSch = new JSch();
		Session session = null;
		ChannelSftp channelSftp = null;
		//jSch.addIdentity(sftpPrivateKeyPath);
		System.out.println("Private Key Added.");
		session = jSch.getSession(username, remoteHost);
		session.setPassword(password);
		log.info("session created.");
		java.util.Properties config = new java.util.Properties();
		config.put("StrictHostKeyChecking", "no");
		session.setConfig(config);
		session.setTimeout(6000);
		session.connect();
		channelSftp = (ChannelSftp) session.openChannel("sftp");
		channelSftp.connect();
		return session;
	}

	public String getClobString(String sql) throws Exception {
		Connection con = null;
		ResultSet rs = null;
		String clobString = "";
		try {
			log.info("sql:::" + sql);
			con = dataSourceUtil.createConnection();
			// step3 create the statement object
			PreparedStatement stmt = con.prepareStatement(sql);

			// step4 execute query
			rs = stmt.executeQuery();
			// int columnCount = 0;
			if (rs.next()) {
				// columnCount = rs.getMetaData().getColumnCount();
				clobString = Utils.clobToString(rs.getClob(1));
			}
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		} finally {
			if (con != null)
				con.close();
		}
		return clobString;
	}

	public static <T> Predicate<T> distinctByKey(Function<? super T, ?> keyExtractor) {
		Set<Object> seen = ConcurrentHashMap.newKeySet();
		return t -> seen.add(keyExtractor.apply(t));
	}

	public String getProcessStatus(Long requestId,String podId) throws Exception {
		String status = "";
		Connection con = null;
		con = dynamicDataSourceBasedMultiTenantConnectionProvider.getConnection(String.valueOf(podId));
		String query = "SELECT status FROM CR_PROCESS_REQUESTS_V C WHERE C.request_id =?";
		try {
			while (true) {
				ResultSet rs = null;
				PreparedStatement selectStmnt = null;
				try {
					selectStmnt = con.prepareStatement(query);
					selectStmnt.setLong(1,requestId);
					rs = selectStmnt.executeQuery();
					if (rs.next())
						status = rs.getString("status");
					log.info("Transform status----{}",status);
					if ("C".equalsIgnoreCase(status) || "CE".equalsIgnoreCase(status)) {
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
		} finally {
			if (con != null)
				con.close();
		}
		return status;
	}


	public static void getStatus(Connection con, String requestId) throws Exception {
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
					log.info("entering:::::::");
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

	public ChannelSftp setupJsch() throws JSchException {
		JSch jSch = new JSch();
		Session session = null;
		jSch.addIdentity(sftpPrivateKeyPath);
		System.out.println("Private Key Added.");
		session = jSch.getSession(username, remoteHost);
		log.info("session created.");
		java.util.Properties config = new java.util.Properties();
		config.put("StrictHostKeyChecking", "no");
		session.setConfig(config);
		session.setTimeout(6000);
		session.connect();
		return (ChannelSftp) session.openChannel("sftp");
	}

	public MetaDataColumnsPo getMetaDataAndColumnNames(Long metaDataTableId) {
		MetaDataColumnsPo metaDataColumnsPo = new MetaDataColumnsPo();
		List<XxrCloudColumns> cloudColumnsLi = xxrCloudColumnsRepository.findByTableIdOrderByColumnId(metaDataTableId);
		String columnName = "";
		String columnType = "";
		String width = "";
		int size = cloudColumnsLi.size();
		StringBuffer metaData = new StringBuffer();
		StringBuffer columnNames = new StringBuffer();
		for (int i = 0; i < size; i++) {
			columnName = cloudColumnsLi.get(i).getColumnName();
			columnType = cloudColumnsLi.get(i).getColumnType();
			width = cloudColumnsLi.get(i).getWidth();
			columnNames.append(columnName);
			if (columnType.equalsIgnoreCase("V"))
				metaData.append(columnName + "   VARCHAR2(" + width + ")");
			else if (columnType.equalsIgnoreCase("N")) {
				// if (width == null || width.equals("0"))
				metaData.append(columnName + "   NUMBER");
				// else
				// metaData.append(columnName + " NUMBER(" + width + ")");
			} else if (columnType.equalsIgnoreCase("D"))
				metaData.append(columnName + "   VARCHAR2(245)");
			else
				metaData.append(columnName + "  VARCHAR2(245)");

			if (i != size - 1) {
				metaData.append(",");
				columnNames.append(",");
			}
		}
		metaDataColumnsPo.setMetaDataStr(metaData.toString());
		metaDataColumnsPo.setColumnNames(columnNames.toString());
		return metaDataColumnsPo;
	}

	public void basicAuthentication1(ServiceClient client, String userName, String password) {
		HttpTransportPropertiesImpl.Authenticator auth = new HttpTransportPropertiesImpl.Authenticator();
		auth.setUsername(userName);//lisa.jones
		auth.setPassword(password);
		client.getOptions().setProperty(org.apache.axis2.transport.http.HTTPConstants.AUTHENTICATE, auth);
		client.getOptions().setProperty(org.apache.axis2.transport.http.HTTPConstants.CHUNKED, false);
	}

	//SQL Injection vunerability fix
	public static String cleanEmail(String emailId) {
		return emailId.replaceAll("[^a-zA-Z0-9_+&*-@.]", "");
	}

	public Stub getCloudLoginDetails(Long podId, String bearerToken, CrCloudTemplateHeadersView crCldTempHdrsView, String urlType) throws Exception {
		HttpHeaders header = new HttpHeaders();
		header.setAccept(Arrays.asList(MediaType.APPLICATION_JSON));
		header.set("Authorization", bearerToken);
		HttpEntity<String> entity = new HttpEntity<String>(header);
		RestTemplate restTemplate = new RestTemplate();
		ObjectMapper mapper = new ObjectMapper();
		JsonNode root = null;
		String cloudUrl = null;
		GenericSoapServiceStub genericSoapServiceStub = null;
		ServiceClient client = null;
		try {
			String url = convertriteAdminHost + "/api/convertriteadmin/podcloudconfigs/" + podId;
			ResponseEntity<String> objects = restTemplate.exchange(url, HttpMethod.GET, entity, String.class);
			root = mapper.readTree(objects.getBody());
			if (root.has("payload") && root.get("payload").isArray() && root.get("payload").size() > 0) {
				JsonNode payloadNode = root.get("payload").get(0);
				cloudUrl = payloadNode.get("url").asText();
				String userName = payloadNode.get("modules").get(0).get("userName").asText();
				String password = payloadNode.get("modules").get(0).get("password").asText();

				if (urlType.equals("hcmUrl")) {
					log.info(userName + "==hcmUrl==" + password);
					HCMDataLoaderStub hCMDataLoaderStub = new HCMDataLoaderStub(cloudUrl + hcmUrl);
					client = hCMDataLoaderStub._getServiceClient();
					HttpTransportPropertiesImpl.Authenticator auth = new HttpTransportPropertiesImpl.Authenticator();
					auth.setUsername(userName);//lisa.jones
					auth.setPassword(password);
					client.getOptions().setProperty(org.apache.axis2.transport.http.HTTPConstants.AUTHENTICATE, auth);
					client.getOptions().setProperty(org.apache.axis2.transport.http.HTTPConstants.CHUNKED, false);
					return hCMDataLoaderStub;
				} else if (urlType.equals("genericSoapUrl")) {
					genericSoapServiceStub = new GenericSoapServiceStub(cloudUrl + genericSoapUrl);
					client = genericSoapServiceStub._getServiceClient();
					HttpTransportPropertiesImpl.Authenticator auth = new HttpTransportPropertiesImpl.Authenticator();
					log.info(userName + "==genericSoapUrl==" + password);
					auth.setUsername(userName);//lisa.jones
					auth.setPassword(password);
					client.getOptions().setProperty(org.apache.axis2.transport.http.HTTPConstants.AUTHENTICATE, auth);
					client.getOptions().setProperty(org.apache.axis2.transport.http.HTTPConstants.CHUNKED, false);
					return genericSoapServiceStub;
				}
			}
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return null;
	}

	public String findDelimeter(String str) {
		String specialCharacter = "";
		Pattern p = Pattern.compile("[^a-z0-9 ]", Pattern.CASE_INSENSITIVE);
		Matcher m = p.matcher(str);
		while (m.find()) {
			System.out.println("position " + m.start() + ": " + str.charAt(m.start()));
			specialCharacter = String.valueOf(str.charAt(m.start()));
			break;
		}
		return specialCharacter;
	}


	public  CrObjectInformationPo getObjectsWithInformation(Long podId, String objectId, String bearerToken) throws Exception {
		HttpHeaders header = new HttpHeaders();
		header.setAccept(Arrays.asList(MediaType.APPLICATION_JSON));
		header.set("Authorization", bearerToken);
		HttpEntity<String> entity = new HttpEntity<String>(header);
		RestTemplate restTemplate = new RestTemplate();
		ObjectMapper mapper = new ObjectMapper();
		JsonNode root = null;
		JsonNode name = null;
		CrObjectInformationPo crObjectInformationPo =null;
		try {
			String url = convertriteAdminHost + "/api/convertriteadmin/getObjectsWithInformation?podId=" + podId + "&objectId=" + objectId;
			log.info("url-->" + url);
			ResponseEntity<String> objects = restTemplate.exchange(url, HttpMethod.GET, entity, String.class);
			root = mapper.readTree(objects.getBody());
			name = root.path("payload");
			log.info("Response-->" + name);
			ObjectMapper objectMapper = new ObjectMapper();
			crObjectInformationPo = objectMapper.readValue(name.toString(), CrObjectInformationPo.class);

		} catch (Exception e) {
			log.info("Exception in getObjectsWithInformation----{}",e.getMessage());
			e.printStackTrace();
			throw new Exception(e.getMessage());
		}
		return crObjectInformationPo;
	}

	/**
	 * covert lob to List
	 * @param lob
	 * @return
	 */
	public static List<String[]> convertLobToList(String lob) {
		List<String[]> result = new ArrayList<>();
		if (lob == null || lob.isEmpty()) {
			return result; // Return an empty list if the LOB string is null or empty
		}
		// Split the LOB string into lines
		String[] lines =lob.split("\n");
		log.info("columnslength--->"+lines.length);
		// Split each line into an array of strings and add it to the result list
		for (int i = 1; i < lines.length; i++) {
			String[] columns = lines[i].split(",",-1);
			result.add(columns);
		}
		return result;
	}
	/**
	 * cloudDataProcess SQL query processing
	 * @param sqlQuery
	 * @param description
	 * @param request
	 * @return
	 */
	public CrCloudDataProcessResPo cloudDataProcessThroughSqlQuery(String sqlQuery, String description, HttpServletRequest request){
		String responseBody = null;
		CrCloudDataProcessResPo crCloudDataProcessResPo=new CrCloudDataProcessResPo();

		Date date = new Date();
		CrCloudReqPo crCloudReqPo = new CrCloudReqPo();
		crCloudReqPo.setPodId(Integer.parseInt(request.getHeader("X-TENANT-ID")));
		crCloudReqPo.setScheduledJobCall("N");
		crCloudReqPo.setSqlQuery(sqlQuery);
		crCloudReqPo.setDestinationType(description);
		crCloudReqPo.setCreationDate(date);
		crCloudReqPo.setCreatedBy("Convertrite-Core");
		crCloudReqPo.setLookUpFlag("N");
		crCloudReqPo.setIsInternalServiceCall(true);
		crCloudReqPo.setLastUpdateDate(date);

		HttpHeaders headers = new HttpHeaders();
		headers.set("Accept", MediaType.APPLICATION_JSON_VALUE);
		headers.setContentType(MediaType.APPLICATION_JSON);
		headers.set("Authorization", request.getHeader("Authorization"));
		HttpEntity<CrCloudReqPo> requestEntity = new HttpEntity<>(crCloudReqPo, headers);
		ResponseEntity<String> cloudDataApiResponse = restTemplate.exchange(cloudDataProcessUrl, HttpMethod.POST, requestEntity,
				String.class);
		System.out.println(cloudDataApiResponse.getBody());
		if (cloudDataApiResponse.getStatusCode() == HttpStatus.OK) {
			responseBody = cloudDataApiResponse.getBody();
		}
		JSONObject jsonObject = new JSONObject(responseBody);
		crCloudDataProcessResPo.setStatusId(jsonObject.getJSONObject("crCloudStatusInformation").getLong("statusId"));
		crCloudDataProcessResPo.setId(jsonObject.getJSONObject("cloudDataProcess").getLong("id"));
		return crCloudDataProcessResPo;
	}

	public JSONObject getCldJobStatus(long id, long statusId) {
		HttpHeaders headers = new HttpHeaders();
		String responseBody = null;
		String url = statusUrl + "?id=" + id + "&statusid=" + statusId;
		//log.info("CloudJobStatusUrl{}",url);
		HttpEntity<?> httpEntity = new HttpEntity<>(headers);
		restTemplate = new RestTemplate();
		ResponseEntity<String> response = restTemplate.exchange(url, HttpMethod.GET, httpEntity, String.class);
		if (response.getStatusCode() == HttpStatus.OK) {
			responseBody = response.getBody();
		}
		JSONObject jsonObject = new JSONObject(responseBody);
		return jsonObject;

	}
	public static List<String> getHeaders(Class<?> clazz) {
		List<String> headers = new ArrayList<>();

		// Iterate over all declared fields in the class
		for (Field field : clazz.getDeclaredFields()) {
			Column columnAnnotation = field.getAnnotation(Column.class);

			// If the field has a @Column annotation, add its name to headers
			if (columnAnnotation != null) {
				headers.add(columnAnnotation.name());
			} else {
				// Use field name if no @Column annotation is present
				headers.add(field.getName());
			}
		}
		return headers;
	}

	// Static method to get the object with the least sequence value
	public static Optional<ObjectInfoWithPodClodConfigPo> getObjectWithLeastSequence(List<ObjectInfoWithPodClodConfigPo> objectDetailsList) {
		log.info("Received objectDetailsList size: " + objectDetailsList.size());

		return objectDetailsList.stream()
				.filter(object -> {
					boolean hasCrObjectInformation = object.getCrObjectInformation() != null;
					log.info("Checking objectId: " + object.getObjectId() + " - has CrObjectInformation: " + hasCrObjectInformation);
					return hasCrObjectInformation;
				})
				.filter(object -> {
					boolean hasSequenceInfo = object.getCrObjectInformation().stream()
							.anyMatch(info -> "Sequence".equals(info.getInfoType()) && isNumeric(info.getInfoValue()));
					log.info("Checking objectId: " + object.getObjectId() + " - has valid sequence info: " + hasSequenceInfo);
					return hasSequenceInfo;
				})
				.min(Comparator.comparingInt(object -> {
					int minSequence = getMinSequence(object.getCrObjectInformation());
					log.info("Checking objectId: " + object.getObjectId() + " - min sequence: " + minSequence);
					return minSequence;
				}));
	}

	// Static helper method to get the minimum sequence value from the CrObjectInformation list
	private static int getMinSequence(List<CrObjectInformation> crObjectInfoList) {
		return crObjectInfoList.stream()
				.filter(info -> "Sequence".equals(info.getInfoType()) && isNumeric(info.getInfoValue()))
				.mapToInt(info -> Integer.parseInt(info.getInfoValue()))
				.min()
				.orElse(Integer.MAX_VALUE);  // If no sequence value is found, return max value
	}

	// Static helper method to check if a string is numeric
	private static boolean isNumeric(String str) {
		try {
			Integer.parseInt(str);
			return true;
		} catch (NumberFormatException e) {
			return false;
		}
	}
}
