package com.rite.products.convertrite.service;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.transaction.Transactional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import com.rite.products.convertrite.exception.ValidationException;
import com.rite.products.convertrite.model.XxrEbsIntegrationDbDetails;
import com.rite.products.convertrite.model.XxrProcessRequests;
import com.rite.products.convertrite.model.XxrSrcSystemSqlClob;
import com.rite.products.convertrite.model.XxrSrcTempHdrsView;
import com.rite.products.convertrite.po.DbLinkReqPo;
import com.rite.products.convertrite.po.DbLinkResPo;
import com.rite.products.convertrite.po.LoadDataFromEbsReqPo;
import com.rite.products.convertrite.po.LoadDataFromEbsResPo;
import com.rite.products.convertrite.po.LoadMetaDataFromEbsReqPo;
import com.rite.products.convertrite.po.LoadMetaDataFromEbsRes;
import com.rite.products.convertrite.po.SaveEbsIntegrationDetailsPo;
import com.rite.products.convertrite.po.SaveEbsIntegrationResponsePo;
import com.rite.products.convertrite.po.SaveEbsViewReqpo;
import com.rite.products.convertrite.po.SrcSystemLobIdRes;
import com.rite.products.convertrite.po.XxrEbsIntegrationDetailsResPo;
import com.rite.products.convertrite.respository.GenerateOrigTransRefDaoImpl;
import com.rite.products.convertrite.respository.SourceTemplateHeadersRepository;
import com.rite.products.convertrite.respository.XxrEbsIntegrationDbDetailsRepository;
import com.rite.products.convertrite.respository.XxrLookUpValuesRepository;
import com.rite.products.convertrite.respository.XxrLookupSetsRepository;
import com.rite.products.convertrite.respository.XxrProcessRequestsRepository;
import com.rite.products.convertrite.respository.XxrSourceTablesRepository;
import com.rite.products.convertrite.respository.XxrSrcSystemSqlClobRepository;
import com.rite.products.convertrite.respository.XxrSrcTempHdrsViewRepository;
import com.rite.products.convertrite.utils.DataSourceUtil;

import oracle.jdbc.pool.OracleDataSource;

@Service
public class EbsIntegrationServiceImpl implements EbsIntegrationService {

	private static final Logger log = LoggerFactory.getLogger(EbsIntegrationServiceImpl.class);

	@Autowired
	XxrEbsIntegrationDbDetailsRepository xxrEbsIntegrationDbDetailsRepository;
	@Autowired
	XxrLookUpValuesRepository xxrLookUpValuesRepository;
	@Autowired
	SourceTemplateHeadersRepository sourceTemplateHeadersRepository;
	@Autowired
	XxrSrcTempHdrsViewRepository xxrSrcTempHdrsViewRepository;
	@Autowired
	GenerateOrigTransRefDaoImpl generateOrigTransRefDaoImpl;
	@Autowired
	XxrSourceTablesRepository xxrSourceTablesRepository;
	@Autowired
	XxrLookupSetsRepository xxrLookupSetsRepository;
	@Autowired
	XxrSrcSystemSqlClobRepository XxrSrcSystemSqlClobRepository;
	@Autowired
	XxrProcessRequestsRepository xxrProcessRequestsRepository;

	@Autowired
	DataSourceUtil dataSourceUtil;

	@Value("${spring.profiles.active}")
	private String env;

	@Override
	public SaveEbsIntegrationResponsePo saveEbsIntegrationDetails(SaveEbsIntegrationDetailsPo ebsIntegartionDetailsPo)
			throws Exception {
		log.info("Start of saveEbsIntegrationDetails Method in service");
		SaveEbsIntegrationResponsePo saveEbsIntegrationResponsePo = new SaveEbsIntegrationResponsePo();
		XxrEbsIntegrationDbDetails xxrEbsIntegrationDbDetails = new XxrEbsIntegrationDbDetails();
		Connection con = null;
		Connection ebscon = null;
		PreparedStatement stmnt = null;
		try {

			XxrEbsIntegrationDbDetails x = xxrEbsIntegrationDbDetailsRepository
					.findByPodIdAndProjectIdAndParentObjectId(ebsIntegartionDetailsPo.getPodId(),
							ebsIntegartionDetailsPo.getProjectId(), ebsIntegartionDetailsPo.getParentObjectId());
			if (x != null)
				throw new ValidationException("Combination of podId,projectId and parentObjectId already exists");
			try {
				Class.forName("oracle.jdbc.driver.OracleDriver");
				OracleDataSource dataSource = new OracleDataSource();
				dataSource.setServerName(ebsIntegartionDetailsPo.getHostName());
				dataSource.setUser(ebsIntegartionDetailsPo.getUserName());
				dataSource.setPassword(ebsIntegartionDetailsPo.getPassword());
				dataSource.setDatabaseName(ebsIntegartionDetailsPo.getServiceName());
				dataSource.setPortNumber(ebsIntegartionDetailsPo.getPort());
				dataSource.setDriverType("thin");
				ebscon = dataSource.getConnection();
			} catch (Exception e) {
				throw new ValidationException("Please cross verify database connection details");
			}
			if (ebscon == null)
				throw new ValidationException("Please cross verify database connection details");
			xxrEbsIntegrationDbDetails.setPodId(ebsIntegartionDetailsPo.getPodId());
			xxrEbsIntegrationDbDetails.setProjectId(ebsIntegartionDetailsPo.getProjectId());
			// xxrEbsIntegrationDbDetails.setObjectId(ebsIntegartionDetailsPo.getObjectId());
			xxrEbsIntegrationDbDetails.setParentObjectId(ebsIntegartionDetailsPo.getParentObjectId());
			xxrEbsIntegrationDbDetails.setHostName(ebsIntegartionDetailsPo.getHostName());
			xxrEbsIntegrationDbDetails.setPort(ebsIntegartionDetailsPo.getPort());
			xxrEbsIntegrationDbDetails.setServiceName(ebsIntegartionDetailsPo.getServiceName());
			xxrEbsIntegrationDbDetails.setUserName(ebsIntegartionDetailsPo.getUserName());
			xxrEbsIntegrationDbDetails.setPassword(ebsIntegartionDetailsPo.getPassword());
			xxrEbsIntegrationDbDetails.setCreationDate(new java.sql.Date(new java.util.Date().getTime()));
			xxrEbsIntegrationDbDetails.setCreatedBy("ConvertRite");
			xxrEbsIntegrationDbDetails.setLastUpdatedDate(new java.sql.Date(new java.util.Date().getTime()));
			xxrEbsIntegrationDbDetails.setLastUpdateBy("ConvertRite");
			XxrEbsIntegrationDbDetails ebsIntegrationDbDetails = xxrEbsIntegrationDbDetailsRepository
					.save(xxrEbsIntegrationDbDetails);

			StringBuffer sb = new StringBuffer("CREATE PUBLIC DATABASE LINK ");
			sb.append("dblink" + ebsIntegartionDetailsPo.getServiceName() + ebsIntegrationDbDetails.getId() + env);
			sb.append("  CONNECT TO " + ebsIntegartionDetailsPo.getUserName() + " IDENTIFIED BY "
					+ ebsIntegartionDetailsPo.getPassword());
			sb.append(" USING '(DESCRIPTION=\r\n" + "		                (ADDRESS=(PROTOCOL=TCP)(HOST="
					+ ebsIntegartionDetailsPo.getHostName() + ")(PORT=" + ebsIntegartionDetailsPo.getPort() + "))");
			sb.append("(CONNECT_DATA=(SERVICE_NAME=" + ebsIntegartionDetailsPo.getServiceName() + ")))'");
//			log.info(sb.toString() + "########SqlQuery");
			// create database connection
			con = dataSourceUtil.createConnection();
			// create Prepared Statement
			stmnt = con.prepareStatement(sb.toString());
			int count = stmnt.executeUpdate();
			log.info("count::::::" + count);

			ebsIntegrationDbDetails.setDabaseLink(
					"dblink" + ebsIntegartionDetailsPo.getServiceName() + ebsIntegrationDbDetails.getId() + env);
			xxrEbsIntegrationDbDetailsRepository.save(ebsIntegrationDbDetails);
			saveEbsIntegrationResponsePo.setId(ebsIntegrationDbDetails.getId());
			saveEbsIntegrationResponsePo.setMessage("EbsIntegration details saved successfully");
		} catch (ValidationException e) {
			e.printStackTrace();
			throw new ValidationException(e.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e.getMessage());
		} finally {
			// step5 close the connection object
			if (ebscon != null)
				ebscon.close();

			if (con != null)
				con.close();
		}
		return saveEbsIntegrationResponsePo;
	}

	/*
	 * @Override public List<XxrEbsIntegrationDbDetailsResPo>
	 * getEbsIntegrationDetails() throws Exception { // TODO Auto-generated method
	 * stub log.info("Start of getEbsIntegrationDetails Method::::::");
	 * List<XxrEbsIntegrationDbDetails> xxrEbsIntegrationDetails = new
	 * ArrayList<>(); List<XxrEbsIntegrationDbDetailsResPo> xxrEbsIntegrationRes =
	 * new ArrayList<>(); try {
	 * 
	 * xxrEbsIntegrationDetails = xxrEbsIntegrationDbDetailsRepository.findAll();
	 * 
	 * xxrEbsIntegrationDetails.stream().forEach(x -> {
	 * 
	 * XxrEbsIntegrationDbDetailsResPo xxrEbsIntegrationDbDetailsResPo = new
	 * XxrEbsIntegrationDbDetailsResPo();
	 * 
	 * String podName = xxrLookUpValuesRepository.getValueById(x.getPodId()); String
	 * projectName = xxrLookUpValuesRepository.getValueById(x.getProjectId()); //
	 * String objectCode = xxrLookUpValuesRepository.getValueById(x.getObjectId());
	 * String parentObjectCode =
	 * xxrLookUpValuesRepository.getValueById(x.getParentObjectId());
	 * 
	 * xxrEbsIntegrationDbDetailsResPo.setId(x.getId());
	 * xxrEbsIntegrationDbDetailsResPo.setHostName(x.getHostName()); //
	 * xxrEbsIntegrationDbDetailsResPo.setObjectCode(objectCode); //
	 * xxrEbsIntegrationDbDetailsResPo.setObjectId(x.getObjectId());
	 * xxrEbsIntegrationDbDetailsResPo.setParentObjectId(x.getParentObjectId());
	 * xxrEbsIntegrationDbDetailsResPo.setParentObjectCode(parentObjectCode);
	 * xxrEbsIntegrationDbDetailsResPo.setPodId(x.getPodId());
	 * xxrEbsIntegrationDbDetailsResPo.setPodName(podName);
	 * xxrEbsIntegrationDbDetailsResPo.setProjectId(x.getProjectId());
	 * xxrEbsIntegrationDbDetailsResPo.setProjectName(projectName);
	 * xxrEbsIntegrationDbDetailsResPo.setServiceName(x.getServiceName());
	 * xxrEbsIntegrationDbDetailsResPo.setUserName(x.getUserName());
	 * xxrEbsIntegrationDbDetailsResPo.setPassword(x.getPassword());
	 * xxrEbsIntegrationDbDetailsResPo.setPort(x.getPort());
	 * xxrEbsIntegrationDbDetailsResPo.setDabaseLink(x.getDabaseLink());
	 * 
	 * xxrEbsIntegrationRes.add(xxrEbsIntegrationDbDetailsResPo); });
	 * 
	 * } catch (Exception e) { throw new Exception(e.getMessage()); } return
	 * xxrEbsIntegrationRes; }
	 */

	@Override
	public LoadDataFromEbsResPo loadDataFromEbs(LoadDataFromEbsReqPo loadDataFromEbsReqPo, String userId)
			throws Exception {
		log.info("Start of loadDataFromEbs Method in service");
		XxrSrcTempHdrsView sourceTemplateHeaders = new XxrSrcTempHdrsView();
		String stagingTableName = "";
		Connection con = null;
		LoadDataFromEbsResPo loadDataFromEbsResPo = new LoadDataFromEbsResPo();
		PreparedStatement stmnt = null;
		String sql = "";
		String objectCode = "";
		try {
			/*
			 * InputStream inputStream = this.getClass().getClassLoader()
			 * .getResourceAsStream(objectCode.toLowerCase() + ".sql"); BufferedReader
			 * reader = new BufferedReader(new InputStreamReader(inputStream));
			 * 
			 */
			Long sourceTemplateId = loadDataFromEbsReqPo.getSourceTemplateId();
			sourceTemplateHeaders = xxrSrcTempHdrsViewRepository.findByTemplateId(sourceTemplateId);
			if (sourceTemplateHeaders != null) {
				stagingTableName = sourceTemplateHeaders.getStagingTableName();
				objectCode = sourceTemplateHeaders.getObjectCode();
			}
			XxrSrcSystemSqlClob xxrSrcSystemSqlClobRes = XxrSrcSystemSqlClobRepository.getSrcSystemRec(objectCode,
					loadDataFromEbsReqPo.getSourceSystem(), loadDataFromEbsReqPo.getVersion());
			if (xxrSrcSystemSqlClobRes == null)
				throw new ValidationException(
						"Combination of ObjectCode,version,sourceSystem doent exists,Please help to create");
			else if (xxrSrcSystemSqlClobRes != null)
				sql = xxrSrcSystemSqlClobRes.getSqlClob();
			String dbLink = loadDataFromEbsReqPo.getDbLink();
			// String objectCode = loadDataFromEbsReqPo.getObjectCode();

			int count = getCount(dbLink, stagingTableName, sourceTemplateId, sql);

			generateOrigTransRefDaoImpl.generateOrigTranRef(sourceTemplateId, stagingTableName, userId,loadDataFromEbsReqPo.getBatchName());
			loadDataFromEbsResPo.setLoadedRecCount(count);
			loadDataFromEbsResPo.setMessage("Data Loaded successfully from EBS");
			loadDataFromEbsResPo.setStagingTableName(stagingTableName);
		} catch (ValidationException e) {
			throw new ValidationException(e.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e.getMessage());
		} finally {
			// step5 close the connection object
			if (con != null)
				con.close();
		}
		return loadDataFromEbsResPo;
	}

	private int getCount(String dbLink, String stagingTableName, Long sourceTemplateId, String sql) throws Exception{
		Connection con = null;
		PreparedStatement stmnt = null;
		int count = 0;
		try {
			StringBuffer sb = new StringBuffer("INSERT INTO " + stagingTableName + "   SELECT '" + sourceTemplateId
					+ "',NULL,NULL,ROWNUM,b.* FROM ( ");
			// sb.append(reader.lines().collect(Collectors.joining(System.lineSeparator())));
			sb.append(sql);
			sb.append(") b");
			String sqlQuery = sb.toString();
			sqlQuery = sqlQuery.replaceAll("\\{0\\}", dbLink);
			// sqlQuery.replaceAll("\\{0\\}", dbLink);
			//			log.info("sqlQuery::::::" + sqlQuery);
			// Create Coonection
			con = dataSourceUtil.createConnection();
			// create Prepared Statement
			stmnt = con.prepareStatement(sqlQuery);
			count = stmnt.executeUpdate();
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		} finally {
			if(stmnt !=null ){
				stmnt.close();
			}
			if (con != null) {
				con.close();
			}
		}
		return count;
	}

	@Override
	public DbLinkResPo getDbLink(DbLinkReqPo dbLinkReqPo) throws Exception {
		String dbLink = "";
		DbLinkResPo dbLinkResPo = new DbLinkResPo();
		try {
			/*
			 * XxrEbsIntegrationDbDetails x = xxrEbsIntegrationDbDetailsRepository
			 * .findByPodIdAndProjectIdAndParentObjectIdAndUserNameAndPassword(dbLinkReqPo.
			 * getPodId(), dbLinkReqPo.getProjectId(), dbLinkReqPo.getParentObjectId(),
			 * dbLinkReqPo.getUserName(), dbLinkReqPo.getPassword());
			 */
			XxrEbsIntegrationDbDetails x = xxrEbsIntegrationDbDetailsRepository
					.findByPodIdAndProjectIdAndParentObjectId(dbLinkReqPo.getPodId(), dbLinkReqPo.getProjectId(),
							dbLinkReqPo.getParentObjectId());
			if (x != null) {
				dbLink = x.getDabaseLink();
				dbLinkResPo.setDbLink(dbLink);
				dbLinkResPo.setMessage("Successfully Retrieved DbLink");
			} else
				throw new ValidationException(
						"Combination of podId,projectId,parentObjectId,userName and password doesn't exists in External Source Integartion,Please help to create");
		} catch (ValidationException e) {
			throw new ValidationException(e.getMessage());
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return dbLinkResPo;
	}

	@Override
	public LoadMetaDataFromEbsRes loadMetaDataFromEbs(LoadMetaDataFromEbsReqPo loadMetaDataFromEbsReqPo)
			throws Exception {
		LoadMetaDataFromEbsRes loadMetaDataFromEbsRes = new LoadMetaDataFromEbsRes();
		Connection con = null;
		PreparedStatement stmnt = null;
		Long metaDataTableId = null;
		String sql = "";

		try {
			String objectCode = loadMetaDataFromEbsReqPo.getObjectCode();
			String metaDataTableName = loadMetaDataFromEbsReqPo.getMetaDataTableName();
			String dbLink = loadMetaDataFromEbsReqPo.getDbLink();
			XxrSrcSystemSqlClob xxrSrcSystemSqlClobRes = XxrSrcSystemSqlClobRepository.getSrcSystemRec(
					loadMetaDataFromEbsReqPo.getObjectCode(), loadMetaDataFromEbsReqPo.getSourceSystem(),
					loadMetaDataFromEbsReqPo.getVersion());
			/*
			 * InputStream inputStream = this.getClass().getClassLoader()
			 * .getResourceAsStream(objectCode.toLowerCase() + ".sql"); BufferedReader
			 * reader = new BufferedReader(new InputStreamReader(inputStream)); String sql =
			 * reader.lines().collect(Collectors.joining(System.lineSeparator()));
			 */
			if (xxrSrcSystemSqlClobRes == null)
				throw new ValidationException(
						"Combination of ObjectCode,version,sourceSystem doent exists,Please help to create");
			else if (xxrSrcSystemSqlClobRes != null)
				sql = xxrSrcSystemSqlClobRes.getSqlClob();
			sql = sql.replaceAll("\\{0\\}", dbLink);

			// Create Connection
			con = dataSourceUtil.createConnection();
			// To insert metadata of table into xxr_source_tables
			metaDataTableName = insertTableMetaData(objectCode, con, metaDataTableName);
			log.debug("metaDataTableName::" + metaDataTableName);
			metaDataTableId = xxrSourceTablesRepository.getTableId(metaDataTableName);
			log.info("metaDataTableId:::::" + metaDataTableId);
			// create Prepared Statement
			stmnt = con.prepareStatement(sql);
			ResultSet rs = stmnt.executeQuery();
			ResultSetMetaData rsmd = rs.getMetaData();
			// To insert MetaData columns into xxr_source_columns
			insertColumnMetaData(rsmd, metaDataTableId, con);

			loadMetaDataFromEbsRes.setMessage("Successfully loaded MetaData from Ebs");
			loadMetaDataFromEbsRes.setTableName(metaDataTableName);
			loadMetaDataFromEbsRes.setTableId(metaDataTableId);

		} catch (ValidationException e) {
			throw new ValidationException(e.getMessage());
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		} finally {
			// step5 close the connection object
			if (con != null)
				con.close();
		}
		return loadMetaDataFromEbsRes;
	}

	/*
	 * private String sourceMetaDataTableName(String objectCode) { String
	 * sourceMetaDataTable = ""; Map<String, String> map = new HashMap<>();
	 * map.put("supplier", "XXR_SUPPLIERS"); map.put("sites", "XXR_SUPPLIER_SITES");
	 * map.put("site assignments", "XXR_SUP_SITE_ASSIGNMENTS");
	 * map.put("product and services", "XXR_SUP_PRODS_SERVS"); map.put("contacts",
	 * "XXR_SUP_CONTACTS"); map.put("contact address", "XXR_SUP_CONT_ADDRESSES");
	 * map.put("business classifications", "XXR_SUP_BUS_CLASSIFICATIONS");
	 * map.put("attachments", "XXR_SUP_ATTACHMENTS"); map.put("address",
	 * "XXR_SUP_ADDRESSES"); map.put("third party", "XXR_SUP_TPP_RELATIONSHIP");
	 * 
	 * sourceMetaDataTable = map.get(objectCode.toLowerCase());
	 * 
	 * return sourceMetaDataTable; }
	 */

	private String insertTableMetaData(String objectCode, Connection con, String metaDataTableName) throws Exception {
		PreparedStatement insertStmnt = null;
		int count = 0;
		// String metaDataTable = "";
		try {
			// metaDataTable = sourceMetaDataTableName(objectCode);
			Long lookupSetId = xxrLookupSetsRepository.getLookupSetId("Object Code");
			Long objectId = xxrLookUpValuesRepository.getIdByValuesetId(objectCode, lookupSetId);
			log.info("objectId:::::::" + objectId);
			StringBuffer sb = new StringBuffer(
					"insert into xxr_source_tables(table_id,table_name,user_table_name,description,application_id,auto_size,table_type,initial_extent,next_extent,min_extents,max_extents,pct_increase,ini_trans,max_trans,pct_free,pct_used,hosted_support_style,irep_comments,irep_annotations,object_id,attribute1,attribute2,attribute3,attribute4,attribute5,last_update_date,last_updated_by,creation_date,created_by) values ");
			sb.append("(xxr_src_table_id_s.nextval,'" + metaDataTableName + "','" + metaDataTableName + "','"
					+ objectCode + "',200,'Y','T',4,8,1,50,0,3,255,5,80,'Local',null,null," + objectId
					+ ",null,null,null,null,null,sysdate,'CONVRITE',sysdate,'CONVRITE')");
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

	private void insertColumnMetaData(ResultSetMetaData rsmd, Long metaDataTableId, Connection con) throws Exception {
		PreparedStatement insertColumnsStmnt = null;
		int count = 0;
		try {
			StringBuffer sb = new StringBuffer("insert all ");

			int numColumns = rsmd.getColumnCount();
			
			String nullableFlag = "";
			for (int i = 1; i < numColumns + 1; i++) {
				String column_name = rsmd.getColumnName(i);
				String column_type = rsmd.getColumnTypeName(i);

				int isNullable = rsmd.isNullable(numColumns);
				if (column_type.contains("VARCHAR") || column_type.contains("CHAR"))
					column_type = "V";
				else if (column_type.equalsIgnoreCase("NUMBER"))
					column_type = "N";
				else if (column_type.equalsIgnoreCase("DATE"))
					column_type = "D";
				else if (column_type.equalsIgnoreCase("TIMESTAMP"))
					column_type = "T";
				int columnSize = rsmd.getColumnDisplaySize(i);
				if (columnSize == 0)
					columnSize = 1000;
				if (isNullable == ResultSetMetaData.columnNullable)
					nullableFlag = "Y";
				else if (isNullable == ResultSetMetaData.columnNoNulls)
					nullableFlag = "N";

				sb.append(
						"into xxr_source_columns (table_id,column_id,column_name,user_column_name,description,application_id,column_sequence,column_type,width,null_allowed_flag,translate_flag,flexfield_usage_code,flexfield_application_id,flexfield_name,flex_value_set_application_id,flex_value_set_id,default_value,precision,scale,irep_comments,attribute1,attribute2,attribute3,attribute4,attribute5,last_update_date,last_updated_by,creation_date,created_by) values ("
								+ metaDataTableId + "," + i + ",'" + column_name + "','" + column_name + "','"
								+ column_name + "',200," + i + ",'" + column_type + "'," + columnSize + ",'"
								+ nullableFlag
								+ "','N','N',null,null,null,null,null,null,null,null,null,null,null,null,null,sysdate,'CONVRITE',sysdate,'CONVRITE')");

				log.info(column_name + "::::column_name" + column_type + "::::column_type" + nullableFlag
						+ ":::::::nullableFlag" + "::::::::columnSize:::::" + columnSize);
			}
			sb.append("SELECT 1 FROM DUAL");
//			log.info("sqlquery:::" + sb.toString());
			//insertColumnsStmnt = con.prepareStatement(sb.toString());
			//count = insertColumnsStmnt.executeUpdate();
			log.info("No of meta data columns inserted::" + count);
		} catch (Exception e) {
			// e.printStackTrace();
			throw new Exception(e.getMessage());
		}
	}

	@Override
	public XxrSrcSystemSqlClob uploadSqlFile(MultipartFile file, String sourceSystem, String objectCode, String version)
			throws Exception {
		log.info("Start of uploadSqlFile in service####");
		String line = "";
		StringBuffer sb = new StringBuffer();
		XxrSrcSystemSqlClob xxrSrcSystemSqlClob = new XxrSrcSystemSqlClob();
		XxrSrcSystemSqlClob xxrSrcSystemSqlClobRes = new XxrSrcSystemSqlClob();
		try {
			BufferedReader br = new BufferedReader(new InputStreamReader(file.getInputStream()));
			while ((line = br.readLine()) != null) // returns a Boolean value
			{
				sb.append(line);
				sb.append("\n");
			}
//			log.info("lines ::::::" + sb.toString());
			Clob myClob = new javax.sql.rowset.serial.SerialClob(sb.toString().toCharArray());
//			log.info(myClob + "::::::clob");
			XxrSrcSystemSqlClob srcSystemSqlClob = XxrSrcSystemSqlClobRepository.getSrcSystemRec(objectCode,
					sourceSystem, version);
			if (srcSystemSqlClob != null) {
				srcSystemSqlClob.setSqlClob(sb.toString());
				srcSystemSqlClob.setLastUpdatedDate(new java.sql.Date(new java.util.Date().getTime()));
				srcSystemSqlClob.setLastUpdateBy("ConvertRite");
				xxrSrcSystemSqlClobRes = XxrSrcSystemSqlClobRepository.save(srcSystemSqlClob);
			} else {
				xxrSrcSystemSqlClob.setObjectCode(objectCode);
				xxrSrcSystemSqlClob.setSourceSystem(sourceSystem);
				xxrSrcSystemSqlClob.setVersion(version);
				xxrSrcSystemSqlClob.setSqlClob(sb.toString());
				xxrSrcSystemSqlClob.setCreationDate(new java.sql.Date(new java.util.Date().getTime()));
				xxrSrcSystemSqlClob.setCreatedBy("ConvertRite");
				xxrSrcSystemSqlClob.setLastUpdatedDate(new java.sql.Date(new java.util.Date().getTime()));
				xxrSrcSystemSqlClob.setLastUpdateBy("ConvertRite");
				xxrSrcSystemSqlClobRes = XxrSrcSystemSqlClobRepository.save(xxrSrcSystemSqlClob);
			}
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return xxrSrcSystemSqlClobRes;
	}

	@Override
	public List<XxrEbsIntegrationDetailsResPo> getEbsIntegrationDetails() throws Exception {
		log.info("Start of getEbsIntegrationDetails ######");
		List<XxrEbsIntegrationDetailsResPo> ebsIntegrationLi = new ArrayList<>();
		ebsIntegrationLi = xxrEbsIntegrationDbDetailsRepository.getEbsIntegrationDetails();
		log.info("Size:::" + ebsIntegrationLi.size());
		return ebsIntegrationLi;
	}

	@Override
	@Transactional
	public LoadDataFromEbsResPo loadDataFromEbsV1(LoadDataFromEbsReqPo loadDataFromEbsReqPo, HttpServletRequest request)
			throws ValidationException, Exception {
		log.info("Start of loadDataFromEbs Method in service");
		XxrSrcTempHdrsView sourceTemplateHeaders = new XxrSrcTempHdrsView();
		String stagingTableName = "";
		Connection con = null;
		LoadDataFromEbsResPo loadDataFromEbsResPo = new LoadDataFromEbsResPo();
		PreparedStatement stmnt = null;
		// PreparedStatement selectStmnt = null;
		String sql = "";
		String objectCode = "";
		try {
			/*
			 * InputStream inputStream = this.getClass().getClassLoader()
			 * .getResourceAsStream(objectCode.toLowerCase() + ".sql"); BufferedReader
			 * reader = new BufferedReader(new InputStreamReader(inputStream));
			 */

			XxrProcessRequests xxrRequestsBatch = xxrProcessRequestsRepository
					.findByXxrBatchNameAndTemplateId(loadDataFromEbsReqPo.getBatchName(),loadDataFromEbsReqPo.getSourceTemplateId());
			if (xxrRequestsBatch != null)
				throw new ValidationException("This BatchName already exists,Please provide different BatchName");
			Long sourceTemplateId = loadDataFromEbsReqPo.getSourceTemplateId();
			String dbLink = loadDataFromEbsReqPo.getDbLink();

			sourceTemplateHeaders = xxrSrcTempHdrsViewRepository.findByTemplateId(sourceTemplateId);
			if (sourceTemplateHeaders != null) {
				stagingTableName = sourceTemplateHeaders.getStagingTableName();
				objectCode = sourceTemplateHeaders.getObjectCode();
			}

			// saving to processrequest
			XxrProcessRequests xxrProcessRequestsRes = saveToProcessRequests(sourceTemplateHeaders,
					loadDataFromEbsReqPo.getBatchName());

			/*
			 * StringBuffer sb1 = new StringBuffer("select count(*) from  " +
			 * stagingTableName + " where xxr_batch_name='" +
			 * loadDataFromEbsReqPo.getBatchName() + "'"); // Create Connection con =
			 * dataSourceUtil.createConnection(); // create Prepared Statement selectStmnt =
			 * con.prepareStatement(sb1.toString()); ResultSet rs =
			 * selectStmnt.executeQuery(); int rowCount = 0; if (rs.next()) rowCount =
			 * rs.getInt(1); if (rowCount > 0) throw new
			 * ValidationException("This BatchName already exists,Please provide different BatchName"
			 * );
			 */

			// Getting ebs view of particular object
			XxrSrcSystemSqlClob xxrSrcSystemSqlClobRes = XxrSrcSystemSqlClobRepository.getSrcSystemRec(objectCode,
					loadDataFromEbsReqPo.getSourceSystem(), loadDataFromEbsReqPo.getVersion());
			if (xxrSrcSystemSqlClobRes == null)
				throw new ValidationException(
						"Combination of ObjectCode,version,sourceSystem doent exists,Please help to create");
			else if (xxrSrcSystemSqlClobRes != null)
				sql = xxrSrcSystemSqlClobRes.getSqlClob();

			// Inserting data into staging table
			StringBuffer sb = new StringBuffer("INSERT INTO " + stagingTableName + "   SELECT '" + sourceTemplateId
					+ "',NULL,NULL,ROWNUM,b.* ,ROWNUM,'" + loadDataFromEbsReqPo.getBatchName() + "' FROM ( ");
			// sb.append(reader.lines().collect(Collectors.joining(System.lineSeparator())));
			sb.append(sql);
			sb.append(") b");
			String sqlQuery = sb.toString();
			sqlQuery = sqlQuery.replaceAll("\\{0\\}", dbLink);
			// sqlQuery.replaceAll("\\{0\\}", dbLink);
//			log.info("sqlQuery::::::" + sqlQuery);
			con=dataSourceUtil.createConnection();
			// create Prepared Statement
			stmnt = con.prepareStatement(sqlQuery);
			int count = stmnt.executeUpdate();
			log.info("count:::::" + count);
			// generating origtransref
		//	generateOrigTransRefDaoImpl.generateOrigTranRef(sourceTemplateId, stagingTableName, request,loadDataFromEbsReqPo.getBatchName());

			// update process_requests table
			xxrProcessRequestsRes.setStatus("C");
			xxrProcessRequestsRes.setTotalRecords(count);
			xxrProcessRequestsRes.setEndDate(new java.sql.Date(new java.util.Date().getTime()));
			xxrProcessRequestsRes.setLastUpdatedDate(new java.sql.Date(new java.util.Date().getTime()));
			xxrProcessRequestsRes.setLastUpdateBy("ConvertRite");
			XxrProcessRequests xxrProcessRequestsResU = xxrProcessRequestsRepository.save(xxrProcessRequestsRes);

			loadDataFromEbsResPo.setLoadedRecCount(count);
			loadDataFromEbsResPo.setMessage("Data Loaded successfully");
			loadDataFromEbsResPo.setStagingTableName(stagingTableName);
		} catch (ValidationException e) {
			throw new ValidationException(e.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e.getMessage());
		} finally {
			// step5 close the connection object
			if (con != null)
				con.close();
		}
		return loadDataFromEbsResPo;
	}

	private XxrProcessRequests saveToProcessRequests(XxrSrcTempHdrsView sourceTemplateHeaders, String batchName) {
		XxrProcessRequests xxrProcessRequests = new XxrProcessRequests();
		xxrProcessRequests.setTemplateId(sourceTemplateHeaders.getTemplateId());
		xxrProcessRequests.setTemplateName(sourceTemplateHeaders.getTemplateName());
		xxrProcessRequests.setParentObjectId(sourceTemplateHeaders.getParentObjectId());
		//xxrProcessRequests.setPodId(sourceTemplateHeaders.getPodId());
		xxrProcessRequests.setPercentage(100);
		xxrProcessRequests.setRequestType("LOADING");
		xxrProcessRequests.setStatus("I");
		xxrProcessRequests.setStartDate(new java.sql.Date(new java.util.Date().getTime()));
		xxrProcessRequests.setXxrBatchName(batchName);
		xxrProcessRequests.setCreationDate(new java.sql.Date(new java.util.Date().getTime()));
		xxrProcessRequests.setCreatedBy("ConvertRite");
		xxrProcessRequests.setLastUpdatedDate(new java.sql.Date(new java.util.Date().getTime()));
		xxrProcessRequests.setLastUpdateBy("ConvertRite");
		XxrProcessRequests xxrProcessRequestsRes = xxrProcessRequestsRepository.save(xxrProcessRequests);
		return xxrProcessRequestsRes;
	}

	@Override
	public void downloadEbsView(Long id, HttpServletResponse response) throws Exception {
		log.info("download EbsView############");
		XxrSrcSystemSqlClob xxrSrcSystemSqlClob = new XxrSrcSystemSqlClob();
		Optional<XxrSrcSystemSqlClob> srcSystemClobRes = XxrSrcSystemSqlClobRepository.findById(id);
		if (srcSystemClobRes.isPresent())
			xxrSrcSystemSqlClob = srcSystemClobRes.get();
		else
			throw new Exception("There is no Record with this id");
		String fileName = xxrSrcSystemSqlClob.getObjectCode().trim().replaceAll("\\s+", "") + "-View.txt";
		// log.info(fileName);
		response.setHeader(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=" + fileName);

		PrintWriter writer = response.getWriter();
		writer.write(xxrSrcSystemSqlClob.getSqlClob());
	}

	@Override
	public SrcSystemLobIdRes getSrcSystemLobId(String objectCode, String sourceSystem, String version) {
		log.info("Start of getSrcSystemLobId #########");
		SrcSystemLobIdRes srcSystemLobIdRes = new SrcSystemLobIdRes();
		XxrSrcSystemSqlClob xxrSrcSystemSqlClob = new XxrSrcSystemSqlClob();
		xxrSrcSystemSqlClob = XxrSrcSystemSqlClobRepository.findByObjectCodeAndSourceSystemAndVersion(objectCode,
				sourceSystem, version);

		srcSystemLobIdRes.setId(xxrSrcSystemSqlClob.getId());
		srcSystemLobIdRes.setMessage("Successfully Retrieved SrcSystemLobId");
		return srcSystemLobIdRes;
	}

	@Override
	public XxrSrcSystemSqlClob saveEbsView(SaveEbsViewReqpo saveEbsViewReqpo) throws Exception {
		log.info("Start of saveEbsView #########");
		XxrSrcSystemSqlClob xxrSrcSystemSqlClob = new XxrSrcSystemSqlClob();
		XxrSrcSystemSqlClob xxrSrcSystemSqlClobRes = new XxrSrcSystemSqlClob();
		try {
			/*
			 * Clob myClob = new
			 * javax.sql.rowset.serial.SerialClob(sb.toString().toCharArray());
			 * log.info(myClob + "::::::clob");
			 */
			XxrSrcSystemSqlClob srcSystemSqlClob = XxrSrcSystemSqlClobRepository.getSrcSystemRec(
					saveEbsViewReqpo.getObjectCode(), saveEbsViewReqpo.getSourceSystem(),
					saveEbsViewReqpo.getVersion());
			if (srcSystemSqlClob != null) {
				srcSystemSqlClob.setSqlClob(saveEbsViewReqpo.getEbsViewSql());
				srcSystemSqlClob.setLastUpdatedDate(new java.sql.Date(new java.util.Date().getTime()));
				srcSystemSqlClob.setLastUpdateBy("ConvertRite");
				xxrSrcSystemSqlClobRes = XxrSrcSystemSqlClobRepository.save(srcSystemSqlClob);
			} else {
				xxrSrcSystemSqlClob.setObjectCode(saveEbsViewReqpo.getObjectCode());
				xxrSrcSystemSqlClob.setSourceSystem(saveEbsViewReqpo.getSourceSystem());
				xxrSrcSystemSqlClob.setVersion(saveEbsViewReqpo.getVersion());
				xxrSrcSystemSqlClob.setSqlClob(saveEbsViewReqpo.getEbsViewSql());
				xxrSrcSystemSqlClob.setCreationDate(new java.sql.Date(new java.util.Date().getTime()));
				xxrSrcSystemSqlClob.setCreatedBy("ConvertRite");
				xxrSrcSystemSqlClob.setLastUpdatedDate(new java.sql.Date(new java.util.Date().getTime()));
				xxrSrcSystemSqlClob.setLastUpdateBy("ConvertRite");
				xxrSrcSystemSqlClobRes = XxrSrcSystemSqlClobRepository.save(xxrSrcSystemSqlClob);
			}
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return xxrSrcSystemSqlClobRes;
	}

}
