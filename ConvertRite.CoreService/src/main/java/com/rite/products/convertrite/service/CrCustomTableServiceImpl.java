package com.rite.products.convertrite.service;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.Session;
import com.opencsv.CSVWriter;
import com.rite.products.convertrite.Validations.Validations;
import com.rite.products.convertrite.exception.ValidationException;
import com.rite.products.convertrite.model.*;
import com.rite.products.convertrite.multitenancy.config.tenant.hibernate.DynamicDataSourceBasedMultiTenantConnectionProvider;
import com.rite.products.convertrite.po.*;
import com.rite.products.convertrite.respository.*;
import com.rite.products.convertrite.utils.Utils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.poi.util.IOUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Service;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.persistence.Query;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.transaction.Transactional;
import java.io.*;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Service
@Slf4j
public class CrCustomTableServiceImpl implements CrCustomTableService {

    @Autowired
    Utils utils;
    @Autowired
    CrCustomSourceTableDtlsRepository crCustomSourceTableDtlsRepo;
    @Autowired
    CrFileDetailsRepo crFileDetailsRepo;
    @Autowired
    CrDDLExecutorDaoImpl crDDLExecutorDaoImpl;
    @Autowired
    CrCustomTablesRepository crCustomTablesRepository;
    @Autowired
    CrCustomColumnsRepository crCustomColumnsRepository;
    @Autowired
    CrCustomTableDaoImpl crCustomTableDaoImpl;
    @Autowired
    CrCustomLoadDataFailedRecordsRepository crCustomLoadFailRecordsRepo;
    @PersistenceContext
    private EntityManager entityManager;
    @Autowired
    DynamicDataSourceBasedMultiTenantConnectionProvider dynamicDataSourceBasedMultiTenantConnectionProvider;
    private static final String CR_PREFIX = "CR_";
    @Value("${default-column-width}")
    private int defaultColumnWidth;
    @Value("${file.upload-dir}")
    String fileUploadDir;
    @Value("${file-transfer}")
    String fileTransfer;

    @Override
    @Transactional
    public BasicResponsePo createCustomTable(CrCreateCustomTableReqPo customTableReqPo) throws ValidationException {
        BasicResponsePo basicResponsePo = new BasicResponsePo();
        //Prefix custom TableName with "CR_"
        String customTableName = (customTableReqPo.getCustomTableName().toUpperCase().contains(CR_PREFIX)) ? customTableReqPo.getCustomTableName() : CR_PREFIX + customTableReqPo.getCustomTableName();
        CrCustomSourceTableDtls crCustomSourceTableDtls = crCustomSourceTableDtlsRepo.findByCustomTableName(customTableName);
        if (crCustomSourceTableDtls != null)
            throw new ValidationException("Custom TableName Already Exists");
        CrFileDetails crFileDetails = crFileDetailsRepo.findByFileName(customTableReqPo.getFileName());
        if (crFileDetails == null)
            throw new ValidationException("No Data Found with given FileName");
        //Covert Lob to List
        List<String[]> metaDataList = Utils.convertLobToList(crFileDetails.getFileContent());
        CrCustomTables cutmTables = crCustomTablesRepository.findByTableName(metaDataList.get(0)[0]);
        if (cutmTables != null)
            throw new ValidationException("Metadata Table Name "+ cutmTables.getTableName()+" Already Exists ");
        String metaData = metaDataListToString(metaDataList);
        String customTableDDl = String.format("Create table %s (CR_BATCH_NAME VARCHAR2(2400), " +
                "%s)", customTableName, metaData);
        log.info("CustomTable DDL Query -->{}", customTableDDl);
        try {
            log.info("start of creation of custom table {}", customTableName);
            // CustomTable Creation
            crDDLExecutorDaoImpl.executeDDL(customTableDDl);
            log.info("end of creation of custom table {}", customTableName);
        } catch (Exception e) {
            log.error("CustomTable Creation Failed--> {} ",  e.getMessage(), e);
            basicResponsePo.setMessage("CustomTable creation failed");
            basicResponsePo.setError(e.getMessage());
            return basicResponsePo;
        }
        // insert metadata  into CR_CUSTOM_TABLES
        CrCustomTables crCustomTables = saveMetaDataCrCustomTables(metaDataList);
        // Insert MetaData of columns into cr_custom_columns
        saveMetaDataCrCustomColumns(metaDataList, crCustomTables.getTableId());
        //Save Custom Source TableDtls
        CrCustomSourceTableDtls resp = saveCrCustomSourceTableDtls(customTableReqPo, customTableName, crCustomTables);
        basicResponsePo.setMessage("Successfully Created CustomTable");
        basicResponsePo.setPayload(resp);
        return basicResponsePo;
    }

    @Override
    public BasicResponsePo getCustomTableDtls() {
        BasicResponsePo basicResponsePo = new BasicResponsePo();
        basicResponsePo.setMessage("Successfully retrieved CustomSourceTable details");
        basicResponsePo.setPayload(crCustomSourceTableDtlsRepo.getCustomTableDtls());
        return basicResponsePo;
    }

    @Override
    @Transactional
    public BasicResponsePo descCustomTable(String tableName) {
        BasicResponsePo basicResponsePo = new BasicResponsePo();
        basicResponsePo.setMessage("Successfully retrieved table description");
        basicResponsePo.setPayload(crDDLExecutorDaoImpl.getTableDescription(tableName));
        return basicResponsePo;
    }

    @Override
    public BasicResponsePo modifyCustomTblColumns(CrModifyCustomTblColumnsReqPo mdfyCustmTblReq, HttpServletRequest request) {
        if ("INSERT".equals(mdfyCustmTblReq.getOperationType())) {
            List<CrCustomColumns> customColumnsList = crCustomColumnsRepository
                    .findCrColumnsForSequenceOrColumnName(mdfyCustmTblReq.getCustomTableId(), mdfyCustmTblReq.getColumnName().toUpperCase(), mdfyCustmTblReq.getDisplaySeq().intValue());
            if (!customColumnsList.isEmpty()) {
                BasicResponsePo basicResponsePo = new BasicResponsePo();
                basicResponsePo.setError("Custom Column already exists with same Name or Display Sequence");
                return basicResponsePo;
            }
        }
        BasicResponsePo basicResponsePo = new BasicResponsePo();
        try {
            CrMdfyCustomTblResPo result = crCustomTableDaoImpl.mdfyCustomTableColumns(mdfyCustmTblReq, request.getHeader("userId"));
            if("N".equals(result.getResCode())){
                basicResponsePo.setError(result.getMessage());
                basicResponsePo.setMessage(null);
            } else {
                basicResponsePo.setMessage("Successfully modified custom table columns");
            }
            basicResponsePo.setPayload(result);
        } catch (Exception e) {
            log.error("CustomTable Creation Failed--> {} ", e.getMessage(), e);
            basicResponsePo.setMessage("CustomTable creation failed. " + e.getMessage());
            basicResponsePo.setError(e.getMessage());
            return basicResponsePo;
        }
        return basicResponsePo;
    }

    @Override
    public BasicResponsePo loadDataToCustomTable(LoadCustomDataReqPo loadCustomDataReqPo, HttpServletRequest request) throws Exception {
        Connection con = null;
        BasicResponsePo basicResPo = new BasicResponsePo();
        Session jschSession = null;
        ChannelSftp channelSftp = null;
        InputStream inputStream = null;
        String strMessage = "";
        int insertcount = 0;
        long failedCount = 0;
        String result = null;
        String logFileText = null;
        CrLoadDataCustomTableResPo loadDataCustomTableResPo = new CrLoadDataCustomTableResPo();
        try {
            //Get CustomSource table details
            CrCustomSourceTableDtls custmTblDtls = crCustomSourceTableDtlsRepo.findById(loadCustomDataReqPo.getCustomTableId()).get();
            //Create connection for given tenant
            con = dynamicDataSourceBasedMultiTenantConnectionProvider.getConnection(request.getHeader("X-TENANT-ID").toString());

            //Checking in Custom table already any records exists with provided BatchName
            int count = getRecordCountByBatchName(con, custmTblDtls.getCustomTableName(), loadCustomDataReqPo.getCrBatchName());
            if (count > 0)
                throw new ValidationException("Given Batch Name  already exists, please load the data with new Batch Name.");

            String fileName = loadCustomDataReqPo.getFileName();
            try {
                log.error("fileTransfer---> {} ",  fileTransfer);
                if ("SFTP".equalsIgnoreCase(fileTransfer)) {
                    jschSession = utils.setupJschSession();
                    channelSftp = (ChannelSftp) jschSession.openChannel("sftp");
                    channelSftp.connect();
                    channelSftp.cd(fileUploadDir);
                }
                if ("SFTP".equalsIgnoreCase(fileTransfer)) {
                    inputStream = channelSftp.get(fileName);
                } else if ("NFS".equalsIgnoreCase(fileTransfer)) {
                    log.info("fileUploadDir + fileName------> {} ",  fileUploadDir + fileName);
                    inputStream = new FileInputStream(fileUploadDir + fileName);
                }

            } catch (Exception e) {
                log.error("Error in loadDataToCustomTable --->" + e);
            }
            //Custom Table Metadata & ColumnNames
            MetaDataColumnsPo metaDataColumnsPo = getMetaDataAndColumnNames(custmTblDtls.getMetadataTableId());
            if (inputStream != null) {
                boolean equalHeaderColumnsFlag = custmTblHdrColumnsEqualToDataFileHdrColumns(metaDataColumnsPo, inputStream);
                if (!equalHeaderColumnsFlag)
                    throw new ValidationException("Column Sequence of Csv file is not same as Custom table");

            }
            // Generate a timestamp without special characters
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd_HHmmss");
            String timestamp = dateFormat.format(new Date()).replaceAll("[^a-zA-Z0-9]", "");
            String extrnlTableName = custmTblDtls.getCustomTableName() + timestamp + "_EXT";
            //Create External Table
            String externalSql = "CREATE TABLE " + extrnlTableName + " (" + metaDataColumnsPo.getMetaDataStr() + " )"
                    + "ORGANIZATION EXTERNAL\r\n" + "(TYPE ORACLE_LOADER\r\n" + "DEFAULT DIRECTORY G2N_TAB_MAIN\r\n"
                    + "ACCESS PARAMETERS\r\n" + "(\r\n" + "records delimited by newline\r\n"
                    + "LOGFILE G2N_TAB_MAIN:'" + extrnlTableName + ".log'\r\n" + "BADFILE G2N_TAB_MAIN:'"
                    + extrnlTableName + ".bad'\r\n" + "skip 1\r\n"
                    + "fields terminated by ',' optionally enclosed BY '\"' LDRTRIM\r\n"
                    + "missing field values are null\r\n" + ")\r\n" + "LOCATION ('" + loadCustomDataReqPo.getFileName()
                    + "')\r\n" + ")  REJECT LIMIT UNLIMITED";
            log.info(" {} ", externalSql);
            PreparedStatement extrnlStmnt = con.prepareStatement(externalSql);
            int extrnlCount = extrnlStmnt.executeUpdate();
            log.info("{} -->count", extrnlCount);
            extrnlStmnt.close();
            try {
                //Insert Data into Custom Table
                String executeQuery = "insert into " + custmTblDtls.getCustomTableName() + "  SELECT '"
                        + loadCustomDataReqPo.getCrBatchName() + "',b.* FROM (SELECT " + metaDataColumnsPo.getColumnNames()
                        + " FROM " + extrnlTableName + ") b";
                log.info("{} --->Insert executeQuery", executeQuery);
                PreparedStatement extStmnt = con.prepareStatement(executeQuery);
                insertcount = extStmnt.executeUpdate();
                extStmnt.close();
                log.info("{} insertcount-->", insertcount);
                try {
                    if ("SFTP".equalsIgnoreCase(fileTransfer)) {
                        inputStream = channelSftp.get(extrnlTableName + ".bad");
                        if (inputStream != null) {
                            failedCount = new BufferedReader(new InputStreamReader(channelSftp.get(extrnlTableName + ".bad")))
                                    .lines().count();
                            result = new BufferedReader(new InputStreamReader(channelSftp.get(extrnlTableName + ".bad")))
                                    .lines().collect(Collectors.joining("\n"));
                            logFileText = new BufferedReader(new InputStreamReader(channelSftp.get(extrnlTableName + ".log")))
                                    .lines().collect(Collectors.joining("\n"));
                            channelSftp.rm(extrnlTableName + ".bad");
                        }
                    } else if ("NFS".equalsIgnoreCase(fileTransfer)) {
                        inputStream = new FileInputStream(fileUploadDir + extrnlTableName + ".bad");
                        if (inputStream != null) {
                            failedCount = new BufferedReader(new InputStreamReader(new FileInputStream(fileUploadDir + extrnlTableName + ".bad")))
                                    .lines().count();
                            result = new BufferedReader(new InputStreamReader(new FileInputStream(fileUploadDir + extrnlTableName + ".bad")))
                                    .lines().collect(Collectors.joining("\n"));
                            logFileText = new BufferedReader(new InputStreamReader(new FileInputStream(fileUploadDir + extrnlTableName + ".log")))
                                    .lines().collect(Collectors.joining("\n"));
                        }
                    }
                } catch (Exception e) {
                    log.error("File Not Found --->" + extrnlTableName + ".bad");
                    log.error("File Not Found --->" + e);
                }
                CrCustomLoadDataFailedRecords crCustomLoadFailRecords = new CrCustomLoadDataFailedRecords();
                crCustomLoadFailRecords.setCustomTableId(loadCustomDataReqPo.getCustomTableId());
                crCustomLoadFailRecords.setFileName(fileName);
                crCustomLoadFailRecords.setFailed(failedCount);
                crCustomLoadFailRecords.setCrBatchName(loadCustomDataReqPo.getCrBatchName());
                crCustomLoadFailRecords.setSuccess(insertcount);
                crCustomLoadFailRecords.setFailedClob(result);
                crCustomLoadFailRecords.setCreatedBy("ConvertRite");
                crCustomLoadFailRecords.setCreationDate(new java.sql.Date(new java.util.Date().getTime()));
                crCustomLoadFailRecords.setLastUpdateBy("ConvertRite");
                crCustomLoadFailRecords.setLastUpdatedDate(new java.sql.Date(new java.util.Date().getTime()));
                crCustomLoadFailRecords.setLogFileBlob(logFileText);
                //Saving Custom Table Load Data Failed Records details
                crCustomLoadFailRecordsRepo.save(crCustomLoadFailRecords);
            } catch (Exception e) {
                if (!Validations.isNullOrEmpty(extrnlTableName)) {
                    PreparedStatement dropStmnt = con.prepareStatement("DROP TABLE " + extrnlTableName);
                    int tableDeleted = dropStmnt.executeUpdate();
                    log.info(" {} --> tableDeleted", tableDeleted);
                    dropStmnt.close();
                }
                throw new Exception(e.getMessage());
            }
            PreparedStatement dropStmnt = con.prepareStatement("DROP TABLE " + extrnlTableName);
            int tableDeleted = dropStmnt.executeUpdate();
            log.info("{} --> external table Deleted", tableDeleted);
            dropStmnt.close();

            if (failedCount == 0) {
                strMessage = "Successfully Loaded Data into Custom Table";
            } else {
                strMessage = failedCount + " records failed loading into Custom Table";
            }

            loadDataCustomTableResPo.setLoadedRecords(insertcount);
            loadDataCustomTableResPo.setFailedRecords(failedCount);
            loadDataCustomTableResPo.setCrBatchName(loadCustomDataReqPo.getCrBatchName());
            loadDataCustomTableResPo.setCustomTableName(custmTblDtls.getCustomTableName());
        }finally {
            if (channelSftp != null) {
                channelSftp.exit();
                channelSftp.disconnect();
            }
            if (jschSession != null)
                jschSession.disconnect();
            if (con != null)
                con.close();
        }
        basicResPo.setMessage(strMessage);
        basicResPo.setPayload(loadDataCustomTableResPo);
        return basicResPo;
    }

    @Override
    public void downloadCustmTblFailedRecLogFile(Long customTableId, String crBatchName, HttpServletResponse resp) throws Exception {
        resp.setContentType("text/plain");
        Optional<CrCustomLoadDataFailedRecords> customLoadFailedRec = crCustomLoadFailRecordsRepo
                .findByCustomTableIdAndCrBatchName(customTableId, crBatchName);
        if (customLoadFailedRec.isPresent()) {
            CrCustomLoadDataFailedRecords crCustomLoadFailRecords = customLoadFailedRec.get();

            CrCustomSourceTableDtls custmTableDtls = crCustomSourceTableDtlsRepo.findById(customTableId).get();
            resp.setHeader(HttpHeaders.CONTENT_DISPOSITION,
                    "attachment; filename=" + custmTableDtls.getCustomTableName() + ".log");
            if (crCustomLoadFailRecords.getLogFileBlob() == null) {
                throw new Exception("Failure log file is not present");
            } else {
                IOUtils.copy(new ByteArrayInputStream(crCustomLoadFailRecords.getLogFileBlob().getBytes()), resp.getOutputStream());
            }
        } else {
            throw new Exception("No Data Found");
        }
    }

    @Override
    public void getCustmTblRecsByBatchName(GetCustomTableRecordsReqPo custmTableReqPo, HttpServletResponse response, PrintWriter writer, HttpServletRequest request) throws Exception {
        ResultSet rs = null;
        Connection con = null;
        String batchName = custmTableReqPo.getCrBatchName();
        String respType = custmTableReqPo.getResponseType();
        String tableName = custmTableReqPo.getCustomTableName();
        PreparedStatement stmt = null;
        boolean isJsonResp = "JSON".equalsIgnoreCase(respType);
        try {
            // count of Records
            if (isJsonResp) {
                Object count = getRecCountOfCustomTbl(tableName, batchName);
                log.info("count--> {} ", count);
                response.setHeader("count", String.valueOf(count));
            }
            String sql = sqlQueryofCustTable(isJsonResp, tableName);
            log.info("sql--> {} ", sql);
            con = dynamicDataSourceBasedMultiTenantConnectionProvider.getConnection(request.getHeader("X-Tenant-Id"));
            stmt = con.prepareStatement(sql);
            stmt.setString(1, batchName);
            if (isJsonResp) {
                stmt.setLong(2, custmTableReqPo.getPageNo());
                stmt.setLong(3, custmTableReqPo.getPageSize());
                stmt.setLong(4, custmTableReqPo.getPageNo());
                stmt.setLong(5, custmTableReqPo.getPageSize());
            }
            stmt.setFetchSize(50000);
            rs = stmt.executeQuery();
            if (isJsonResp) {
                processResultSetAsJson(rs, response, writer);
            } else if ("CSV".equalsIgnoreCase(respType)) {
                processResultSetAsCSV(rs, writer);
            } else {
                throw new IllegalArgumentException("Unsupported type: " + respType);
            }
        } finally {
            if (rs != null) {
                rs.close();
            }
            if (stmt != null) {
                stmt.close();
            }
            if (con != null) {
                con.close();
            }
        }
    }

    @Override
    public BasicResponsePo getBatchNamesByCustomTblId(Long customTableId) {
        BasicResponsePo resp = new BasicResponsePo();
        List<CrCustomLoadDataFailedRecords> customLoadDataFailedRecLi = crCustomLoadFailRecordsRepo.findByCustomTableId(customTableId);
        resp.setMessage("Successfully retrieved distinct batch names");
        resp.setPayload(customLoadDataFailedRecLi.stream().map(x -> x.getCrBatchName()).distinct().collect(Collectors.toList()));
        return resp;
    }

    @Override
    public void downloadCustmTblFailedRecBadFile(Long customTableId, String crBatchName, HttpServletResponse resp) throws Exception {
        resp.setContentType("text/plain");
        Optional<CrCustomLoadDataFailedRecords> customLoadFailedRec = crCustomLoadFailRecordsRepo
                .findByCustomTableIdAndCrBatchName(customTableId, crBatchName);
        if (customLoadFailedRec.isPresent()) {
            CrCustomLoadDataFailedRecords crCustomLoadFailRecords = customLoadFailedRec.get();

            CrCustomSourceTableDtls custmTableDtls = crCustomSourceTableDtlsRepo.findById(customTableId).get();
            resp.setHeader(HttpHeaders.CONTENT_DISPOSITION,
                    "attachment; filename=" + custmTableDtls.getCustomTableName() + ".bad");
            if (crCustomLoadFailRecords.getFailedClob() == null) {
                throw new Exception("Bad file is not present");
            } else {
                IOUtils.copy(new ByteArrayInputStream(crCustomLoadFailRecords.getFailedClob().getBytes()), resp.getOutputStream());
            }
        } else {
            throw new Exception("No Data Found");
        }
    }


    private String sqlQueryofCustTable(boolean isJsonResp, String custTableName) {
        StringBuilder sqlBuilder = new StringBuilder();
        if (isJsonResp)
            sqlBuilder.append("select * from (");
        sqlBuilder.append(" select a.* ");
        if (isJsonResp)
            sqlBuilder.append(", rownum r_");
        sqlBuilder.append(" from ").append(custTableName).append(" a where a.CR_BATCH_NAME= ?");
        if (isJsonResp) {
            sqlBuilder.append(" AND rownum < ((? * ?) + 1)");
            sqlBuilder.append(") WHERE r_ >= (((")
                    .append("? - 1) * ?) + 1)");
        }
        return sqlBuilder.toString();
    }

    private Object getRecCountOfCustomTbl(String tableName, String batchName) {
        StringBuilder countSqlBuilder = new StringBuilder(
                "SELECT count(*) FROM " + tableName + " where CR_BATCH_NAME= :batchName");
        Query countQuery = entityManager.createNativeQuery(countSqlBuilder.toString());
        countQuery.setParameter("batchName", batchName);
        return countQuery.getSingleResult();
    }

    private void processResultSetAsJson(ResultSet rs, HttpServletResponse response, PrintWriter writer) throws Exception {
        response.setContentType("application/json");
        response.setCharacterEncoding("UTF-8");
        JSONArray json = new JSONArray();
        ResultSetMetaData rsmd = rs.getMetaData();
        while (rs.next()) {
            int numColumns = rsmd.getColumnCount();
            JSONObject obj = new JSONObject() {
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

            for (int i = 1; i < numColumns + 1; i++) {
                String column_name = rsmd.getColumnName(i);
                obj.put(column_name, rs.getObject(column_name) != null ? rs.getObject(column_name) : JSONObject.NULL);

            }
            json.put(obj);
        }
        if (!json.isEmpty()) {
            writer.print(json);
            writer.flush();
        } else {
            writer.write("No Records");
        }
    }

    private void processResultSetAsCSV(ResultSet rs, PrintWriter writer) throws Exception {
        try (CSVWriter csvWriter = new CSVWriter(writer)) {
            csvWriter.writeAll(rs, true);
        }
    }

    private boolean custmTblHdrColumnsEqualToDataFileHdrColumns(MetaDataColumnsPo metaDataColumnsPo, InputStream inputStream) throws Exception {
        String line;
        String delimiter = ",";  // Define your delimiter, usually it's a comma
        String[] fileHeader = null;

        //read file header
        BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
        if ((line = br.readLine()) != null) {
            // Split the header line by the delimiter
            fileHeader = line.replace("\uFEFF", "").split(delimiter);
        }
        String[] tableHeader = metaDataColumnsPo.getColumnNames().split(delimiter);
        //Check Csv file Columns Seq is same as table columns
        return printNotEqualElementsWithIndexAndReturn(fileHeader, tableHeader);
    }

    private int getRecordCountByBatchName(Connection con, String customTableName, String crBatchName) throws Exception {
        PreparedStatement countStmnt = con
                .prepareStatement("select count(*) from " + customTableName
                        + " where cr_batch_name='" + crBatchName + "'");
        ResultSet rs = countStmnt.executeQuery();
        int count = 0;
        if (rs.next()) {
            count = rs.getInt("count(*)");
            log.info("######## {} ", rs.getInt("count(*)"));
        }
        countStmnt.close();
        return count;
    }

    public static boolean printNotEqualElementsWithIndexAndReturn(String[] array1, String[] array2) {
        if (array1.length != array2.length) {
            log.error("Arrays have different lengths.");
            return false;
        }
        boolean isEqual = IntStream.range(0, array1.length)
                .allMatch(i -> array1[i].trim().equalsIgnoreCase(array2[i].trim()));
        if (!isEqual) {
            IntStream.range(0, array1.length)
                    .filter(i -> !array1[i].trim().equalsIgnoreCase(array2[i].trim()))
                    .forEach(i -> log.error("Index " + i + ": " + array1[i].toUpperCase() + " != " + array2[i].toUpperCase()));
        }
        return isEqual;
    }

    private MetaDataColumnsPo getMetaDataAndColumnNames(Long metaDataTableId) {
        MetaDataColumnsPo metaDataColumnsPo = new MetaDataColumnsPo();

        List<CrCustomColumns> sortedCustomColumnsLi = crCustomColumnsRepository
                .findAllByTableIdOrderByColumnSequence(metaDataTableId);

        int size = sortedCustomColumnsLi.size();
        log.info("sourceColumnsLi--> {} ", sortedCustomColumnsLi);
        log.info("sourceColumnsLi--> {}", size);

        String columnName = "";
        String columnType = "";
        Integer width = null;
        StringBuffer metaData = new StringBuffer();
        StringBuffer columnNames = new StringBuffer();
        for (int i = 0; i < size; i++) {
            columnName = sortedCustomColumnsLi.get(i).getColumnName();
            log.debug(i + "-columnName-->" + columnName);
            columnType = sortedCustomColumnsLi.get(i).getColumnType();
            width = sortedCustomColumnsLi.get(i).getWidth();
            columnNames.append(columnName);
            if (columnType.equalsIgnoreCase("V"))
                metaData.append(columnName + "   VARCHAR2(" + width + ")");
            else if (columnType.equalsIgnoreCase("N")) {
                // if (width == null || width.equals("0"))
                metaData.append(columnName + "   NUMBER");
                // else
                // metaData.append(columnName + " NUMBER(" + width + ")");
            } else if (columnType.equalsIgnoreCase("D"))
                metaData.append(columnName + "  DATE");
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

    private void saveMetaDataCrCustomColumns(List<String[]> metaDataList, Long tableId) {
        List<CrCustomColumns> crCustomColumnsLi = new ArrayList<>();
        int i = 1;
        for (String[] columns : metaDataList) {
            Integer wdth = null;
            CrCustomColumns crCustomColumn = new CrCustomColumns();
            crCustomColumn.setTableId(tableId);
            crCustomColumn.setColumnId(i);
            crCustomColumn.setApplicationId(200);
            if(StringUtils.isNotBlank(columns[1]))
            crCustomColumn.setColumnName(columns[1].trim().toUpperCase());
            crCustomColumn.setUserColumnName(columns[2]);
            crCustomColumn.setColumnSequence(Integer.parseInt(columns[3]));
            crCustomColumn.setColumnType(columns[4]);
            if(!(List.of("N","D","L").contains(columns[4]))) {
                wdth = StringUtils.isBlank(columns[5]) ? defaultColumnWidth : Integer.parseInt(columns[5]);
            }
            crCustomColumn.setWidth(wdth);
            crCustomColumn.setNullAllowedFlag(columns[6]);
            crCustomColumn.setTranslateFlag(columns[7]);
            crCustomColumn.setFlexFieldUsageCode(columns[8]);
            crCustomColumn.setFlexFieldName(columns[9]);
            crCustomColumn.setDefaultValue(columns[10]);
            if (StringUtils.isNotBlank(columns[11]))
                crCustomColumn.setPrecision(Integer.parseInt(columns[11]));
            crCustomColumn.setDescription(columns[12]);
            if (StringUtils.isNotBlank(columns[13]))
                crCustomColumn.setScale(Integer.parseInt(columns[13]));
            crCustomColumn.setIrepComments(columns[14]);
            crCustomColumn.setCreationDate(new Date());
            crCustomColumn.setCreatedBy("ConvertRite");
            crCustomColumn.setLastUpdatedDate(new Date());
            crCustomColumn.setLastUpdatedBy("ConvertRite");
            crCustomColumnsLi.add(crCustomColumn);
            i++;
        }
        crCustomColumnsRepository.saveAll(crCustomColumnsLi);
    }

    private CrCustomTables saveMetaDataCrCustomTables(List<String[]> metaDataList) {
        CrCustomTables customTables = new CrCustomTables();
        String metaDataTblName = metaDataList.get(0)[0];
        customTables.setTableName(metaDataTblName);
        customTables.setUserTableName(metaDataTblName);
        customTables.setDescription(metaDataTblName);
        customTables.setApplicationId(200);
        customTables.setAutoSize("Y");
        customTables.setTableType("T");
        customTables.setInitialExtent(4);
        customTables.setNextExtent(8);
        customTables.setMinExtents(1);
        customTables.setMaxExtents(50);
        customTables.setPctIncrease(0);
        customTables.setIniTrans(3);
        customTables.setMaxTrans(255);
        customTables.setPctFree(5);
        customTables.setPctUsed(80);
        customTables.setHostedSupportStyle("Local");
        customTables.setCreationDate(new Date());
        customTables.setCreatedBy("ConvertRite");
        customTables.setLastUpdatedDate(new Date());
        customTables.setLastUpdatedBy("ConvertRite");
        return crCustomTablesRepository.save(customTables);
    }

    private CrCustomSourceTableDtls saveCrCustomSourceTableDtls(CrCreateCustomTableReqPo crCreateCustomTableReqPo, String customTableName, CrCustomTables crCustomTables) {
        CrCustomSourceTableDtls customSourceTableDtls = new CrCustomSourceTableDtls();
        customSourceTableDtls.setCustomTableName(customTableName);
        customSourceTableDtls.setMetadataTableId(crCustomTables.getTableId());
        customSourceTableDtls.setParentObjectId(crCreateCustomTableReqPo.getParentObjectId());
        customSourceTableDtls.setObjectId(crCreateCustomTableReqPo.getObjectId());
        customSourceTableDtls.setProjectId(crCreateCustomTableReqPo.getProjectId());
        customSourceTableDtls.setCreationDate(new Date());
        customSourceTableDtls.setCreatedBy("ConvertRite");
        return crCustomSourceTableDtlsRepo.save(customSourceTableDtls);
    }

    private String metaDataListToString(List<String[]> metaData) {
        return metaData.stream()
                .sorted(Comparator.comparingInt(array -> Integer.parseInt(array[3])))
                .map(array -> {
                    String dataType = "";
                    if ("V".equalsIgnoreCase(array[4])) {
                        String width = StringUtils.isBlank(array[5])  ? String.valueOf(defaultColumnWidth) : array[5];
                        dataType = "VARCHAR2(" + width + ")";
                    } else if ("N".equalsIgnoreCase(array[4])) {
                        dataType = "NUMBER";
                    } else if ("L".equalsIgnoreCase(array[4])) {
                        dataType = "LONG";
                    } else {
                        dataType = "VARCHAR2(" + defaultColumnWidth + ")";
                    }
                    String nullAllowedFlag = "N".equalsIgnoreCase(array[6]) ? "NOT NULL" : "NULL";
                    return array[1].trim() + " " + dataType + " " + nullAllowedFlag;
                }).collect(Collectors.joining(",\n"));
    }
}
