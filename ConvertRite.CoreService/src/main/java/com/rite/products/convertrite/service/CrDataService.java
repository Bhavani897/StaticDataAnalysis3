package com.rite.products.convertrite.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.rite.products.convertrite.enums.Status;
import com.rite.products.convertrite.model.*;
import com.rite.products.convertrite.multitenancy.config.tenant.hibernate.DynamicDataSourceBasedMultiTenantConnectionProvider;
import com.rite.products.convertrite.po.*;
import com.rite.products.convertrite.respository.*;
import com.rite.products.convertrite.utils.Utils;
import java.lang.reflect.Field;
import java.sql.ResultSetMetaData;
import javax.persistence.Query;
import javax.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.client.RestTemplate;

import javax.persistence.EntityManager;
import javax.persistence.ParameterMode;
import javax.persistence.PersistenceContext;
import javax.persistence.StoredProcedureQuery;
import javax.servlet.http.HttpServletRequest;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;

@Service
@Slf4j
public class CrDataService {

    @Value("${convertrite-admin-host}")
    String ConvertriteAdminHost;

    @Value("${cloud-status-check}")
    private String statusUrl;

    @Value("${clouddataprocess-url}")
    private String url;
    @Value("${time-zone}")
    private String timeZone;

    @Value("${no-of-rows-sync}")
    private String noOfRows;
    @Autowired
    Utils utils;

    @Autowired
    RestTemplate restTemplate;

    @Autowired
    CrFileDetailsRepo crFileDetailsRepo;

    @Autowired
    CrSourceTemplateHeadersViewRepo crSourceTemplateHeadersViewRepo;

    @Autowired
    CrCloudJobStatusRepo crCloudJobStatusRepo;

    @Autowired
    CrObjectGroupLinesRepo crObjectGroupLinesRepo;
    @Autowired
    CrCloudTemplateHeadersViewRepository crCloudTemplateHeadersViewRepository;
    @Autowired
    CrProjectsObjectsRepo crProjectsObjectsRepo;

    @Autowired
    DynamicDataSourceBasedMultiTenantConnectionProvider dynamicDataSourceBasedMultiTenantConnectionProvider;

    @PersistenceContext
    private EntityManager entityManager;
    @Autowired
    CrCloudTemplateHeadersRepository crCloudTemplateHeadersRepository;
    @Autowired
    CRTableSyncStatusRepository crTableSyncStatusRepository;
    @Autowired
    AsynService asynService;
    @Autowired
    CrScheduleJobViewRepository crScheduleJobViewRepository;
    @Autowired
    CrObjectsRepository crObjectsRepository;


    public List<String> getBatchNames(Long templateId, HttpServletRequest request) throws Exception {
        log.info("Start of  getBatchNames in service ######");
        Connection con = null;
        PreparedStatement stmnt = null;
        List<String> li = new ArrayList<>();
        ResultSet rs = null;
        try {
            String srcStagingTableName = crSourceTemplateHeadersViewRepo.getSrcStagingTableName(templateId);
            if (srcStagingTableName != null) {
                log.info("TENANT-->" + request.getHeader("X-TENANT-ID").toString());
                con = dynamicDataSourceBasedMultiTenantConnectionProvider.getConnection(request.getHeader("X-TENANT-ID").toString());

                stmnt = con.prepareStatement("select distinct cr_batch_name from " + srcStagingTableName);
                rs = stmnt.executeQuery();

                while (rs.next()) {
                    li.add(rs.getString("cr_batch_name"));
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

    public Object downloadResults(int pageNo, int pageSize, Long srcTemplateId, String dataCriteria, String batchName, String returnType, HttpHeaders httpHeaders) throws IOException {
        log.error("=================Entered into downloadResults===============");
        try {
            StoredProcedureQuery createStoredProcedure = entityManager
                    .createStoredProcedureQuery("CR_SRC_RECORD_DETAILS_PROC")
                    .registerStoredProcedureParameter("p_src_template_id", Long.class, ParameterMode.IN)
                    .registerStoredProcedureParameter("p_src_data_criteria", String.class, ParameterMode.IN)
                    .registerStoredProcedureParameter("p_batch_name", String.class, ParameterMode.IN)
                    .registerStoredProcedureParameter("p_clob_src_file", Clob.class, ParameterMode.OUT)
                    .registerStoredProcedureParameter("p_ret_code", String.class, ParameterMode.OUT)
                    .registerStoredProcedureParameter("p_ret_msg", String.class, ParameterMode.OUT)
                    .setParameter("p_src_template_id", srcTemplateId)
                    .setParameter("p_src_data_criteria", dataCriteria)
                    .setParameter("p_batch_name", batchName);

            createStoredProcedure.execute();

            Clob clobString = (Clob) createStoredProcedure.getOutputParameterValue("p_clob_src_file");
            String dataLines = Utils.clobToString(clobString);
            if ("csv".equalsIgnoreCase(returnType)) {
                // Handle CSV case
                File csvOutputFile = new File("Records_" + srcTemplateId + ".csv");
                try (PrintWriter pw = new PrintWriter(csvOutputFile)) {
                    Arrays.stream(dataLines.split("\n"))
                            .map(line -> line.split(","))
                            .map(this::convertToCSV)
                            .forEach(pw::println);
                }
                return csvOutputFile;
            } else if ("json".equalsIgnoreCase(returnType)) {
                CSVReader csvReader = new CSVReader(new StringReader(dataLines));
                List<String[]> lines = csvReader.readAll();
                String[] headers = lines.get(0);
                List<String[]> dataLines1 = lines.subList(1, lines.size());
                List<Map<String, String>> dataList = dataLines1.stream()
                        .map(line -> {
                            Map<String, String> entry = new LinkedHashMap<>();
                            for (int i = 0; i < headers.length; i++) {
                                entry.put(headers[i].replaceAll("[/\"\\\\]", ""), line[i]);
                            }
                            return entry;
                        })
                        .collect(Collectors.toList());
                int start = (pageNo - 1) * pageSize;
                int end = Math.min(start + pageSize, dataList.size());
                int totalElements = dataList.size();
                int totalPages = (int) Math.ceil((double) totalElements / pageSize);
                httpHeaders.set("pagecount", String.valueOf(totalPages));
                httpHeaders.set("totalcount", String.valueOf(end));

                List<Map<String, String>> paginatedList = dataList.subList(start, end);

                ObjectMapper objectMapper = new ObjectMapper();
                String jsonList = objectMapper.writeValueAsString(paginatedList);
                return jsonList;

            } else {
                throw new IllegalArgumentException("Invalid returnType");
            }

        } catch (Exception e) {
            log.error("Error in downloadResults---->" + e.getMessage());
            throw new IOException("Error downloading records", e);
        }
    }

    public String convertToCSV(String[] data) {
        return Stream.of(data)
                .map(this::escapeSpecialCharacters)
                .collect(Collectors.joining(","));
    }

    public String escapeSpecialCharacters(String data) {
        String escapedData = data.replaceAll("\\R", " ");
        return escapedData;
    }

    public void onLoadCallback(Long requestId, String jobStatus, String resultMessage, String tenantId) {
        log.info("Start of onLoadCallback in service ####");
        List<CrCloudJobStatus> crCloudJobStatusList = crCloudJobStatusRepo.findByLoadRequestId(requestId);
        crCloudJobStatusList.forEach(crCloudJobStatus -> {
            crCloudJobStatus.setJobStatus(jobStatus);
            crCloudJobStatus.setErrorMsg(resultMessage);
        });
        List<CrCloudJobStatus> crCloudJobStatusRes = crCloudJobStatusRepo.saveAll(crCloudJobStatusList);
        for (CrCloudJobStatus crCloudStatus : crCloudJobStatusRes) {
            //if(!jobStatus.equals("ERROR")) {
            syncTables(crCloudStatus, tenantId, "N");
            //}
        }
    }

    public void onScheduledJob(String tenantId) {
        log.debug("tenantId-->" + tenantId);
        List<CrCloudJobStatus> jobStatusLi = crCloudJobStatusRepo.findByJobStatus(Status.PROCESSING.getStatus());
        for (CrCloudJobStatus crCloudStatus : jobStatusLi) {
            Long loadRequestId = crCloudStatus.getLoadRequestId();
            log.info("loadRequestId-->" + loadRequestId);
            ZoneId zoneId = ZoneId.of(timeZone);
            LocalDateTime creationDateTime = crCloudStatus.getCreationDate().toInstant().atZone(zoneId).toLocalDateTime();
            // Get the current timestamp
            LocalDateTime currentDateTime = LocalDateTime.now();
            // Calculate the duration between creation date and current timestamp
            Duration duration = Duration.between(creationDateTime, currentDateTime);
            //get Ess job status
            if (duration.toHours() > 24) {
                updateCloudJobStatus(loadRequestId, Status.ERROR.getStatus(), "Please contact System Administrator,this EssJob is running more than 24hrs");
            } else {
                EssJobStatusResPo essJobStatusResPo = getEssJobStatus(tenantId, loadRequestId);
                if ((List.of(Status.COMPLETED.getStatus(), Status.ERROR.getStatus())).contains(essJobStatusResPo.getStatus())) {
                    updateCloudJobStatus(loadRequestId, essJobStatusResPo.getStatus(), essJobStatusResPo.getMessage());
                    if (Status.COMPLETED.getStatus().equalsIgnoreCase(essJobStatusResPo.getStatus()))
                        syncTables(crCloudStatus, tenantId, "Y");
                }
            }
        }
    }

    private void updateCloudJobStatus(Long loadRequestId, String status, String message) {
        List<CrCloudJobStatus> crCloudJobStatusList = crCloudJobStatusRepo.findByLoadRequestId(loadRequestId);
        crCloudJobStatusList.forEach(crCloudJobStatus -> {
            crCloudJobStatus.setJobStatus(status);
            crCloudJobStatus.setErrorMsg(message);
        });
        List<CrCloudJobStatus> crCloudJobStatusRes = crCloudJobStatusRepo.saveAll(crCloudJobStatusList);
    }

    private void syncTables(CrCloudJobStatus crCloudJobStatusRes, String tenantId, String scheduledJobFlag) {
        List<CrCldTemplateHdrsObjectGroupLinesRes> crCldTemplateHdrsObjectGroupLines = null;
        Long groupId = null;
        String objectIds = null;
        Long cloudTemplateId = crCloudJobStatusRes.getCldTemplateId();
        log.info("cloudTemplateId:::::::" + cloudTemplateId);
        Long objectId = crCloudJobStatusRes.getObjectId();
        String objectName = crProjectsObjectsRepo.getAllByCloudTemplateId(cloudTemplateId).get(0).objectName;

        if (objectId != null) {
            objectIds = objectId.toString();
            groupId = crObjectGroupLinesRepo.getGroupIdbyObjectId(objectId);
        }
        if (groupId != null) {
            crCldTemplateHdrsObjectGroupLines = crCloudTemplateHeadersViewRepository.getCldRemplateHdrsbyGroupId(groupId);
            objectIds = "";
            for (CrCldTemplateHdrsObjectGroupLinesRes cldtempHdrsObjectGroupLine : crCldTemplateHdrsObjectGroupLines) {
                objectIds = objectIds + cldtempHdrsObjectGroupLine.getObjectGroupLinesView().getObjectId() + ", ";
            }
        }
        String objectIdsList = Utils.replaceLastComma(objectIds);
        String[] objectIdsArr = objectIdsList.split(",");
        for (String objId : objectIdsArr) {
            JsonNode node = getObjectsWithInformation(tenantId, objId);
            JsonNode objectsDetailsNode = node.get("objectsDetails");

            ObjectMapper mapper = new ObjectMapper();
            List<ObjectInfoWithPodClodConfigPo> objectsInfoDetailsList = mapper.convertValue(objectsDetailsNode, new TypeReference<List<ObjectInfoWithPodClodConfigPo>>() {
            });

            if (objectsInfoDetailsList.get(0).getInsertTableName() != null && objectsInfoDetailsList.get(0).getInsertTableName().length() > 0) {
                initiateSyncTablesForObject(tenantId, objectsInfoDetailsList.get(0).getInsertTableName(), scheduledJobFlag);
            } else {
                log.info("Interface table not configured for object ::" + objectName);
            }

            if (objectsInfoDetailsList.get(0).getRejectionTableName() != null && objectsInfoDetailsList.get(0).getRejectionTableName().length() > 0) {
                initiateSyncTablesForObject(tenantId, objectsInfoDetailsList.get(0).getRejectionTableName(), scheduledJobFlag);
            } else {
                log.info("Rejection table not configured for object ::" + objectName);
            }

            if (objectsInfoDetailsList.get(0).getBaseTables() != null && objectsInfoDetailsList.get(0).getBaseTables().length() > 0) {
                String[] arrOfTableNameStr = objectsInfoDetailsList.get(0).getBaseTables().split(",");
                for (String tableName : arrOfTableNameStr) {
                    initiateSyncTablesForObject(tenantId, tableName, scheduledJobFlag);
                }
            } else {
                log.info("Base tables not configured for object ::" + objectName);
            }
        }
    }

    private JsonNode getObjectsWithInformation(String podId, String objectId) {
        HttpHeaders header = new HttpHeaders();
        header.setAccept(Arrays.asList(MediaType.APPLICATION_JSON));
        HttpEntity<String> entity = new HttpEntity<String>(header);
        RestTemplate restTemplate = new RestTemplate();
        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = null;
        JsonNode name = null;
        try {
            String url = ConvertriteAdminHost + "/api/convertriteadmin/auth/getObjectsWithInformation?podId=" + podId + "&objectId=" + objectId;
            ResponseEntity<String> objects = restTemplate.exchange(url, HttpMethod.GET, entity, String.class);
            System.out.println("url-->" + url);
            root = mapper.readTree(objects.getBody());
            name = root.path("payload");

        } catch (Exception e) {
            e.printStackTrace();
        }
        return name;
    }

    private EssJobStatusResPo getEssJobStatus(String tenantId, Long loadRequestId) {
        EssJobStatusResPo essJobStatusResPo = new EssJobStatusResPo();
        String essJobStatus = Status.PROCESSING.getStatus();
        String message = "EssJob is InProgress";
        String responseBody = null;
        Long statusId = null;
        Date date = new Date();
        CrCloudReqPo crCloudReqPo = new CrCloudReqPo();
        crCloudReqPo.setPodId(Integer.parseInt(tenantId));
        crCloudReqPo.setSqlQuery(essJobStatusCheckingQuery(loadRequestId));
        crCloudReqPo.setScheduledJobCall("N");
        crCloudReqPo.setCreationDate(date);
        crCloudReqPo.setCreatedBy("Convertrite-Core");
        crCloudReqPo.setLookUpFlag("N");
        crCloudReqPo.setIsInternalServiceCall(true);
        crCloudReqPo.setLastUpdatedBy("Convertrite-Core");
        crCloudReqPo.setLastUpdateDate(date);
        crCloudReqPo.setApiCallFrom("For Ess Job Status:Internal Call");

        HttpHeaders headers = new HttpHeaders();
        headers.set("Accept", MediaType.APPLICATION_JSON_VALUE);
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<CrCloudReqPo> requestEntity = new HttpEntity<>(crCloudReqPo, headers);
        ResponseEntity<String> cloudDataApiResponse = restTemplate.exchange(url, HttpMethod.POST, requestEntity,
                String.class);
        // log.info(cloudDataApiResponse.getBody());
        if (cloudDataApiResponse.getStatusCode() == HttpStatus.OK) {
            responseBody = cloudDataApiResponse.getBody();
        }
        try {
            JSONObject jsonObject = new JSONObject(responseBody);

            JSONObject crCloudStatusInfo = jsonObject.getJSONObject("crCloudStatusInformation");
            statusId = crCloudStatusInfo.getLong("statusId");

            JSONObject cloudDataProcess = jsonObject.getJSONObject("cloudDataProcess");
            long id = cloudDataProcess.getLong("id");
            while (true) {
                JSONObject statusJson = utils.getCldJobStatus(id, statusId);
                String status = statusJson.getString("status");
                if (Objects.equals(status, "completed")) {
                    break;
                }
                if (Objects.equals(status, "error")) {
                    essJobStatus = Status.ERROR.getStatus();
                    String errorMessage = statusJson.getString("status_error_msg");
                    message = errorMessage;
                    throw new Exception(errorMessage);
                }
            }
        } catch (Exception e) {
            log.error("getEssJobStatus-->" + e.getMessage());
        }
        if (statusId != null) {
            Optional<CrFileDetails> crFileDetailsOptional = crFileDetailsRepo.findByCldFileId(statusId);
            if (crFileDetailsOptional.isPresent()) {
                CrFileDetails res = crFileDetailsOptional.get();
                String lob = res.getFileContent();
                String[] lines = lob.split("\n");
                if (null != lines && lines.length > 1) {
                    if (("SUCCESS").equalsIgnoreCase(lines[1])) {
                        essJobStatus = Status.COMPLETED.getStatus();
                        message = "EssJob completed";
                    }
                }
            }
        }
        essJobStatusResPo.setStatus(essJobStatus);
        essJobStatusResPo.setMessage(message);
        return essJobStatusResPo;
    }

    private String essJobStatusCheckingQuery(Long requestId) {
        return "WITH    requst_dd AS\r\n"
                + "        (\r\n"
                + "        SELECT  DISTINCT\r\n"
                + "                (CASE\r\n"
                + "                WHEN    executable_status NOT IN ('SUCCEEDED','COMPLETED','CANCELLED','ERROR')\r\n"
                + "                        THEN    'INPROGRESS'\r\n"
                + "                ELSE    'SUCCESS' END) status\r\n"
                + "        FROM    (\r\n"
                + "                SELECT  requestid\r\n"
                + "                       ,parentrequestid\r\n"
                + "                       ,executable_status\r\n"
                + "                FROM    ess_request_history\r\n"
                + "                WHERE   ecid = \r\n"
                + "                               (\r\n"
                + "                               SELECT  ecid\r\n"
                + "                               FROM    ess_request_history\r\n"
                + "                               WHERE   requestid =" + requestId
                + "                               )\r\n"
                + "                )\r\n"
                + "        )\r\n"
                + "SELECT  (CASE\r\n"
                + "        WHEN    count (*) = sum(case when status='SUCCESS' then 1 else 0 end)\r\n"
                + "                THEN    'SUCCESS'\r\n"
                + "        ELSE    'INPROGRESS' END) job_status\r\n"
                + "FROM    requst_dd";
    }

    private String initiateSyncTablesForObject(String podId, String tableName, String scheduledJobFlag) {
        String responseBody = null;
        Date date = new Date();
        CrCloudReqPo crCloudReqPo = new CrCloudReqPo();
        crCloudReqPo.setBatchSize(Integer.valueOf(noOfRows));
        crCloudReqPo.setPodId(Integer.parseInt(podId));
        crCloudReqPo.setTableName(tableName);
        if (scheduledJobFlag.equalsIgnoreCase("Y")) {
            crCloudReqPo.setApiCallFrom("The table sync was initiated by a scheduled job");
        } else if (scheduledJobFlag.equalsIgnoreCase("N")) {
            crCloudReqPo.setApiCallFrom("The table sync was initiated by a callback API");
        }
        crCloudReqPo.setScheduledJobCall(scheduledJobFlag);
        crCloudReqPo.setDestinationType("Table Sync");
        crCloudReqPo.setCreationDate(date);
        crCloudReqPo.setCreatedBy("Convertrite-Core");
        crCloudReqPo.setLookUpFlag("Y");
        crCloudReqPo.setIsInternalServiceCall(true);
        crCloudReqPo.setLastUpdatedBy("Convertrite-Core");
        crCloudReqPo.setLastUpdateDate(date);

        HttpHeaders headers = new HttpHeaders();
        headers.set("Accept", MediaType.APPLICATION_JSON_VALUE);
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<CrCloudReqPo> requestEntity = new HttpEntity<>(crCloudReqPo, headers);
        try {
            ResponseEntity<String> cloudDataApiResponse = restTemplate.exchange(url, HttpMethod.POST, requestEntity,
                    String.class);
            log.info(cloudDataApiResponse.getBody());
            if (cloudDataApiResponse.getStatusCode() == HttpStatus.OK) {
                responseBody = cloudDataApiResponse.getBody();
            }


            JSONObject jsonObject = new JSONObject(responseBody);
            JSONObject crCloudStatusInfo = jsonObject.getJSONObject("crCloudStatusInformation");
            long statusId = crCloudStatusInfo.getLong("statusId");
            log.info("statusId:::::::" + statusId);
        } catch (Exception e) {
            log.error("initiateSyncTablesForObject-->" + e.getMessage());
        }
        return responseBody;
    }

    public void getSourceRecords(GetSourceRecords getSourceRecords, HttpServletResponse response, HttpServletRequest request) throws Exception {
        RecordsPostJobExcecutionPo recordsPostJobExecutionPo = crCloudTemplateHeadersRepository.getRecordsPostJobExecution(getSourceRecords.getCloudTemplateName());

        if (recordsPostJobExecutionPo == null || recordsPostJobExecutionPo.getSourceTableName() == null) {
            throw new Exception("There is no cloud template name in this pod");
        }

        getSourceRecords(getSourceRecords, recordsPostJobExecutionPo.getSourceTableName(), response, request);
    }

    public void getSourceRecords(GetSourceRecords getSourceRecords, String tableName, HttpServletResponse response, HttpServletRequest request) throws Exception {
        ResultSet rs = null;
        Connection con = null;
        PrintWriter writer = response.getWriter();
        String status = getSourceRecords.getStatus();
        String batchName = getSourceRecords.getBatchName();
        String type = getSourceRecords.getType();
        PreparedStatement stmt = null;
        try {
            // count of Records
            if (getSourceRecords.getType().equals("JSON")) {
                StringBuilder countSqlBuilder = new StringBuilder(
                        "SELECT count(*) FROM " + tableName + " where CR_BATCH_NAME='" + batchName + "'");
                if (status != null && !status.isBlank() && !status.equalsIgnoreCase("all")) {
                    countSqlBuilder.append("  and VALIDATION_FLAG in(" + "'").append(status).append("')");
                }
                Query countQuery = entityManager.createNativeQuery(countSqlBuilder.toString());
                Object count = countQuery.getSingleResult();
                log.info(count + "count:::::::::");
                response.setHeader("count", String.valueOf(count));
            }
            StringBuilder sqlBuilder = new StringBuilder();
            if (getSourceRecords.getType().equals("JSON"))
                sqlBuilder.append("select * from (");
            sqlBuilder.append(" select a.* ");
            if (getSourceRecords.getType().equals("JSON"))
                sqlBuilder.append(", rownum r_");
            sqlBuilder.append(" from ").append(tableName).append(" a where a.CR_BATCH_NAME='").append(batchName).append("'");
            if (status != null && !status.isBlank() && !status.equalsIgnoreCase("all")) {
                sqlBuilder.append("  and a.VALIDATION_FLAG in(" + "'").append(status).append("')");
            }
            if (getSourceRecords.getType().equals("JSON")) {
                sqlBuilder.append("  and rownum < ((").append(getSourceRecords.getPageNo()).append("*").append(getSourceRecords.getPageSize()).append(")+1)");
                sqlBuilder.append(" ) WHERE r_ >= (((").append(getSourceRecords.getPageNo()).append("- 1)*").append(getSourceRecords.getPageSize()).append(")+1)");
            }

            String sql = sqlBuilder.toString();
            log.info(sql);
            con = dynamicDataSourceBasedMultiTenantConnectionProvider.getConnection(request.getHeader("X-Tenant-Id"));

            stmt = con.prepareStatement(sql);
//            stmt.setString(1, batchName);
//            if (status != null && !status.isBlank() && !status.equalsIgnoreCase("all")) {
//                stmt.setString(2, status);
//            }
//            if ("JSON".equals(getSourceRecords.getType())) {
//                stmt.setLong(3, getSourceRecords.getPageNo());
//                stmt.setLong(4, getSourceRecords.getPageSize());
//                stmt.setLong(5, getSourceRecords.getPageNo());
//                stmt.setLong(6, getSourceRecords.getPageSize());
//            }
            stmt.setFetchSize(50000);
            rs = stmt.executeQuery();

            if ("JSON".equalsIgnoreCase(type)) {
                processResultSetAsJson(rs, response, writer);
            } else if ("CSV".equalsIgnoreCase(type)) {
                processResultSetAsCSV(rs, writer);
            } else {
                throw new IllegalArgumentException("Unsupported type: " + type);
            }
        } finally {
            if (writer != null) {
                writer.close();
            }
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


    // Get records from Cloud Staging table based on the batch name and status requested
    public void getCloudStagingRecords(CrCloudRecordsReqPo crCloudRecordsReqPo, HttpServletResponse response, HttpServletRequest request) throws Exception {

        RecordsPostJobExcecutionPo recordsPostJobExecutionPo = crCloudTemplateHeadersRepository.getRecordsPostJobExecution(crCloudRecordsReqPo.getCloudTemplateName());

        if (recordsPostJobExecutionPo == null || recordsPostJobExecutionPo.getCloudTableName() == null) {
            throw new Exception("There is no cloud template name in this pod: " + crCloudRecordsReqPo.getCloudTemplateName());
        }

        ResultSet rs = null;
        Connection con = null;
        PreparedStatement stmt = null;
        PrintWriter writer = response.getWriter();
        String batchName = crCloudRecordsReqPo.getBatchName();
        String responseType = crCloudRecordsReqPo.getResponseType();
        String cloudTableName = recordsPostJobExecutionPo.getCloudTableName();
        String sourceTableName = recordsPostJobExecutionPo.getSourceTableName();
        try {
            // count of Records
            if (responseType.equals("JSON")) {
                StringBuilder countSqlBuilder = new StringBuilder("SELECT count(*) FROM ");
                countSqlBuilder.append(cloudTableName).append(" where CR_BATCH_NAME='").append(batchName).append("'");
                log.info("Count SQlQuery: " + countSqlBuilder.toString());
                Query countQuery = entityManager.createNativeQuery(countSqlBuilder.toString());
                Object count = countQuery.getSingleResult();
                log.info("count:" + count);
                response.setHeader("count", String.valueOf(count));
            }
            StringBuilder sqlBuilder = new StringBuilder();
            if (responseType.equals("JSON"))
                sqlBuilder.append("select * from (");
            sqlBuilder.append(" select c.* ");
            if (responseType.equals("JSON"))
                sqlBuilder.append(", rownum r_");
            sqlBuilder.append(" from ").append(cloudTableName).append(" c ").append(" where c.CR_BATCH_NAME='").append(batchName).append("'");
            if (responseType.equals("JSON")) {
                sqlBuilder.append("  and rownum < ((").append(crCloudRecordsReqPo.getPageNo()).append("*").append(crCloudRecordsReqPo.getPageSize()).append(")+1)");
                sqlBuilder.append(" ) WHERE r_ >= (((").append(crCloudRecordsReqPo.getPageNo()).append("- 1)*").append(crCloudRecordsReqPo.getPageSize()).append(")+1)");
            }

            String sql = sqlBuilder.toString();
            log.info(sql);
            con = dynamicDataSourceBasedMultiTenantConnectionProvider.getConnection(request.getHeader("X-Tenant-Id"));

            stmt = con.prepareStatement(sql);

            stmt.setFetchSize(2000);
            rs = stmt.executeQuery();

            if ("JSON".equalsIgnoreCase(responseType)) {
                processResultSetAsJson(rs, response, writer);
            } else if ("CSV".equalsIgnoreCase(responseType)) {
                processResultSetAsCSV(rs, writer);
            } else {
                throw new IllegalArgumentException("Unsupported response type: " + responseType);
            }
        } catch (Exception e) {
            log.error("Exception while retrieving cloud staging records: " + e.getMessage());
            throw e;
        } finally {
            if (writer != null) {
                writer.close();
            }
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
            // System.out.println("JsonObject:::::"+obj);
        }

        // System.out.println("JsonArray:::::"+json);
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

    public void cloudJobStatusCheck(String tenantId, Long loadRequestId, Long jobID) {
        log.info("========cloudJobStatusCheck=========");
        EssJobStatusResPo essJobStatusResPo = new EssJobStatusResPo();
        List<CrCloudJobStatus> jobStatusLi = crCloudJobStatusRepo.findByLoadRequestId(loadRequestId);
        if (loadRequestId != 0L) {
            do {
                essJobStatusResPo = getEssJobStatus(tenantId, loadRequestId);
            } while (!Status.COMPLETED.getStatus().equalsIgnoreCase(essJobStatusResPo.getStatus()));
            for (CrCloudJobStatus crCloudStatus : jobStatusLi) {

                updateCloudJobStatus(loadRequestId, essJobStatusResPo.getStatus(), essJobStatusResPo.getMessage());
                syncCloudTables(crCloudStatus, tenantId, "Y", jobID);
            }
        } else {
            syncDependentTables(jobID, tenantId);
        }
    }

    private void syncDependentTables(Long jobId, String tenantId) {
        CrScheduleJobView crScheduleJobView = crScheduleJobViewRepository.findByJobId(jobId);
        //String[] objectIdsArr = getObjectInformation(crScheduleJobView.getObjectId());
        CrObjects crObjects = crObjectsRepository.findByObjectId(crScheduleJobView.getObjectId());
        // Convert the comma-separated string into a Set<String>
        Set<String> tableNames = null;
        if (crObjects.getDependentTables() != null && !crObjects.getDependentTables().isEmpty()) {
            tableNames = new HashSet<>(Arrays.asList(crObjects.getDependentTables().split(",")));
        }
        if (tableNames != null) {
            for (String tableName : tableNames) {
                String responseBody = initiateSyncTablesForObject(tenantId, tableName, "Y");
                if (responseBody != null && !responseBody.isEmpty()) {
                    asynService.checkStatusOftableSync(responseBody, jobId);
                } else {
                    //saving into cr_table_sync_status table
                    CRTableSyncStatus crTableSyncStatus = saveIntoTableSyncStatusTable(jobId, 0L, 0L, tableName, Status.ERROR.getStatus());
                }
            }
        }
    }

    private void syncCloudTables(CrCloudJobStatus crCloudJobStatusRes, String tenantId, String scheduledJobFlag, Long jobId) {
        String objectName = crProjectsObjectsRepo.getAllByCloudTemplateId(crCloudJobStatusRes.getCldTemplateId()).get(0).objectName;
        String[] objectIdsArr = getObjectInformation(crCloudJobStatusRes.getObjectId());
        Set<String> tableNames = new HashSet<>();
        for (String objId : objectIdsArr) {
            JsonNode node = getObjectsWithInformation(tenantId, objId);
            JsonNode objectsDetailsNode = node.get("objectsDetails");

            ObjectMapper mapper = new ObjectMapper();
            List<ObjectInfoWithPodClodConfigPo> objectsInfoDetailsList = mapper.convertValue(objectsDetailsNode, new TypeReference<List<ObjectInfoWithPodClodConfigPo>>() {
            });
            if (objectsInfoDetailsList.get(0).getInsertTableName() != null && objectsInfoDetailsList.get(0).getInsertTableName().length() > 0) {
                tableNames.add(objectsInfoDetailsList.get(0).getInsertTableName());
            } else {
                log.info("Interface table not configured for object ::" + objectName);
            }

            if (objectsInfoDetailsList.get(0).getRejectionTableName() != null && objectsInfoDetailsList.get(0).getRejectionTableName().length() > 0) {
                tableNames.add(objectsInfoDetailsList.get(0).getRejectionTableName());
            } else {
                log.info("Rejection table not configured for object ::" + objectName);
            }

            if (objectsInfoDetailsList.get(0).getBaseTables() != null && objectsInfoDetailsList.get(0).getBaseTables().length() > 0) {
                String[] arrOfTableNameStr = objectsInfoDetailsList.get(0).getBaseTables().split(",");
                tableNames.addAll(Arrays.asList(arrOfTableNameStr));
            } else {
                log.info("Base tables not configured for object ::" + objectName);
            }

        }
        for (String tableName : tableNames) {
            String responseBody = initiateSyncTablesForObject(tenantId, tableName, scheduledJobFlag);
            if (responseBody != null && !responseBody.isEmpty()) {
                asynService.checkStatusOftableSync(responseBody, jobId);
            } else {
                //saving into cr_table_sync_status table
                CRTableSyncStatus crTableSyncStatus = saveIntoTableSyncStatusTable(jobId, 0L, 0L, tableName, Status.ERROR.getStatus());
            }
        }
    }

    CRTableSyncStatus saveIntoTableSyncStatusTable(Long jobId, Long entityId, Long statusId, String tableName, String status) {
        CRTableSyncStatus crTableSyncStatus = new CRTableSyncStatus();
        crTableSyncStatus.setJobId(jobId);
        crTableSyncStatus.setTableName(tableName);
        crTableSyncStatus.setSyncStatusId(statusId);
        crTableSyncStatus.setSyncEntityId(entityId);
        crTableSyncStatus.setStatus(status);
        return crTableSyncStatusRepository.save(crTableSyncStatus);
    }

    @Async
    void checkStatusOftableSync(String responseBody, Long jobId) {
        String essJobStatus = Status.PROCESSING.getStatus();
        String message = "EssJob is InProgress";
        //saving into cr_table_sync_status table
        // CRTableSyncStatus crTableSyncStatus = saveIntoTableSyncStatusTable(jobId, id, statusId, tableName);
        try {
            JSONObject jsonObject = new JSONObject(responseBody);

            JSONObject crCloudStatusInfo = jsonObject.getJSONObject("crCloudStatusInformation");
            long statusId = crCloudStatusInfo.getLong("statusId");

            JSONObject cloudDataProcess = jsonObject.getJSONObject("cloudDataProcess");
            long id = cloudDataProcess.getLong("id");
            String tableName = cloudDataProcess.getString("tableName");
            //saving into cr_table_sync_status table
            CRTableSyncStatus crTableSyncStatus = saveIntoTableSyncStatusTable(jobId, id, statusId, tableName, "Inprogress");
            while (true) {
                JSONObject statusJson = utils.getCldJobStatus(id, statusId);
                String status = statusJson.getString("status");
                if (Objects.equals(status, "completed")) {
                    updateStatusOfSync(crTableSyncStatus.getSyncId(), Status.COMPLETED.getStatus());
                    break;
                }
                if (Objects.equals(status, "error")) {
                    updateStatusOfSync(crTableSyncStatus.getSyncId(), Status.ERROR.getStatus());
                    break;
                }
            }
        } catch (Exception e) {
            log.error("getEssJobStatus-->" + e.getMessage());

        }
    }

    void updateStatusOfSync(Long statusId, String status) {
        Optional<CRTableSyncStatus> crTableSyncStatusOptional = crTableSyncStatusRepository.findById(statusId);
        CRTableSyncStatus crTableSyncStatus = new CRTableSyncStatus();
        if (crTableSyncStatusOptional.isPresent()) {
            crTableSyncStatus = crTableSyncStatusOptional.get();
        }
        crTableSyncStatus.setStatus(status);
        crTableSyncStatusRepository.save(crTableSyncStatus);
    }

    String[] getObjectInformation(Long objectId) {
        log.info("getObjectInformation method");
        List<CrCldTemplateHdrsObjectGroupLinesRes> crCldTemplateHdrsObjectGroupLines = null;
        Long groupId = null;
        String objectIds = null;
        //String objectName = crProjectsObjectsRepo.getAllByCloudTemplateId(cloudTemplateId).get(0).objectName;
        if (objectId != null) {
            objectIds = objectId.toString();
            groupId = crObjectGroupLinesRepo.getGroupIdbyObjectId(objectId);
        }
        if (groupId != null) {
            crCldTemplateHdrsObjectGroupLines = crCloudTemplateHeadersViewRepository.getCldRemplateHdrsbyGroupId(groupId);
            objectIds = "";
            for (CrCldTemplateHdrsObjectGroupLinesRes cldtempHdrsObjectGroupLine : crCldTemplateHdrsObjectGroupLines) {
                objectIds = objectIds + cldtempHdrsObjectGroupLine.getObjectGroupLinesView().getObjectId() + ", ";
            }
        }
        String objectIdsList = Utils.replaceLastComma(objectIds);
        return objectIdsList.split(",");
    }

    public List<String> displayParamList(Long objectid, HttpServletRequest request) {
        BasicResponsePo respo = new BasicResponsePo();
        CrObjects crObject = null;
        crObject = crObjectsRepository.findByObjectId(objectid);
        log.info("Start of getParamList in service ######");
        List<String> paramList = new ArrayList<>();
        if (crObject != null) {
            String pLst = crObject.getParameterlist();
            log.info("pLst"+pLst);

            if (pLst != null && !pLst.isEmpty()) {
                String  params = pLst;
                paramList = Collections.singletonList(params);
                log.info("paramList"+paramList);

                respo.setPayload(paramList);
                respo.setMessage("Successfully fetched the paramlist details");
                respo.setError(null);
            } else {
                respo.setPayload(null);
                respo.setMessage(null);
                respo.setError("No parameter list found");
            }
        } else {
            respo.setPayload(null);
            respo.setMessage("No CrObjects found for the provided objectId");
            respo.setError("No data found");
        }

        return paramList;
    }


}

