package com.rite.products.convertrite.service;

import com.rite.products.convertrite.enums.PreLoadJobStatus;
import com.rite.products.convertrite.enums.Status;
import com.rite.products.convertrite.exception.ValidationException;
import com.rite.products.convertrite.model.*;
import com.rite.products.convertrite.multitenancy.util.TenantContext;
import com.rite.products.convertrite.po.*;
import com.rite.products.convertrite.respository.*;
import com.rite.products.convertrite.utils.Utils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.retry.annotation.Recover;
import org.springframework.retry.annotation.Retryable;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import javax.servlet.http.HttpServletRequest;
import java.sql.SQLTransientException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@Component
@Slf4j
public class CrPreLoadCloudTemplatesService {

    @Value("${cloud-version}")
    String cloudVersion;
    @Value("${get-validation-objects-url}")
    String getValidationObjectsApiUrl;
    @Value("${execute-validation-objects-url}")
    String executeValidationObjectsUrl;
    @Autowired
    CrCloudTemplateService crCloudTemplateService;

    @Autowired
    CrCloudTableRepository crCloudTableRepository;

    @Autowired
    CrPreLoadCldSetupStatusRepository crPreLoadCldSetupStatusRepository;

    @Autowired
    CrCloudTemplateHeadersRepository crCloudTemplateHeadersRepository;
    @Autowired
    CrDDLExecutorDaoImpl crDDLExecutorDaoImpl;

    @Autowired
    RestTemplate restTemplate;
    @Autowired
    CrObjectsRepository crObjectsRepository;
    @Autowired
    CrCloudTemplateColumnsRepository crCloudTemplateColumnsRepository;

    @Value("${clouddataprocess-url}")
    private String cloudDataProcessUrl;

    @Autowired
    Utils utils;

    @Autowired
    CreateSourceStagTableDaoImpl createSourceStagTableDaoImpl;
    @Autowired
    CrMetaDataDaoImpl crMetaDataDaoImpl;
    @Autowired
    AsyncProcessStatusService asyncProcessStatusService;

    /**
     *
     * @param preLoadCloudSetUpsReqPo
     * @param request
     * @param asyncProcessId
     * @throws Exception
     */
    public void preLoadCloudSetUps(CrPreLoadCloudSetUpsReqPo preLoadCloudSetUpsReqPo, HttpServletRequest request, Long asyncProcessId) throws Exception {
        log.info("Start of preLoadCloudSetUps in service ##");
        CrMetaDataResPo metaDataResPo = null;
        String userId = request.getHeader("userId");
        try {
            metaDataResPo = crMetaDataDaoImpl.preLoadCldMetaData(preLoadCloudSetUpsReqPo.getProjectId(), cloudVersion, userId);
            log.info("metaDataResPo.getReturnCode()->{}", metaDataResPo.getReturnCode());
        } catch (Exception e) {
            log.error("Meta data creation error->{}", e.getMessage());
            asyncProcessStatusService.endProcess(asyncProcessId, null,null, PreLoadJobStatus.METADATA_LOAD_ERROR.getValue(), e.getMessage(), userId, null);
            throw new Exception("Meta data creation error ->"+e.getMessage());
        }
        if (List.of("SUCCESS", "WARNING").contains(metaDataResPo.getReturnCode().toUpperCase())) {
            List<CompletableFuture<Void>> tasks = new ArrayList<>();
            for (Long objectId : preLoadCloudSetUpsReqPo.getObjectIdLi()) {
                log.info("objectId->{}", objectId);
                tasks.add(preLoadCloudSetUpsAsync(objectId, preLoadCloudSetUpsReqPo.getProjectId(), request));
            }
            //Waits until the future task completes
            if (preLoadCloudSetUpsReqPo.getObjectIdLi().size() > 0)
                CompletableFuture.allOf(tasks.toArray(new CompletableFuture[0])).get();

            asyncProcessStatusService.endProcess(asyncProcessId, null,null, Status.COMPLETED.getStatus(), null, userId, null);
        } else {
            asyncProcessStatusService.endProcess(asyncProcessId, null,null, PreLoadJobStatus.METADATA_LOAD_ERROR.getValue(), metaDataResPo.getReturnMessage(), userId, null);
            throw new Exception("Meta data creation error ->"+metaDataResPo.getReturnMessage());
        }
    }

    @Async
    public CompletableFuture<Void> preLoadCloudSetUpsAsync(Long objectId, Long projectId, HttpServletRequest request) {
        log.info("Start of preLoadCloudSetUpsAsync");
        return CompletableFuture.runAsync(() -> {
            try {
                preLoadCloudSetUps(objectId, projectId, request);
            } catch (SQLTransientException e) {
                log.error("preLoadCloudSetUpsAsync-> {}", e);
                throw new RuntimeException(e);
            }
        });
    }

    @Retryable(retryFor = SQLTransientException.class)
    private void preLoadCloudSetUps(Long objectId, Long projectId, HttpServletRequest request) throws SQLTransientException {
        log.info("Start of preLoadCloudSetUps");
        CrPreLoadCldSetupStatus crPreLoadCldSetupRes = null;
        String userId = request.getHeader("userId");
        try {
            // Set tenant context explicitly for this thread
            TenantContext.setTenantId(String.valueOf(request.getHeader("X-TENANT-ID")));
            crPreLoadCldSetupRes = crPreLoadCldSetupStatusRepository.findByObjectIdAndProjectId(objectId, projectId);
            if (crPreLoadCldSetupRes.getCldSetUpStatus().equalsIgnoreCase(PreLoadJobStatus.METADATA_SUCCESS.getValue())) {
                log.info("started creating {} cloud template", crPreLoadCldSetupRes.getCldTemplateCode());
                Long metaDataTableId = crCloudTableRepository.getTableId(crPreLoadCldSetupRes.getCldMetaDataTableName());
                if (metaDataTableId == null)
                    throw new ValidationException("Meta Data table " + crPreLoadCldSetupRes.getCldMetaDataTableName() + " doesn't exist ");
                //create cloud template request
                CrCloudTemplateHeaderReqPo cldTemplateRePo = createCldTemplateHdrReq(crPreLoadCldSetupRes, objectId, projectId, metaDataTableId);
                //save cloud templated headers
                CrCloudTemplateHeaderResPo crCldTemplateRes = crCloudTemplateService.saveCloudTemplateHeaders(cldTemplateRePo);
                //get cloud metadata columns
                CloudSourceColumnsPo cldSourceColumnsPo = crCloudTemplateService.getCloudSourceColumns(null, crPreLoadCldSetupRes.getCldMetaDataTableName());
                //preparing request for cloud template columns
                List<CrCloudTemplateColumnsReqPo> saveCloudTemplateColumnLi = getCloudTemplateColsReqPo(cldSourceColumnsPo, crCldTemplateRes.getTemplateId());
                //save cloud template columns
                crCloudTemplateService.saveAllCloudTemplateColumns(saveCloudTemplateColumnLi);
                //update preload-cld-setup-status table
                updatePreLoadCldSetupStatus(crPreLoadCldSetupRes.getSetupId(), PreLoadJobStatus.CLD_TEMPLATE_SUCCESS, null, null, userId);
                log.info("end of creation {} cloud template", crPreLoadCldSetupRes.getCldTemplateCode());
                // create cloud staging table
                try {
                    log.info("Started creating cloud staging table ");
                    SourceStagingTablePo stgTablePo = createSourceStagTableDaoImpl.createStaggingTable(metaDataTableId, crCldTemplateRes.getTemplateId(), crCldTemplateRes.getTemplateCode(), "CLOUD", userId);
                    if ("SUCCESS".equalsIgnoreCase(stgTablePo.getResult())) {
                        CrCloudTemplateHeaders existingHeader = crCloudTemplateHeadersRepository.findById(crCldTemplateRes.getTemplateId()).get();
                        updatePreLoadCldSetupStatus(crPreLoadCldSetupRes.getSetupId(), PreLoadJobStatus.CREATE_CLD_STGTABLE_SUCCESS, existingHeader.getStagingTableName(), null,userId);
                    } else {
                        updatePreLoadCldSetupStatus(crPreLoadCldSetupRes.getSetupId(),null, null, stgTablePo.getTableName(),userId);
                    }
                    log.info("end of creating cloud staging table {}", stgTablePo.getTableName());
                } catch (Exception e) {
                    log.error("Cloud staging table creation error --> objectId->{} -> {}",objectId, e.getMessage());
                    updatePreLoadCldSetupStatus(crPreLoadCldSetupRes.getSetupId(), null, null, e.getMessage(), userId);
                }
            }
        } catch (SQLTransientException e) {
            log.error("PreLoad cloud template error -> objectId->{} -> {}", objectId,e.getMessage());
            throw new SQLTransientException(e.getMessage());
        } catch (Exception e) {
            log.error("PreLoad cloud template error --> objectId->{} -> {}", objectId,e.getMessage());
            if (crPreLoadCldSetupRes != null || crPreLoadCldSetupRes.getSetupId() != null)
                updatePreLoadCldSetupStatus(crPreLoadCldSetupRes.getSetupId(), null, null, e.getMessage(), userId);
        } finally {
            // Clean up tenant context
            TenantContext.clear();
        }
    }

    private CrCloudTemplateHeaderReqPo createCldTemplateHdrReq(CrPreLoadCldSetupStatus crPreLoadCldSetupRes, Long objectId, Long projectId, Long metaDataTableId) {

        CrCloudTemplateHeaders existingCldTempHdr = crCloudTemplateHeadersRepository.findByTemplateName(crPreLoadCldSetupRes.getCldTemplateName());
        Long parentObjectId = crObjectsRepository.getParentObjectIdByObjectId(objectId);
        CrCloudTemplateHeaderReqPo cldTemplateReqPo = new CrCloudTemplateHeaderReqPo();
        if (existingCldTempHdr != null)
            cldTemplateReqPo.setTemplateId(existingCldTempHdr.getTemplateId());
        cldTemplateReqPo.setTemplateCode(crPreLoadCldSetupRes.getCldTemplateCode());
        cldTemplateReqPo.setTemplateName(crPreLoadCldSetupRes.getCldTemplateName());
        if (objectId != null)
            cldTemplateReqPo.setObjectId(objectId);
        if (parentObjectId != null)
            cldTemplateReqPo.setParentObjectId(parentObjectId);
        cldTemplateReqPo.setProjectId(projectId);
        cldTemplateReqPo.setVersion(cloudVersion);
        cldTemplateReqPo.setMetaDataTableId(metaDataTableId);
        return cldTemplateReqPo;
    }

    private SyncValidationTablesRes syncValidationTables(String validationTableName, HttpServletRequest request, int i, boolean isValidationTableSyncFailed) {
        log.info("Start of syncing validation table {}", validationTableName);
        String response = "Table" + i + ": ";
        String responseBody = null;
        SyncValidationTablesRes syncTableRes = new SyncValidationTablesRes();
        try {
            //check existence of validation table in pod
            boolean validationTableExistFlag=crDDLExecutorDaoImpl.checkValidationTableExists(validationTableName);
            if(validationTableExistFlag){
                //skipping creation of already existing validation table
                log.info("Validation table {} already exists", validationTableName);
                response=response+"SUCCESS";
            }else {
                //create cloud data processing request
                CrCloudReqPo crCloudReqPo = createDataProcessingRequest(validationTableName, request);
                //API call for cloud data processing
                HttpHeaders headers = new HttpHeaders();
                headers.set("Accept", MediaType.APPLICATION_JSON_VALUE);
                headers.setContentType(MediaType.APPLICATION_JSON);
                headers.set("Authorization", request.getHeader("Authorization"));
                HttpEntity<CrCloudReqPo> requestEntity = new HttpEntity<>(crCloudReqPo, headers);
                ResponseEntity<String> cloudDataApiResponse = restTemplate.exchange(cloudDataProcessUrl, HttpMethod.POST, requestEntity,
                        String.class);
                log.debug(cloudDataApiResponse.getBody());
                if (cloudDataApiResponse.getStatusCode() == HttpStatus.OK) {
                    responseBody = cloudDataApiResponse.getBody();
                }
                JSONObject jsonObject = new JSONObject(responseBody);
                JSONObject crCloudStatusInfo = jsonObject.getJSONObject("crCloudStatusInformation");
                long statusId = crCloudStatusInfo.getLong("statusId");

                JSONObject cloudDataProcess = jsonObject.getJSONObject("cloudDataProcess");
                long id = cloudDataProcess.getLong("id");

                while (true) {
                    JSONObject statusJson = utils.getCldJobStatus(id, statusId);
                    String status = statusJson.getString("status");
                    if (Objects.equals(status, "completed")) {
                        response = response + "SUCCESS";
                        log.info("Validation Table Sync status {} -> {}", validationTableName, "SUCCESS");
                        break;
                    }
                    if (Objects.equals(status, "error")) {
                        isValidationTableSyncFailed = true;
                        String errorMessage = statusJson.isNull("status_error_msg")
                                ? Status.ERROR.getStatus() : statusJson.getString("status_error_msg");
                        response = response + errorMessage;
                        log.error("Validation Table Sync error {} -> {}", validationTableName, errorMessage);
                        break;
                    }
                }
            }
        } catch (Exception e) {
            isValidationTableSyncFailed = true;
            log.error("Validation Table Sync error {} -> {}", validationTableName, e.getMessage());
            response = response + e.getMessage();
        }
        log.info("end of syncing validation table {}", validationTableName);
        syncTableRes.setErrorMessage(response);
        syncTableRes.setValidationTableSyncFailed(isValidationTableSyncFailed);
        return syncTableRes;
    }


    private CrCloudReqPo createDataProcessingRequest(String validationTableName,HttpServletRequest request){
        String sqlQuery = "select * from " + validationTableName + " where 1 = 2";
        Date date = new Date();
        CrCloudReqPo crCloudReqPo = new CrCloudReqPo();
        crCloudReqPo.setBatchSize(10000);
        crCloudReqPo.setPodId(Integer.parseInt(request.getHeader("X-TENANT-ID")));
        crCloudReqPo.setScheduledJobCall("N");
        crCloudReqPo.setSqlQuery(sqlQuery);
        crCloudReqPo.setTableName(validationTableName);
        crCloudReqPo.setDestinationType("Table Sync");
        crCloudReqPo.setCreationDate(date);
        crCloudReqPo.setCreatedBy("Convertrite-Core");
        crCloudReqPo.setLookUpFlag("N");
        crCloudReqPo.setIsInternalServiceCall(true);
        crCloudReqPo.setLastUpdateDate(date);
        crCloudReqPo.setDataSync("IntialLoad");
        crCloudReqPo.setApiCallFrom("Sync Validation Table without data");
        return crCloudReqPo;
    }
    private Long getLongValue(Integer value) {
        return (value != null) ? value.longValue() : null;
    }

    private void updatePreLoadCldSetupStatus(Long setupId, PreLoadJobStatus preLoadJobStatus, String cldStgTbleName, String errorMessage, String userId) {
        CrPreLoadCldSetupStatus preLoadCldSetupStatus = crPreLoadCldSetupStatusRepository.findById(setupId).get();
        if (StringUtils.isNotBlank(cldStgTbleName))
            preLoadCldSetupStatus.setCldStagingTableName(cldStgTbleName);
        if (preLoadJobStatus != null)
            preLoadCldSetupStatus.setCldSetUpStatus(preLoadJobStatus.getValue());
        preLoadCldSetupStatus.setCldSetUpErrorMessage(errorMessage);
        preLoadCldSetupStatus.setLastUpdatedDate(new Date());
        preLoadCldSetupStatus.setLastUpdatedBy(userId);
        crPreLoadCldSetupStatusRepository.save(preLoadCldSetupStatus);
    }

    @Recover
    public void recover(SQLTransientException e, ObjectsPo objectsPo, Long projectId, HttpServletRequest request) {
        log.info("All retry attempts to establish connection failed.");
        //preLoadAndSyncJobStatus(objectsPo, projectId, e.getMessage(),null, PreLoadJobStatus.DB_CONNECTION_ERROR.getValue());
    }

    /**
     * @param cldSourceColumnsPo
     * @param templateId
     * @return
     */
    private List<CrCloudTemplateColumnsReqPo> getCloudTemplateColsReqPo(CloudSourceColumnsPo cldSourceColumnsPo, Long templateId) {
        List<CrCloudTemplateColumnsReqPo> saveCloudTemplateColumnLi = new ArrayList<>();
        for (ColumnPo columnPo : cldSourceColumnsPo.getCloudColumns()) {
            CrCloudTemplateColumnsReqPo saveCloudTemplateColumnsPo = new CrCloudTemplateColumnsReqPo();
            CrCloudTemplateColumns crCldTemplateColumsRes = crCloudTemplateColumnsRepository
                    .findByTemplateIdAndColumnName(templateId, columnPo.getColumnName());
            if (crCldTemplateColumsRes != null)
                saveCloudTemplateColumnsPo.setColumnId(crCldTemplateColumsRes.getColumnId());
            saveCloudTemplateColumnsPo.setColumnName(columnPo.getColumnName());
            saveCloudTemplateColumnsPo.setColumnType(columnPo.getColumnType());
            saveCloudTemplateColumnsPo.setDescription(columnPo.getDescription());
            saveCloudTemplateColumnsPo.setInsertOrDelete("I");
            if (Objects.equals(columnPo.getColumnName(), "CREATED_BY")
                    || Objects.equals(columnPo.getColumnName(), "CREATION_DATE")
                    || Objects.equals(columnPo.getColumnName(), "LAST_UPDATED_BY")
                    || Objects.equals(columnPo.getColumnName(), "LAST_UPDATE_DATE")
                    || Objects.equals(columnPo.getColumnName(), "OBJECT_VERSION_NUMBER")) {
                saveCloudTemplateColumnsPo.setMappingType("Constant");
            } else
                saveCloudTemplateColumnsPo.setMappingType("As-Is");
            if (Objects.equals(columnPo.getColumnName(), "OBJECT_VERSION_NUMBER")) {
                saveCloudTemplateColumnsPo.setMappingValue1("1.0");
            } else
                saveCloudTemplateColumnsPo.setMappingValue1(null);
            if (Objects.equals(columnPo.getNullAllowedFlag(), "Y")) {
                saveCloudTemplateColumnsPo.setSelected("N");
            } else
                saveCloudTemplateColumnsPo.setSelected("M");
            saveCloudTemplateColumnsPo.setTemplateId(templateId);
            saveCloudTemplateColumnLi.add(saveCloudTemplateColumnsPo);
        }
        return saveCloudTemplateColumnLi;
    }

    public BasicResponsePo getPreLoadCldSetUpJobStatus(Long projectId) {
        log.info("getPreLoadCldSetUpJobStatus in service ##");
        BasicResponsePo responsePo = new BasicResponsePo();
        responsePo.setPayload(crPreLoadCldSetupStatusRepository.getByProjectId(projectId));
        return responsePo;
    }

    /**
     *
     * @param processValidationObjectReq
     * @param request
     * @param asyncProcessId
     */
    public void processValidationObjects(ProcessValidationObjectsReqPo processValidationObjectReq, HttpServletRequest request, Long asyncProcessId) {
        log.info("Start of processValidationObjects in service ##");
        CrPreLoadCldSetupStatus crPreLoadCldSetupRes = null;
        try {
            String userId = request.getHeader("userId");
            //Calling API to get validation objects
            Map<String, List<LinkedHashMap>> validationObjectRespMap = initiateGetValidationObjectsApi(processValidationObjectReq.getObjectIdsLi(), request, asyncProcessId);
            if (!validationObjectRespMap.isEmpty()) {
                for (Map.Entry<String, List<LinkedHashMap>> entry : validationObjectRespMap.entrySet()) {
                    Long objectId = Long.parseLong(entry.getKey());
                    log.info("validation sync objectId->{}", objectId);
                    crPreLoadCldSetupRes = crPreLoadCldSetupStatusRepository.findByObjectIdAndProjectId(objectId, processValidationObjectReq.getProjectId());
                    if (crPreLoadCldSetupRes.getValPkgStatus().equalsIgnoreCase(PreLoadJobStatus.SETUP_INITIATED.getValue())) {
                        if (entry.getValue().isEmpty()) {
                            //No Dependent Tables for this package
                            updatePreLoadValidationApiStatus(crPreLoadCldSetupRes.getSetupId(), PreLoadJobStatus.SYNC_VALIDATION_TABLE_SUCCESS, null, "No Dependent Tables for this package", userId);
                        } else {
                            String validationSyncTables =
                                    entry.getValue()
                                            .stream()
                                            .filter(x->{
                                                Object value = x.get("syncDependentTables");
                                                return value != null && StringUtils.isNotBlank(value.toString());
                                            })
                                            .map(x -> String.valueOf(x.get("syncDependentTables")).toUpperCase())
                                            .flatMap(syncTables -> Arrays.stream(syncTables.split(",")))
                                            .distinct()
                                            .collect(Collectors.joining(","));
                            if (StringUtils.isBlank(validationSyncTables)) {
                                //No Dependent Tables for this package
                                updatePreLoadValidationApiStatus(crPreLoadCldSetupRes.getSetupId(), PreLoadJobStatus.SYNC_VALIDATION_TABLE_SUCCESS, null, "No Dependent Tables for this package", userId);
                            } else {
                                String[] validationSyncTablesArr = validationSyncTables.split(",");
                                StringBuffer responseMessage = new StringBuffer();
                                boolean isValidationTableSyncFailed = false;
                                SyncValidationTablesRes syncValidationTablesRes = new SyncValidationTablesRes();
                                int i = 0;
                                for (String validationSyncTable : validationSyncTablesArr) {
                                    syncValidationTablesRes = syncValidationTables(validationSyncTable, request, i, isValidationTableSyncFailed);
                                    responseMessage.append(syncValidationTablesRes.getErrorMessage());
                                    if (i != validationSyncTablesArr.length - 1)
                                        responseMessage.append(", ");
                                    i++;
                                }
                                if (!syncValidationTablesRes.isValidationTableSyncFailed())
                                    updatePreLoadValidationApiStatus(crPreLoadCldSetupRes.getSetupId(), PreLoadJobStatus.SYNC_VALIDATION_TABLE_SUCCESS, validationSyncTables, responseMessage.toString(), userId);
                                else
                                    updatePreLoadValidationApiStatus(crPreLoadCldSetupRes.getSetupId(), null, validationSyncTables, responseMessage.toString(), userId);
                            }
                        }
                    }
                }
                //calling executeValidateObjects API
                initiateExecuteValidationObjectApi(processValidationObjectReq, request);
                //updating async process status table
                asyncProcessStatusService.endProcess(asyncProcessId,null,null, Status.COMPLETED.getStatus(), null, userId,null);
            }
        } catch (Exception e) {
            log.error("Error during processValidationObjects ->{}", e);
            asyncProcessStatusService.endProcess(asyncProcessId,null,null, Status.ERROR.getStatus(), e.getMessage(), request.getHeader("userId"), null);
        }
    }

    private void initiateExecuteValidationObjectApi(ProcessValidationObjectsReqPo processValidationObjectReq, HttpServletRequest request) {
        log.info("start of initiateExecuteValidationObjectApi ->{}", executeValidationObjectsUrl);
        ExecuteCustomObjectReq customObjectReq = new ExecuteCustomObjectReq();
        customObjectReq.setObjectIds(processValidationObjectReq.getObjectIdsLi());
        customObjectReq.setProjectId(processValidationObjectReq.getProjectId());
        customObjectReq.setPodId(Long.valueOf(request.getHeader("X-TENANT-ID")));
        HttpHeaders header = new HttpHeaders();
        header.setAccept(Arrays.asList(MediaType.APPLICATION_JSON));
        header.setContentType(MediaType.APPLICATION_JSON);
        log.info("bearerToken-->" + request.getHeader("Authorization"));
        header.set("Authorization", request.getHeader("Authorization"));
        header.set("X-Username", request.getHeader("userId"));
        //Calling RestApi for executing validation objects
        HttpEntity<ExecuteCustomObjectReq> requestEntity = new HttpEntity<>(customObjectReq, header);
        ResponseEntity<String> response = restTemplate.exchange(executeValidationObjectsUrl, HttpMethod.POST, requestEntity, String.class);
    }

    private void updatePreLoadValidationApiStatus(Long setupId, PreLoadJobStatus preLoadJobStatus, String valSyncTables, String errorMessage, String userId) {
        CrPreLoadCldSetupStatus preLoadCldSetupStatus = crPreLoadCldSetupStatusRepository.findById(setupId).get();
        preLoadCldSetupStatus.setValSyncTables(valSyncTables);
        if (preLoadJobStatus != null)
            preLoadCldSetupStatus.setValPkgStatus(preLoadJobStatus.getValue());
        preLoadCldSetupStatus.setValPkgErrorMessage(errorMessage);
        preLoadCldSetupStatus.setLastUpdatedDate(new Date());
        preLoadCldSetupStatus.setLastUpdatedBy(userId);
        crPreLoadCldSetupStatusRepository.save(preLoadCldSetupStatus);
    }

    private Map<String, List<LinkedHashMap>> initiateGetValidationObjectsApi(List<Long> objectLi, HttpServletRequest request, Long asyncProcessId) {
        log.info("Start of initiateGetValidationObjectsApi ->{}", getValidationObjectsApiUrl);
        HttpHeaders header = new HttpHeaders();
        Map<String, List<LinkedHashMap>> validationObjectResMap = new LinkedHashMap<>();
        header.setContentType(MediaType.APPLICATION_JSON);
        header.setAccept(Arrays.asList(MediaType.APPLICATION_JSON));
        log.info("bearerToken-->" + request.getHeader("Authorization"));
        header.set("Authorization", request.getHeader("Authorization"));
        Map<String, List<Long>> requestBody = new HashMap<>();
        requestBody.put("objectIdLi", objectLi);
        //Calling RestApi to get validation objects
        HttpEntity< Map<String, List<Long>>> requestEntity = new HttpEntity<>(requestBody, header);
        ResponseEntity<BasicResPo> response = restTemplate.exchange(getValidationObjectsApiUrl, HttpMethod.POST, requestEntity, BasicResPo.class);
        if (response.getBody().getPayload() != null) {
            validationObjectResMap = (Map<String, List<LinkedHashMap>>) response.getBody().getPayload();
        } else {
            log.error("initiateGetValidationObjectsApi->{}", response.getBody().getMessage());
            asyncProcessStatusService.endProcess(asyncProcessId,null,null, Status.ERROR.getStatus(), response.getBody().getMessage(), request.getHeader("userId"), null);
        }
        return validationObjectResMap;
    }

    public BasicResponsePo getPreloadSetupDtls(Long projectId)  {
        BasicResponsePo resPo = new BasicResponsePo();
        try {
            List<CRPreloadCldSetupResPo> res = crPreLoadCldSetupStatusRepository.fetchCldSetupStatusDetails(projectId);
            resPo.setPayload(res);
            resPo.setMessage("Successfully retrieved Cloud Setup Status details");
        }catch(Exception ex){
            log.error(" Exception :: {} " , ex.getMessage());
        }
        return resPo;
    }

    public BasicResponsePo getPreloadValidationSetupDtls(Long projectId)  {
        BasicResponsePo resPo = new BasicResponsePo();
        try {
            List<CRPreloadValidationSetupResPo> res = crPreLoadCldSetupStatusRepository.fetchValidationSetupStatusDetails(projectId);
            resPo.setPayload(res);
            resPo.setMessage("Successfully retrieved  Validation Setup Status details");
        }catch(Exception ex){
            log.error(" Exception :: {} " , ex.getMessage());
        }
        return resPo;
    }

}
