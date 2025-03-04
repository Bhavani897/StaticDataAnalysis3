package com.rite.products.convertrite.service;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rite.products.convertrite.enums.ScheduleJobStatus;
import com.rite.products.convertrite.enums.Status;
import com.rite.products.convertrite.model.*;
import com.rite.products.convertrite.multitenancy.config.tenant.hibernate.DynamicDataSourceBasedMultiTenantConnectionProvider;
import com.rite.products.convertrite.multitenancy.util.TenantContext;
import com.rite.products.convertrite.po.*;
import com.rite.products.convertrite.respository.*;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import com.rite.products.convertrite.utils.Utils;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.http.*;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletRequest;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@Service
@Slf4j
public class CrScheduleService {

    @Autowired
    private CrScheduleMasterRepository scheduleMasterRepository;
    @Autowired
    CrSourceTemplateHeadersRepo crSourceTemplateHeadersRepo;

    @Autowired
    private CrJobMasterRepository jobMasterRepository;
    @Autowired
    private CRTableSyncStatusRepository crTableSyncStatusRepository;
    @Autowired
    private CrObjectsRepository crObjectsRepository;
    @Autowired
    CrEbsConnectionServiceImpl crEbsConnectionServiceImpl;
    @Autowired
    CrEbsConnectionDetailsRepository crEbsConnectionDetailsRepository;
    @Autowired
    CrTemplateStateRepository crTemplateStateRepository;

    @Autowired
    private TaskScheduler taskScheduler;

    @Autowired
    CrDataService crDataService;

    @Autowired
    CrConversionService crConversionService;
    @Autowired
    Utils utils;
    @Autowired
    ErpIntegrationService erpIntegrationService;
    @Autowired
    CrScheduleJobViewRepository scheduleJobViewRepository;
    @Autowired
    CrCloudTemplateHeadersRepository cloudTemplateHeadersRepository;
    @Autowired
    DynamicDataSourceBasedMultiTenantConnectionProvider dynamicDataSourceBasedMultiTenantConnectionProvider;
    @Autowired
    CrProjectsObjectsRepo crProjectsObjectsRepo;
    @Value("${app.time.zone}")
    private String appTimeZone;
    @Value("${reissue.token.url}")
    private String reIssueTokenUrl;
    @Value("${file.upload-dir}")
    private String fileUploadUrl;

    public BasicResPo saveSchedule(CrDataSyncScheduleReqPo reqPo, List<MultipartFile> files, HttpServletRequest request) throws Exception {
        CrScheduleMaster scheduleMaster = new CrScheduleMaster();
        try {

            Date convertedDate = convertToDateWithZone(reqPo.getScheduleStartTime(), appTimeZone);
            long adjustedTime = convertedDate.getTime() + reqPo.getScheduleFrequency() * 60 * 1000;
            Date startTime = new Date(adjustedTime);
            log.info(String.valueOf(startTime));
            boolean isExistsByBatchName = scheduleMasterRepository.existsByBatchName(reqPo.getBatchName());
            boolean isExistsByScheduleName = scheduleMasterRepository.existsByScheduleName(reqPo.getScheduleName());
            if (isExistsByBatchName) {
                return new BasicResPo() {{
                    setStatusCode(HttpStatus.CONFLICT);
                    setStatus("error");
                    setMessage("Batch Name Already exists");
                }};
            }

            if (isExistsByScheduleName) {
                return new BasicResPo() {{
                    setStatusCode(HttpStatus.CONFLICT);
                    setStatus("error");
                    setMessage("Schedule Name Already exists");
                    setPayload(null);
                }};
            }
            JSONObject jsonFilePaths = placeFilesInTargetLocation(files, reqPo);

            scheduleMaster.setScheduleName(reqPo.getScheduleName());
            scheduleMaster.setSourceTemplateIdsList(reqPo.getSourceTemplateIdsList());
            scheduleMaster.setScheduleFrequency(reqPo.getScheduleFrequency());
            scheduleMaster.setBatchName(reqPo.getBatchName());
            scheduleMaster.setExtractionType(reqPo.getExtractionType());
            scheduleMaster.setIsDependent(reqPo.getIsDependent());
            scheduleMaster.setSourceSystemId(reqPo.getSourceSystemId());
            scheduleMaster.setTargetSystemId(reqPo.getTargetSystemId());
            scheduleMaster.setScheduleStartTime(startTime);
            scheduleMaster.setStatus(ScheduleJobStatus.PENDING.getCode());
            scheduleMaster.setAuthToken(request.getHeader("Authorization"));
            scheduleMaster.setCreatedBy(request.getHeader("userId"));
            scheduleMaster.setCreatedDate(new Date());
            scheduleMaster.setFilePath(jsonFilePaths.toString());
            // Save the schedule
            scheduleMaster = scheduleMasterRepository.save(scheduleMaster);

            // Schedule the job
            scheduleJob(scheduleMaster, null, request);

        } catch (Exception e) {
            log.error("Error while scheduling the job: {}", e.getMessage(), e);
            return new BasicResPo() {{
                setStatusCode(HttpStatus.INTERNAL_SERVER_ERROR);
                setStatus("error");
                setMessage("Error while scheduling the job");
            }};
        }

        CrScheduleMaster finalSavedSchedule = scheduleMaster;
        return new BasicResPo() {{
            setStatusCode(HttpStatus.OK);
            setStatus("success");
            setMessage("Successfully scheduled the job");
            setPayload(finalSavedSchedule);
        }};
    }

    private JSONObject placeFilesInTargetLocation(List<MultipartFile> files, CrDataSyncScheduleReqPo reqPo) {
        log.info("--files received--{}" , files.size());
        log.info("--reqPo--{}" , reqPo);

        // Get the list of template IDs from the request object
        String[] templateIds = reqPo.getSourceTemplateIdsList().split(",");
        JSONObject jsonFilePaths = new JSONObject();
        // Iterate through each template ID
        for (String templateId : templateIds) {
            // Retrieve the file names associated with this template from the templateFiles map
            List<String> templateFiles = reqPo.getTemplateFiles().get(Integer.parseInt(templateId));
            log.info("--templateFiles for Template ID " + templateId + "--" + templateFiles.size());

            // Filter the files for this template by checking the file names
            //  List<MultipartFile> templateFileList = new ArrayList<>();

            for (String templateFileName : templateFiles) {
                log.info("Matching template file: {}" , templateFileName);
                for (MultipartFile file : files) {
                    log.info("Checking file: {}" , file.getOriginalFilename());
                    if (file.getOriginalFilename().equals(templateFileName)) {
                        log.info(templateId + " Matched file: " + file.getOriginalFilename());
                        // Process the file...
                        String filePath = createDirectoryIfNotExists(fileUploadUrl, file, templateId);
                        jsonFilePaths.put(templateId,filePath);
                    }
                }
            }
        }
        log.info("jsonFilePaths--{}", jsonFilePaths);
        return jsonFilePaths;
    }

    // Method to check and create the directory
    public String createDirectoryIfNotExists(String directoryPath, MultipartFile file, String templateId) {

        // Check the operating system
        String osName = System.getProperty("os.name").toLowerCase();

        // Normalize the directory path to ensure cross-platform compatibility
        if (osName.contains("win")) {
            directoryPath = directoryPath.replace("/", File.separator);  // Ensure the path uses the correct separator for Windows
        } else {
            directoryPath = directoryPath.replace("\\", File.separator);  // Ensure the path uses the correct separator for Linux/Mac
        }

        // Create a File object for the parent directory
        File dir = new File(directoryPath);

        // Retrieve the template state information
        CrTemplateStateView crTemplateStateView = crTemplateStateRepository.findBySrcTemplateId(Long.valueOf(templateId));
        String projectName = crTemplateStateView.getProjectName();
        String moduleName = crTemplateStateView.getModuleName();
        String objectName = crTemplateStateView.getObjectName();

        // Define the full path for the nested directories
        String fullPath = directoryPath + File.separator + projectName + File.separator + moduleName + File.separator + objectName;

        // Ensure the directory exists for each level of the hierarchy
        File parentDir = new File(directoryPath);
        if (!parentDir.exists()) {
            boolean created = parentDir.mkdirs();  // Create parent directory, including any necessary parents
            if (created) {
                log.info("Parent directory created: " + directoryPath);
            } else {
                log.error("Failed to create parent directory: " + directoryPath);
                return null;
            }
        }

        File projectDir = new File(parentDir, projectName);
        if (!projectDir.exists()) {
            boolean created = projectDir.mkdirs();  // Create the project directory
            if (created) {
                log.info("Project directory created: {}" , projectDir.getPath());
            } else {
                log.error("Failed to create project directory: {}" , projectDir.getPath());
                return null;
            }
        }

        File moduleDir = new File(projectDir, moduleName);
        if (!moduleDir.exists()) {
            boolean created = moduleDir.mkdirs();  // Create the module directory
            if (created) {
                log.info("Module directory created: {}" , moduleDir.getPath());
            } else {
                log.error("Failed to create module directory: {}" , moduleDir.getPath());
                return null;
            }
        }

        File objectDir = new File(moduleDir, objectName);
        if (!objectDir.exists()) {
            boolean created = objectDir.mkdirs();  // Create the object directory
            if (created) {
                log.info("Object directory created: {}" , objectDir.getPath());
            } else {
                log.error("Failed to create object directory: {}" , objectDir.getPath());
                return null;
            }
        }
        String filePath = null;
        // Check if the file is provided and save it to the object directory
        if (file != null) {
            // Use File.separator to build a valid path on both Windows and Linux
            File fileToSave = new File(objectDir, file.getOriginalFilename());
            try {
                file.transferTo(fileToSave);  // Save the file
                filePath = fileToSave.getPath();
                log.info("File saved to: {}" , objectDir);
                log.info("Absolute File path saved to: {}" , filePath);
            } catch (IOException e) {
                log.error("Failed to save the file: {}" , e.getMessage());
            }
        }
        return objectDir.getPath();
    }


    public static Date convertToDateWithZone(String startTime, String timeZone) {
        // Use a flexible DateTimeFormatter to handle both ' ' and 'T' as separators
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("[yyyy-MM-dd HH:mm:ss][yyyy-MM-dd'T'HH:mm:ss]");

        // Parse the input string using the formatter
        LocalDateTime localDateTime = LocalDateTime.parse(startTime, formatter);

        // Convert to ZonedDateTime with the specified time zone
        ZonedDateTime zonedDateTime = localDateTime.atZone(ZoneId.of(timeZone));

        // Convert to Date
        return Date.from(zonedDateTime.toInstant());
    }

    private void scheduleJob(CrScheduleMaster schedule, String tenant, HttpServletRequest request) {
        String tenantId = null;
        if (tenant != null) {
            tenantId = tenant;
        }
        if (request != null) {
            // Extract tenant ID from request headers
            tenantId = request.getHeader("X-TENANT-ID");
        }
        log.info("tenantId --- {}", tenantId);

        if (tenantId == null) {
            log.error("Tenant ID not found in the request header.");
            return;
        }

        // Set tenant context before scheduling the job
        TenantContext.setTenantId(tenantId);

        // Format the schedule start time
        TimeZone timeZone = TimeZone.getDefault();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        sdf.setTimeZone(timeZone);

        String formattedStartTime = sdf.format(schedule.getScheduleStartTime());
        log.info("Job has been scheduled at ------------- {}", formattedStartTime);

        // Schedule the task asynchronously with the tenant context
        String finalTenantId = tenantId;
        taskScheduler.schedule(() -> CompletableFuture.runAsync(() -> executeJob(schedule, schedule.getIsDependent(), finalTenantId, request)), schedule.getScheduleStartTime());
    }

    public void executeJob(CrScheduleMaster schedule, String isDependent, String tenantId, HttpServletRequest request) {
        log.info("executeJob  Tenant Id ---{}", tenantId);
        // Set tenant context before processing the job
        TenantContext.setTenantId(tenantId);
        try {
            if (tenantId == null) {
                log.error("Tenant ID is missing in the TenantContext during job execution.");
                return;
            }

            String res = reIssueAndSaveJwtTokenIfExpired(schedule.getAuthToken());
            if (!res.equalsIgnoreCase("Token is Not expired")) {
                schedule.setAuthToken("Bearer " + res);
            }
            schedule.setStatus(ScheduleJobStatus.DATA_LOAD_IN_PROGRESS.getCode());
            scheduleMasterRepository.save(schedule);
            log.info("schedule.getFilePath()---{}", schedule.getFilePath());
            // Step 1: Convert the JSON string to a JsonObject
            JSONObject jsonObject =new JSONObject(schedule.getFilePath());
            log.info("jsonObject--{}", jsonObject);
            // Split source template IDs
            List<Long> templateIds = Arrays.stream(schedule.getSourceTemplateIdsList().split(","))
                    .map(Long::parseLong)
                    .collect(Collectors.toList());

            // Process jobs
            List<CompletableFuture<Boolean>> jobFutures = new ArrayList<>();
            boolean hasError = false;
            int counter = 1; // Initialize a counter
            for (Long templateId : templateIds) {
                CrJobMaster job = new CrJobMaster();
                job.setScheduleId(schedule.getScheduleId());
                job.setSourceTemplateId(templateId);
                job.setStatus(ScheduleJobStatus.DATA_LOAD_IN_PROGRESS.getCode());
                job.setJobStartTime(new Date());

                CrTemplateStateView crTemplateStateView = crTemplateStateRepository.findBySrcTemplateId(templateId);
                job.setCloudTemplateId(crTemplateStateView.getTemplateId());
                job.setMetadataTableId(crTemplateStateView.getMetadataTableId());
                CrSourceTemplateHeaders crSourceTemplateHeaders = crSourceTemplateHeadersRepo.findByTemplateId(templateId);
                job.setSourceStagingTableName(crSourceTemplateHeaders.getStagingTableName());
                job.setObjectId(crTemplateStateView.getObjectId());
                job.setFilePath(jsonObject.get(templateId.toString()).toString());

                String batchName = generateBatchName(schedule.getBatchName());
                log.info("Generated Batch Name-----{}" + batchName);
                job.setBatchName(batchName);
                job.setJobStartTime(new Date());
                job.setCreatedDate(new Date());
                job.setCreatedBy(schedule.getCreatedBy());

                jobMasterRepository.save(job);
                if (!hasError) {
                    if (isDependent.equalsIgnoreCase("N")) {
                        log.info("==========Scheduling Async Job==========={}", schedule.getScheduleId());
                        CompletableFuture<Boolean> jobFuture = processJobAsync(job, schedule, crTemplateStateView.getTemplateName(), tenantId, request);
                        jobFutures.add(jobFuture);
                    } else {
                        log.info("==========Scheduling Sync Job==========={}", schedule.getScheduleId());
                        boolean success = processJobSync(job, schedule, crTemplateStateView.getTemplateName(), tenantId);
                        if (!success) {
                            hasError = true;
                            log.warn("==============Stopping the Schedule Job Execution because of an Error in one of the Dependent Object=============");
                            // break;
                        }
                    }
                }
            }

            // Final status update for the schedule
            updateScheduleStatus(schedule.getScheduleId(), tenantId);

        } catch (Exception e) {
            e.printStackTrace();
            log.error("Error in Execute Job----{}", e.getMessage());
        } finally {
            // Clear tenant context after job completion
            TenantContext.clear();
        }
    }

    public static String generateBatchName(String baseName) {
        // Get the current timestamp
        LocalDateTime now = LocalDateTime.now();

        // Format the timestamp to include only minutes
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd_HHmm");
        String timestamp = now.format(formatter);

        // Append the timestamp to the base name
        return baseName + "_" + timestamp;
    }

    public String reIssueAndSaveJwtTokenIfExpired(String currentToken) {
        log.info("===========reIssueAndSaveJwtTokenIfExpired=============");
        RestTemplate restTemplate = new RestTemplate();
        try {
            // Prepare the HTTP headers
            HttpHeaders headers = new HttpHeaders();
            headers.set("Authorization", currentToken.trim());
            // headers.setContentType(MediaType.APPLICATION_JSON);

            // Create the HTTP request
            HttpEntity<String> entity = new HttpEntity<>(null, headers);

            // Make the API call
            ResponseEntity<String> response = restTemplate.exchange(reIssueTokenUrl, HttpMethod.GET, entity, String.class);

            // Check the response status and return the new token
            if (response.getStatusCode() == HttpStatus.OK) {
                String res = response.getBody(); // Assuming the response body is the new token

                // Optionally save the token to your schedule or database
                log.info("Successfully refreshed JWT token: " + res);
                return res;
            } else {
                log.error("Failed to refresh JWT token. Status code: " + response.getStatusCode());
                throw new RuntimeException("Failed to refresh JWT token");
            }
        } catch (Exception e) {
            log.error("Error while refreshing JWT token", e);
            throw new RuntimeException("Error while refreshing JWT token", e);
        }
    }


    @Async
    public CompletableFuture<Boolean> processJobAsync(CrJobMaster job, CrScheduleMaster schedule, String cldTemplateName, String tenantId, HttpServletRequest request) {
        log.info("processJobAsync------ Job ID: {} , Schedule ID: {}", job.getJobId(), schedule.getScheduleId());

        try {
            boolean isSyncingCompleted = syncingStatusCheck(tenantId, 0L, job);
            if (!isSyncingCompleted) {
                return CompletableFuture.completedFuture(false);
            }

            boolean loadSuccess = extractAndLoadTheDataFromDb(job, schedule, tenantId);
            if (loadSuccess) {
                job.setStatus(ScheduleJobStatus.TRANSFORMATION_IN_PROGRESS.getCode());
                jobMasterRepository.save(job);
                boolean transformSuccess = transformData(job, schedule, cldTemplateName, tenantId);
                if (transformSuccess) {
                    job.setStatus(ScheduleJobStatus.IMPORT_IN_PROGRESS.getCode());
                    job.setJobEndTime(new Date());
                    jobMasterRepository.save(job);
                    boolean importSuccess = false;
                    Map<String, Long> map = importData(job, schedule, tenantId);
                    Long loadRequestId = map.get("loadRequestId");
                    Long isGroupingApplied = map.get("isGroupingApplied");
                    if (loadRequestId != null && loadRequestId > 0) {
                        importSuccess = true;
                    }
                    log.info("==========importSuccess========={}", importSuccess);
                    job.setStatus(importSuccess
                            ? ScheduleJobStatus.IMPORT_SUCCESS.getCode()
                            : ScheduleJobStatus.IMPORT_ERROR.getCode());
                    job.setJobEndTime(new Date());
                    job.setLoadRequestId(loadRequestId);
                    jobMasterRepository.save(job);
                    // boolean isSyncingCompleted = false;
                    if (importSuccess && isGroupingApplied != 1L) {
                        startSyncingProcess(tenantId, loadRequestId, job, true);
                    }
                    return CompletableFuture.completedFuture(importSuccess);
                } else {
                    log.info("==========transformData Failed=========");
                    job.setStatus(ScheduleJobStatus.TRANSFORMATION_ERROR.getCode());
                    job.setJobEndTime(new Date());
                    jobMasterRepository.save(job);
                }
            } else {
                job.setStatus(ScheduleJobStatus.DATA_LOAD_ERROR.getCode());
                job.setJobEndTime(new Date());
                jobMasterRepository.save(job);
            }
        } catch (Exception e) {
            job.setErrorMessage(e.getMessage());
            job.setJobEndTime(new Date());
            jobMasterRepository.save(job);
        }
        return CompletableFuture.completedFuture(false);
    }

    public boolean processJobSync(CrJobMaster job, CrScheduleMaster schedule, String cldTemplateName, String tenantId) {
        log.info("processJobSync------ Job ID: {} , Schedule ID: {}", job.getJobId(), schedule.getScheduleId());
        try {
            log.info("==========extractAndLoadTheData started=========");
            boolean isSyncingCompleted = syncingStatusCheck(tenantId, 0L, job);
            if (!isSyncingCompleted) {
                return false;
            }
            boolean loadSuccess = extractAndLoadTheDataFromDb(job, schedule, tenantId);
            if (loadSuccess) {
                job.setStatus(ScheduleJobStatus.TRANSFORMATION_IN_PROGRESS.getCode());
                job.setJobEndTime(new Date());
                jobMasterRepository.save(job);

                boolean transformSuccess = transformData(job, schedule, cldTemplateName, tenantId);
                if (transformSuccess) {
                    job.setStatus(ScheduleJobStatus.IMPORT_IN_PROGRESS.getCode());
                    job.setJobEndTime(new Date());
                    jobMasterRepository.save(job);

                    boolean importSuccess = false;
                    Map<String, Long> map = importData(job, schedule, tenantId);
                    Long loadRequestId = map.get("loadRequestId");
                    Long isGroupingApplied = map.get("isGroupingApplied");
                    if (loadRequestId != null && loadRequestId > 0) {
                        importSuccess = true;
                    }
                    log.info("==========importSuccess========={}", importSuccess);
                    job.setStatus(importSuccess
                            ? ScheduleJobStatus.IMPORT_SUCCESS.getCode()
                            : ScheduleJobStatus.IMPORT_ERROR.getCode());
                    job.setJobEndTime(new Date());
                    job.setLoadRequestId(loadRequestId);
                    jobMasterRepository.save(job);
                    //  boolean isSyncingCompleted = false;
                    if (importSuccess && isGroupingApplied != 1L) {
                        startSyncingProcess(tenantId, loadRequestId, job, true);
                    }
                    //  isSyncingCompleted = true;
                    return importSuccess;
                } else {
                    job.setStatus(ScheduleJobStatus.TRANSFORMATION_ERROR.getCode());
                    job.setJobEndTime(new Date());
                    jobMasterRepository.save(job);
                }
            } else {
                job.setStatus(ScheduleJobStatus.DATA_LOAD_ERROR.getCode());
                job.setJobEndTime(new Date());
                jobMasterRepository.save(job);
            }
        } catch (Exception e) {
            log.info("Exception in processJobSync----{}", e.getMessage());
            //  job.setStatus(ScheduleJobStatus.DATA_LOAD_ERROR.getCode());
            job.setErrorMessage(e.getMessage());
            job.setJobEndTime(new Date());
            jobMasterRepository.save(job);
        }
        return false;
    }

    private boolean syncingStatusCheck(String tenantId, Long loadRequestId, CrJobMaster job) {
        boolean isDataSyncCompleted = false;
        log.info("=======startSyncingProcess=====");

        // Trigger the syncing process
        startSyncingProcess(tenantId, loadRequestId, job, false);

        log.info("=======checking sync Status=====");
        CrObjects crObjects = crObjectsRepository.findByObjectId(job.getObjectId());
        log.info("=======BaseTable====={}", crObjects != null ? crObjects.getBaseTables() : "null");

        if (crObjects != null && crObjects.getDependentTables() != null) {
            // Retrieve the dependent columns as a comma-separated string
            String dependentColumns = crObjects.getDependentTables(); // This should return a comma-separated string
            if (dependentColumns != null && !dependentColumns.isEmpty()) {
                // Split the dependent columns into an array
                String[] columns = dependentColumns.split(",");
                List<CompletableFuture<Boolean>> futures = new ArrayList<>();

                // Asynchronously check the sync status for each column
                for (String column : columns) {
                    CompletableFuture<Boolean> syncFuture = checkSyncStatusAsync(job, column.trim());
                    futures.add(syncFuture);
                }

                // Wait for all async tasks to complete
                CompletableFuture<Void> allOf = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
                allOf.join(); // Block until all futures are completed

                // After all checks, determine if all syncs are completed successfully
                boolean allCompleted = futures.stream().allMatch(CompletableFuture::join);
                if (allCompleted) {
                    log.info("All syncs completed successfully for jobId {}", job.getJobId());
                    isDataSyncCompleted = true;
                } else {
                    log.info("One or more syncs failed for jobId {}", job.getJobId());
                    isDataSyncCompleted = false;
                }
            } else {
                // Handle case where there are no dependent columns
                log.info("No dependent columns found for objectId {}", job.getObjectId());
                isDataSyncCompleted = true; // No syncing required
            }
        } else {
            log.info("-----No need to sync since base table is null in cr objects table for object id ----{}", job.getObjectId());
            job.setStatus(ScheduleJobStatus.DATA_SYNC_SUCCESS.getCode());
            job.setJobEndTime(new Date());
            jobMasterRepository.save(job);
            isDataSyncCompleted = true;
        }
        return isDataSyncCompleted;
    }

    // Asynchronous method to check sync status for each dependent column
    private CompletableFuture<Boolean> checkSyncStatusAsync(CrJobMaster job, String table) {
        return CompletableFuture.supplyAsync(() -> {
            boolean isSyncCompleted = false;
            try {
                log.info("=======waiting for syncing to be completed for table====={}", table);
                // Wait for the status to become COMPLETED for the given column
                while (true) {
                    CRTableSyncStatus crTableSyncStatus = crTableSyncStatusRepository.findByJobIdAndTableName(job.getJobId(), table);

                    if (crTableSyncStatus != null && crTableSyncStatus.getStatus().equalsIgnoreCase(Status.COMPLETED.getStatus())) {
                        job.setStatus(ScheduleJobStatus.DATA_SYNC_SUCCESS.getCode());
                        job.setJobEndTime(new Date());
                        jobMasterRepository.save(job);
                        log.info("=======syncing completed for table====={}", table);
                        isSyncCompleted = true;
                        break;
                    } else if (crTableSyncStatus != null && crTableSyncStatus.getStatus().equalsIgnoreCase(Status.ERROR.getStatus())) {
                        job.setStatus(ScheduleJobStatus.DATA_SYNC_ERROR.getCode());
                        job.setJobEndTime(new Date());
                        jobMasterRepository.save(job);
                        log.info("=======syncing Error for table====={}", table);
                        isSyncCompleted = false;
                        break;
                    }

                    // Sleep for a short duration before rechecking
                    log.info("=======retrying for the syncing to be completed for table====={}", table);
                    Thread.sleep(10000); // 10 second delay
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("Thread interrupted while waiting for status to complete for table--{} - {}", table, e.getMessage());
                job.setStatus(ScheduleJobStatus.DATA_SYNC_ERROR.getCode());
                job.setJobEndTime(new Date());
                jobMasterRepository.save(job);
                isSyncCompleted = false;
            }
            return isSyncCompleted;
        });
    }

    private void startSyncingProcess(String tenantId, Long loadRequestId, CrJobMaster job, boolean isResync) {
        if (!isResync) {
            job.setStatus(ScheduleJobStatus.DATA_SYNC_IN_PROGRESS.getCode());
        }
        job.setJobEndTime(new Date());
        jobMasterRepository.save(job);
        try {
            crDataService.cloudJobStatusCheck(tenantId, loadRequestId, job.getJobId());
        } catch (Exception e) {
            if (!isResync) {
                job.setStatus(ScheduleJobStatus.DATA_SYNC_ERROR.getCode());
            }
            job.setErrorMessage(e.getMessage());
            job.setJobEndTime(new Date());
            jobMasterRepository.save(job);
            log.error(job.getJobId() + " Error while Syncing ---- {}", e.getMessage());
        }

    }

    private void updateScheduleStatus(Long scheduleId, String tenantId) {
        log.info("------------updateScheduleStatus for scheduleId-----------{}", scheduleId);
        // Fetch all jobs associated with the scheduleId
        List<CrJobMaster> jobs = jobMasterRepository.findAllByScheduleId(scheduleId);
        jobs.stream()
                .allMatch(job -> {
                    log.info(job.getJobId() + "-" + job.getStatus());
                    return true;
                });
        boolean anyDataLoadError = jobs.stream()
                .anyMatch(job -> job.getStatus().equalsIgnoreCase(ScheduleJobStatus.DATA_LOAD_ERROR.getCode()));

        boolean anyTransformError = jobs.stream()
                .anyMatch(job -> job.getStatus().equalsIgnoreCase(ScheduleJobStatus.TRANSFORMATION_ERROR.getCode()));

        boolean anyImportError = jobs.stream()
                .anyMatch(job -> job.getStatus().equalsIgnoreCase(ScheduleJobStatus.IMPORT_ERROR.getCode()));
        boolean anySyncingError = jobs.stream()
                .anyMatch(job -> job.getStatus().equalsIgnoreCase(ScheduleJobStatus.DATA_SYNC_ERROR.getCode()));

        boolean anyImportSuccess = jobs.stream()
                .anyMatch(job -> job.getStatus().equalsIgnoreCase(ScheduleJobStatus.IMPORT_SUCCESS.getCode()));
        boolean allImportSuccess = jobs.stream()
                .allMatch(job -> job.getStatus().equalsIgnoreCase(ScheduleJobStatus.IMPORT_SUCCESS.getCode()));

//        boolean allSyncingSuccess = jobs.stream()
//                .allMatch(job -> job.getStatus().equalsIgnoreCase(ScheduleJobStatus.DATA_SYNC_SUCCESS.getCode()));
//        boolean anySyncingSuccess = jobs.stream()
//                .anyMatch(job -> job.getStatus().equalsIgnoreCase(ScheduleJobStatus.DATA_SYNC_SUCCESS.getCode()));

        // Fetch the schedule record
        CrScheduleMaster schedule = scheduleMasterRepository.findById(scheduleId).orElseThrow();

        log.info("Any Data Load Error: {}", anyDataLoadError);
        log.info("Any Transform Error: {}", anyTransformError);
        log.info("Any Import Error: {}", anyImportError);
        log.info("Any Sync Error: {}", anySyncingError);
        log.info("Any Import Success: {}", anyImportSuccess);
        log.info("All Import Success: {}", allImportSuccess);
//        log.info("Any Sync Success: {}", anySyncingSuccess);
//        log.info("All Sync Success: {}", allSyncingSuccess);
        // Determine the schedule status based on the job statuses
        if (!anyDataLoadError && !anyTransformError && !anyImportError && !anySyncingError && allImportSuccess) {
            schedule.setStatus(ScheduleJobStatus.SUCCESS.getCode()); // All stages successful

        } else if (anyDataLoadError || anyTransformError || anyImportError || anySyncingError && !anyImportSuccess) {
            schedule.setStatus(ScheduleJobStatus.ERROR.getCode()); // Any stage with error
        } else if ((anyDataLoadError || anyTransformError || anyImportError || anyImportError) && anyImportSuccess) {
            schedule.setStatus(ScheduleJobStatus.WARNING.getCode()); // Mixed statuses
        }

        // Save the updated schedule status
        schedule.setScheduleEndTime(new Date());
        scheduleMasterRepository.save(schedule);

        if (schedule.getStatus().equalsIgnoreCase(ScheduleJobStatus.SUCCESS.getCode())) {
            createNextSchedule(schedule, tenantId);
        }
    }

    private void createNextSchedule(CrScheduleMaster schedule, String tenantId) {
        log.info("==========createNextSchedule==========");
        try {
//            String scheduleEndTime=convertToDateAndTime(schedule.getScheduleEndTime().toString());
//            Date convertedDate = convertToDateWithZone(scheduleEndTime, appTimeZone);
            long adjustedTime = schedule.getScheduleEndTime().getTime() + schedule.getScheduleFrequency() * 60 * 1000;
            Date startTime = new Date(adjustedTime);
            schedule.setScheduleStartTime(startTime);
            schedule.setStatus(ScheduleJobStatus.PENDING.getCode());
            schedule.setCreatedDate(new Date());
            // null values to be set to consider as new record
            schedule.setScheduleEndTime(null);
            schedule.setScheduleId(null);
            log.info("==========schedule=========={}", schedule);
            CrScheduleMaster scheduleMaster = scheduleMasterRepository.save(schedule);

            // Schedule the job
            scheduleJob(scheduleMaster, tenantId, null);
        } catch (Exception e) {
            log.error("Exception in createNextSchedule---{}", e.getMessage());
            e.printStackTrace();
        }
    }

    private boolean transformData(CrJobMaster job, CrScheduleMaster schedule, String cldTemplateName, String tenantId) {
        try {
            log.info("==========transformData started=========");
            //Make it dynamic and change it to Y for re-transformation
            String pBatchFlag = "N";
            String pReprocessFlag = "N";
            String userId = schedule.getCreatedBy();
            ResponseEntity<Object> res = crConversionService.transformDataToCloud(cldTemplateName, pReprocessFlag, pBatchFlag, job.getBatchName(), tenantId, userId);
            log.info("transformDataToCloud res----{}" + res);
            log.info("transformDataToCloud res.getBody----{}" + res.getBody());
            // Convert the response body to List<Map<String, String>>
            ObjectMapper objectMapper = new ObjectMapper();
            Map<String, String> response = objectMapper.convertValue(
                    res.getBody(),
                    new TypeReference<Map<String, String>>() {
                    }
            );
            log.info("response----{}", response);

            String requestId = response.get("requestId");
            String responseCode = response.get("responseCode");

            log.info("Request ID: " + requestId);

            if (!"SUCCESS".equalsIgnoreCase(responseCode)) {
                return false;
            }
            String status = utils.getProcessStatus(Long.valueOf(requestId), tenantId);
            log.info("status------{}", status);
            if ("C".equalsIgnoreCase(status)) {
                job.setStatus(ScheduleJobStatus.TRANSFORMATION_SUCCESS.getCode());
                job.setJobEndTime(new Date());
                jobMasterRepository.save(job);
                return true;
            } else if ("CE".equalsIgnoreCase(status)) {
                job.setStatus(ScheduleJobStatus.TRANSFORMATION_ERROR.getCode());
                job.setJobEndTime(new Date());
                jobMasterRepository.save(job);
                return false;
            } else if ("I".equalsIgnoreCase(status)) {
                job.setStatus(ScheduleJobStatus.TRANSFORMATION_IN_PROGRESS.getCode());
                job.setJobEndTime(new Date());
                jobMasterRepository.save(job);
                return false;
            } else {
                return false;
            }

        } catch (Exception e) {
            log.error("Exception in transformData-----{}", e.getMessage());
            e.printStackTrace();
            return false;
        }
    }

    private Map importData(CrJobMaster job, CrScheduleMaster crScheduleMaster, String podId) {
        Long loadRequestId = null;
        Map map = new HashMap<String, Long>();
        LoadandImportDataReqPo loadandImportDataReqPo = new LoadandImportDataReqPo();
        LoadandImportDataResPo loadandImportDataResPo = new LoadandImportDataResPo();
        CrCloudTemplateHeaders crCloudTemplateHeaders = new CrCloudTemplateHeaders();
        try {
            log.info("==========importData started=========");
            Optional<CrCloudTemplateHeaders> cloudTemplateHeadersOptional = cloudTemplateHeadersRepository.findById(job.getCloudTemplateId());
            if (cloudTemplateHeadersOptional.isPresent()) {
                crCloudTemplateHeaders = cloudTemplateHeadersOptional.get();
            }
            CrProjectsObjects crProjectsObjects = crProjectsObjectsRepo.findByObjectId(Math.toIntExact(job.getObjectId()));
            Connection con = dynamicDataSourceBasedMultiTenantConnectionProvider.getConnection(podId);
            String query = "SELECT  DISTINCT CR_PARAMETERS_LIST FROM " + crCloudTemplateHeaders.getStagingTableName() + " WHERE CR_BATCH_NAME='" + job.getBatchName() + "'";
            log.info("==========query========={}", query);
            PreparedStatement stmnt = con.prepareStatement(query);
            ResultSet rs = stmnt.executeQuery();
            while (rs.next()) {
                String parameterList = rs.getString("CR_PARAMETERS_LIST");
                //  parameterList = "No,New,BatchName";
                log.info("parameter List:" + parameterList);
                if (parameterList == null) {
                    String errorMsg = "parameter list cannot be null for cldtemplate name :" + crCloudTemplateHeaders.getTemplateName();
                    log.error(errorMsg);
                    job.setErrorMessage(errorMsg);
                    job.setStatus(ScheduleJobStatus.IMPORT_ERROR.getCode());
                    jobMasterRepository.save(job);
                    return null;
                }
                loadandImportDataReqPo.setBatchName(job.getBatchName());
                loadandImportDataReqPo.setCloudTemplateId(job.getCloudTemplateId());
                loadandImportDataReqPo.setObjectName(crProjectsObjects.getObjectName());
                loadandImportDataReqPo.setParameterList(parameterList);
                loadandImportDataReqPo.setPodId(Long.valueOf(podId));
                loadandImportDataResPo = erpIntegrationService.loadAndImportDataV2(loadandImportDataReqPo, crScheduleMaster.getAuthToken());
            }

            if (loadandImportDataResPo != null && loadandImportDataResPo.getResultId() > 0) {
                loadRequestId = loadandImportDataResPo.getResultId();
                map.put("isGroupingApplied", 0L);
            } else if (loadandImportDataResPo != null && loadandImportDataResPo.getMessage().contains("Objects Grouping Applied")) {
                loadRequestId = extractResultId(loadandImportDataResPo.getMessage());
                map.put("isGroupingApplied", 1L);
            }
            map.put("loadRequestId", loadRequestId);
            log.info("==========loadRequestId========{}", loadRequestId);
            return map;
        } catch (Exception e) {
            log.error("Error in importData-----{}", e.getMessage());
            job.setErrorMessage(e.getMessage());
            job.setStatus(ScheduleJobStatus.IMPORT_ERROR.getCode());
            jobMasterRepository.save(job);
            return null;
        }
    }

    public Long extractResultId(String input) {
        // Split the string using "->" as the delimiter
        String[] parts = input.split("->");
        if (parts.length > 1) {
            return Long.parseLong(parts[1].trim()); // Return the part after "->"
        }
        return null; // Return null if the format is unexpected
    }

    private boolean extractAndLoadTheDataFromDb(CrJobMaster job, CrScheduleMaster schedule, String tenantId) {
        log.info("==========extractAndLoadTheData started=========");
        CrLoadDataFromEbsReqPo crLoadDataFromEbsReqPo = new CrLoadDataFromEbsReqPo();
        crLoadDataFromEbsReqPo.setSrcTemplateId(job.getSourceTemplateId());
        Optional<CrEbsConnectionDetails> res = crEbsConnectionDetailsRepository.findById(schedule.getSourceSystemId());
        if (res.isPresent()) {
            CrEbsConnectionDetails crEbsConnectionDetails = res.get();
            crLoadDataFromEbsReqPo.setConnectionName(crEbsConnectionDetails.getConnectionName());
        }
        crLoadDataFromEbsReqPo.setBatchName(job.getBatchName());

        try {
            String userId = schedule.getCreatedBy();
            BasicResponsePo resPo = crEbsConnectionServiceImpl.loadSrcDataFromEbs(crLoadDataFromEbsReqPo, tenantId, userId);
            log.info("resPo.getError()----{}", resPo.getError());
            log.info("resPo.getPayload()----{}", resPo.getPayload());
            if (resPo.getError() == null && resPo.getPayload() != null) {
                job.setStatus(ScheduleJobStatus.DATA_LOAD_SUCCESS.getCode());
                job.setJobEndTime(new Date());
                jobMasterRepository.save(job);
                return true;
            } else {
                job.setStatus(ScheduleJobStatus.DATA_LOAD_ERROR.getCode());
                job.setErrorMessage(resPo.getError());
                jobMasterRepository.save(job);
                return false;
            }
        } catch (Exception e) {
            log.error("Exception in extractAndLoadTheData()-----{}", e.getMessage());
            job.setStatus(ScheduleJobStatus.DATA_LOAD_ERROR.getCode());
            job.setErrorMessage(e.getMessage());
            jobMasterRepository.save(job);
            return false;
        }
    }

    public Page<CrScheduleJobView> getAllScheduleJobViews(Pageable pageable, List<String> statuses) {
        return scheduleJobViewRepository.findAllByStatusIn(pageable, statuses);
    }

    public BasicResPo terminateSchedule(Long scheduleId, HttpServletRequest request) {
        try {
            List<CrJobMaster> jobs = jobMasterRepository.findAllByScheduleId(scheduleId);
            for (CrJobMaster job : jobs) {
                job.setStatus(ScheduleJobStatus.TERMINATED.getCode());
            }
            jobMasterRepository.saveAll(jobs);

            Optional<CrScheduleMaster> scheduleMasterOpt = scheduleMasterRepository.findById(scheduleId);
            if (!scheduleMasterOpt.isPresent()) {
                return new BasicResPo() {{
                    setStatusCode(HttpStatus.NOT_FOUND);
                    setStatus("error");
                    setMessage("Schedule doesn't Exists");
                }};
            }
            CrScheduleMaster scheduleMaster = scheduleMasterOpt.get();
            scheduleMaster.setStatus(ScheduleJobStatus.TERMINATED.getCode());
            scheduleMaster.setScheduleEndTime(new Date());
            scheduleMaster.setLastUpdatedBy(request.getHeader("userId"));
            scheduleMasterRepository.save(scheduleMaster);
            return new BasicResPo() {{
                setStatusCode(HttpStatus.OK);
                setStatus("success");
                setMessage("Scheduled Jobs Terminated Successfully");
                setPayload(scheduleMaster);
            }};
        } catch (Exception e) {
            log.error("Exception in terminateSchedule()-----{}", e.getMessage());
            return new BasicResPo() {{
                setStatusCode(HttpStatus.INTERNAL_SERVER_ERROR);
                setStatus("error");
                setMessage(e.getMessage());
            }};
        }
    }
}

