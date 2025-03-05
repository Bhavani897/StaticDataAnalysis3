package com.rite.products.convertrite.service;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rite.products.convertrite.enums.Status;
import com.rite.products.convertrite.exception.ValidationException;
import com.rite.products.convertrite.model.*;
import com.rite.products.convertrite.model.CrSupplierTaxProfileErrors;
import com.rite.products.convertrite.multitenancy.config.tenant.hibernate.DynamicDataSourceBasedMultiTenantConnectionProvider;
import com.rite.products.convertrite.po.*;
import com.rite.products.convertrite.respository.*;
import com.rite.products.convertrite.stubs.accountcombinationservice.AccountCombinationServiceStub;
import com.rite.products.convertrite.stubs.accountcombinationservice.AccountCombinationServiceStub.AccountValidationInput;
import com.rite.products.convertrite.stubs.accountcombinationservice.AccountCombinationServiceStub.AccountValidationOutput;
import com.rite.products.convertrite.stubs.accountcombinationservice.AccountCombinationServiceStub.ValidateAndCreateAccounts;
import com.rite.products.convertrite.stubs.accountcombinationservice.AccountCombinationServiceStub.ValidateAndCreateAccountsResponse;
import com.rite.products.convertrite.utils.Utils;
import com.rite.products.convertrite.utils.ValidateAndCreateClass;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import lombok.extern.slf4j.Slf4j;
import org.apache.axis2.client.ServiceClient;
import org.apache.axis2.transport.http.HTTPConstants;
import org.apache.axis2.transport.http.impl.httpclient4.HttpTransportPropertiesImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.*;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

@Service
@Slf4j
public class CrCldImportCustomRestApisServiceImpl {

    @Autowired
    RestTemplate restTemplate;
    @Autowired
    CrValidateCvrCcidRepository validateCvrCcidRepository;
    @Autowired
    CrProjDffErrorRepository crProjDffErrorRepository;
    @Autowired
    CrSupplierTaxProfileErrorsRepository crSupplierTaxProfileErrorsRepository;

    @Autowired
    ProcessJobDaoImpl processJobDaoImpl;

    @Autowired
    CrCreateBankBranchErrorsRepository crCreateBankBranchErrorsRepository;
    @Autowired
    CrCloudTemplateHeadersViewRepository cloudTemplateHeadersViewRepository;
    @Autowired
    DynamicDataSourceBasedMultiTenantConnectionProvider dynamicDataSourceBasedMultiTenantConnectionProvider;
    @Value("${batch-size}")
    private long batchSize;
    @Autowired
    Utils utils;
    @Autowired
    ObjectMapper objectMapper;
    @Autowired
    CrBankAccountErrorsRepository crBankAccountErrorsRepository;
    @Value("${bank_account_cloud_url}")
    String bankAccountCloudUrl;
    @Value("${bank_cloud_url}")
    String bankCloudUrl;
    @Value("${branch_cloud_url}")
    String branchCloudUrl;
    @Autowired
    ValidateAndCreateClass validateAndCreateClass;
    @Autowired
    AsyncProcessStatusRepository asyncProcessStatusRepository;
    @Value("${thread-limit}")
    private int noOfThreads;
    @Autowired
    private AsyncProcessStatusService asyncProcessStatusService;
    @Autowired
    CrCloudJobStatusRepo crCloudJobStatusRepo;

    public void createBankAndBranches(CustomRestApiReqPo customRestApiReqPo) throws Exception {
        log.info("Start of createBankAndBranches Method in service ###");

        Connection con = null;
        CrCloudTemplateHeadersView crCloudTemplateHeadersView = cloudTemplateHeadersViewRepository
                .findById(customRestApiReqPo.getCldTemplateId()).get();
        HttpHeaders headers = new HttpHeaders();
        headers.set("Accept", MediaType.APPLICATION_JSON_VALUE);
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setBasicAuth(customRestApiReqPo.getCldUserName(), customRestApiReqPo.getCldPassword());
        List<CrCreateBankBranchErrors> createBankBranchErrorsLi = new ArrayList<>();
        String stagingTableName = crCloudTemplateHeadersView.getStagingTableName();

        try {
            CrCloudJobStatus crCloudJobStatus = insertIntoCrCloudJobStatus(customRestApiReqPo, Status.COMPLETED.getStatus());
            // create database connection
            log.info("TENANT-->" + customRestApiReqPo.getPodId());
            con = dynamicDataSourceBasedMultiTenantConnectionProvider.getConnection(String.valueOf(customRestApiReqPo.getPodId()));
            PreparedStatement stmnt = con.prepareStatement("select distinct bank_name,bank_number,attribute3 from "
                    + stagingTableName + " where CR_BATCH_NAME='" + customRestApiReqPo.getBatchName()
                    + "' and attribute1='N'");
            ResultSet rs = stmnt.executeQuery();
            while (rs.next()) {
                // CreateBank RestApi call
                createBank(rs, headers, createBankBranchErrorsLi, customRestApiReqPo);
            }
            PreparedStatement stmntBr = con.prepareStatement(
                    "select distinct bank_number,bank_name,BRANCH_NAME,BRANCH_NUMBER,attribute3,attribute4 from "
                            + stagingTableName + " where CR_BATCH_NAME='" + customRestApiReqPo.getBatchName()
                            + "' and attribute1 in('N','BA')");
            ResultSet rsBr = stmntBr.executeQuery();
            while (rsBr.next()) {
                // CreateBankBranch RestApi call
                createBankBranch(rsBr, createBankBranchErrorsLi, customRestApiReqPo, headers);
            }
            if (!createBankBranchErrorsLi.isEmpty())
                crCreateBankBranchErrorsRepository.saveAll(createBankBranchErrorsLi);
        } finally {
            if (con != null)
                con.close();
        }
    }

    private void createBankBranch(ResultSet rsBr, List<CrCreateBankBranchErrors> createBankBranchErrorsLi,
                                  CustomRestApiReqPo customRestApiReqPo,
                                  HttpHeaders headers) throws Exception {
        try {
            CrCreateBankBranchReqPo createBankBranchReqPo = new CrCreateBankBranchReqPo();
            createBankBranchReqPo.setBankName(rsBr.getString("BANK_NAME"));
            createBankBranchReqPo.setBankBranchName(rsBr.getString("BRANCH_NAME"));
            createBankBranchReqPo.setBranchNumber(rsBr.getString("BRANCH_NUMBER"));
            createBankBranchReqPo.setCountryName(rsBr.getString("attribute3"));
            createBankBranchReqPo.setEftSwiftCode(rsBr.getString("attribute4"));
            HttpEntity<CrCreateBankBranchReqPo> createBankBranchRequest = new HttpEntity<>(createBankBranchReqPo,
                    headers);
            ResponseEntity<?> createBankBranchRes = restTemplate.exchange(
                    customRestApiReqPo.getCloudUrl() + "/fscmRestApi/resources/11.13.18.05/cashBankBranches",
                    HttpMethod.POST, createBankBranchRequest, Object.class);
            log.info(createBankBranchRes.getBody().toString());
        } catch (Exception e) {
            log.error("Branch Creation Error" + e.getMessage());
            CrCreateBankBranchErrors error = new CrCreateBankBranchErrors();
            error.setBankName(rsBr.getString("BANK_NAME"));
            error.setBankNumber(rsBr.getString("BANK_NUMBER"));
            error.setBranchName(rsBr.getString("BRANCH_NAME"));
            error.setBranchNumber(rsBr.getString("BRANCH_NUMBER"));
            error.setEftSwiftCode(rsBr.getString("attribute4"));
            error.setErrorType("BRANCH_CREATION_ERROR");
            error.setErrorMessage(e.getMessage());
            error.setCrBatchName(customRestApiReqPo.getBatchName());
            error.setCreationDate(new java.sql.Date(new java.util.Date().getTime()));
            error.setCreatedBy("ConvertRite");
            createBankBranchErrorsLi.add(error);
        }
    }

    private void createBank(ResultSet rs, HttpHeaders headers,
                            List<CrCreateBankBranchErrors> createBankBranchErrorsLi,
                            CustomRestApiReqPo customRestApiReqPo) throws Exception {
        try {
            CrCreateBankReqPo createBankReqPo = new CrCreateBankReqPo();
            createBankReqPo.setBankName(rs.getString("BANK_NAME"));
            createBankReqPo.setCountryName(rs.getString("attribute3"));
            createBankReqPo.setBankNumber(rs.getString("BANK_NUMBER"));
            HttpEntity<CrCreateBankReqPo> createBankRequestEntity = new HttpEntity<>(createBankReqPo, headers);
            ResponseEntity<?> createBankRes = restTemplate.exchange(
                    customRestApiReqPo.getCloudUrl() + "/fscmRestApi/resources/11.13.18.05/cashBanks", HttpMethod.POST,
                    createBankRequestEntity, Object.class);
            log.info(createBankRes.getBody().toString());
        } catch (Exception e) {
            CrCreateBankBranchErrors error = new CrCreateBankBranchErrors();
            error.setBankName(rs.getString("BANK_NAME"));
            error.setBankNumber(rs.getString("BANK_NUMBER"));
            error.setErrorType("BANK_CREATION_ERROR");
            error.setErrorMessage(e.getMessage());
            error.setCrBatchName(customRestApiReqPo.getBatchName());
            error.setCreationDate(new java.sql.Date(new java.util.Date().getTime()));
            error.setCreatedBy("ConvertRite");
            createBankBranchErrorsLi.add(error);
            log.error("Bank Creation Error" + e.getMessage());
        }
    }

   /* public void createOrUpdateBranch(CustomRestApiReqPo customRestApiReqPo) {
        log.info("Start of createOrUpdateBranch Method in service ###");
        Connection con = null;
        List<CrCreateBankBranchErrors> branchesErrorsLi = new ArrayList<>();
        AsyncProcessStatus asyncStatus = null;
        CrCloudJobStatus crCloudJobStatus=insertIntoCrCloudJobStatus(customRestApiReqPo, Status.IN_PROGRESS.getStatus());
        try {
            asyncStatus = asyncProcessStatusService.startProcess("CreateBranch", customRestApiReqPo.getCldTemplateId(), customRestApiReqPo.getBatchName(), "ConvertRite");

            HttpHeaders headers = new HttpHeaders();
            headers.set("Accept", MediaType.APPLICATION_JSON_VALUE);
            headers.setContentType(MediaType.APPLICATION_JSON);
            headers.setBasicAuth(customRestApiReqPo.getCldUserName(), customRestApiReqPo.getCldPassword());

            con = dynamicDataSourceBasedMultiTenantConnectionProvider.getConnection(String.valueOf(customRestApiReqPo.getPodId()));
            CrCloudTemplateHeadersView crCloudTemplateHeadersView = cloudTemplateHeadersViewRepository.findById(customRestApiReqPo.getCldTemplateId()).get();

            String stagingTableName = crCloudTemplateHeadersView.getStagingTableName();
            PreparedStatement stmtA = con.prepareStatement("SELECT DISTINCT * FROM " + stagingTableName + " WHERE CR_BATCH_NAME = ?");
            stmtA.setString(1, customRestApiReqPo.getBatchName());
            ResultSet resultSetA = stmtA.executeQuery();

            List<CrBranchesResPo> branchesList = getAllCashBranches(headers, customRestApiReqPo);
            log.info("branchesList-->" + branchesList.size());

            Set<Long> branchPartyIds = new HashSet<>();
            for (CrBranchesResPo branch : branchesList) {
                branchPartyIds.add(branch.getBranchPartyId());
            }
            log.info("-branchPartyIds-->" + branchPartyIds.size());

            List<CompletableFuture<CrCreateBankBranchErrors>> futures = new ArrayList<>();

            while (resultSetA.next()) {
                Long branchPartyId = null;
                if (resultSetA.getString("BRANCH_PARTY_ID") != null) {
                    branchPartyId = Long.valueOf(resultSetA.getString("BRANCH_PARTY_ID"));
                }
                log.info(branchPartyIds.contains(branchPartyId) + "--branchPartyId-->" + branchPartyId);

                if (branchPartyIds.contains(branchPartyId) && branchPartyId != null) {
                    futures.add(patchBranchAsync(resultSetA, customRestApiReqPo, headers, branchPartyId));
                } else {
                    futures.add(createBranchAsync(resultSetA, customRestApiReqPo, headers));
                }
            }

            // Wait for all futures to complete and gather results
            CompletableFuture<Void> allOf = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));

            // Block until all futures are done
            allOf.join();
            boolean hasError = false;
            boolean hasSuccess = false;
            // Now process the results
            for (CompletableFuture<CrCreateBankBranchErrors> future : futures) {
                CrCreateBankBranchErrors error = future.join();  // Get the result of each future
                if (error != null) {
                    branchesErrorsLi.add(error);
                    if (error.getStatus().equals(Status.ERROR.getStatus())) {
                        hasError = true;
                    } else if (error.getStatus().equals(Status.SUCCESS.getStatus())) {
                        hasSuccess = true;
                    }
                }
            }
            log.info("banksErrorsLi---------{}" , branchesErrorsLi.size());
            updateAsyncProcessStatusAndCrCloudJobStatus(hasError,hasSuccess,asyncStatus,branchesErrorsLi.isEmpty(),customRestApiReqPo,crCloudJobStatus);

            if (!branchesErrorsLi.isEmpty()) {
                crCreateBankBranchErrorsRepository.saveAll(branchesErrorsLi);
            }

        } catch (Exception e) {
            // e.printStackTrace();
            log.error("Error in createOrUpdateBranch()--->" + e.getMessage());
            if (asyncStatus != null) {
                asyncProcessStatusService.endProcess(asyncStatus.getAsyncProcessId(), customRestApiReqPo.getCldTemplateId(), customRestApiReqPo.getBatchName(), Status.ERROR.getStatus(), e.getMessage(), "ConvertRite", crCloudJobStatus.getJobId());
            }
        }
    }
    */

    public void createOrUpdateBranch(CustomRestApiReqPo customRestApiReqPo) {
        log.info("Start of createOrUpdateBranch Method in service ###");
        String sanitizedBatchName = sanitizeInput(customRestApiReqPo.getBatchName());
        String sanitizedTemplateId = sanitizeInput(String.valueOf(customRestApiReqPo.getCldTemplateId()));
        String sanitizedPodId = sanitizeInput(String.valueOf(customRestApiReqPo.getPodId()));
        Connection con = null;
        List<CrCreateBankBranchErrors> branchesErrorsLi = new ArrayList<>();
        AsyncProcessStatus asyncStatus = null;
        CrCloudJobStatus crCloudJobStatus = insertIntoCrCloudJobStatus(customRestApiReqPo, Status.IN_PROGRESS.getStatus());
        try {
            asyncStatus = asyncProcessStatusService.startProcess("CreateBranch", customRestApiReqPo.getCldTemplateId(), customRestApiReqPo.getBatchName(), "ConvertRite");
            HttpHeaders headers = new HttpHeaders();
            headers.set("Accept", MediaType.APPLICATION_JSON_VALUE);
            headers.setContentType(MediaType.APPLICATION_JSON);
            headers.setBasicAuth(customRestApiReqPo.getCldUserName(), customRestApiReqPo.getCldPassword());
            con = dynamicDataSourceBasedMultiTenantConnectionProvider.getConnection(sanitizedPodId);
            CrCloudTemplateHeadersView crCloudTemplateHeadersView = cloudTemplateHeadersViewRepository.findById(customRestApiReqPo.getCldTemplateId()).get();
            String stagingTableName = crCloudTemplateHeadersView.getStagingTableName();
            if (!isValidTableName(stagingTableName)) {
                throw new IllegalArgumentException("Invalid table name");
            }
            PreparedStatement stmtA = con.prepareStatement("SELECT DISTINCT * FROM " + stagingTableName + " WHERE CR_BATCH_NAME = ?");
            stmtA.setString(1, sanitizedBatchName);
            ResultSet resultSetA = stmtA.executeQuery();
            List<CrBranchesResPo> branchesList = getAllCashBranches(headers, customRestApiReqPo);
            log.info("branchesList-->" + branchesList.size());

            Set<Long> branchPartyIds = new HashSet<>();
            for (CrBranchesResPo branch : branchesList) {
                branchPartyIds.add(branch.getBranchPartyId());
            }
            log.info("-branchPartyIds-->" + branchPartyIds.size());
            List<CompletableFuture<CrCreateBankBranchErrors>> futures = new ArrayList<>();

            while (resultSetA.next()) {
                Long branchPartyId = null;
                if (resultSetA.getString("BRANCH_PARTY_ID") != null) {
                    branchPartyId = Long.valueOf(resultSetA.getString("BRANCH_PARTY_ID"));
                }
                log.info(branchPartyIds.contains(branchPartyId) + "--branchPartyId-->" + branchPartyId);

                if (branchPartyIds.contains(branchPartyId) && branchPartyId != null) {
                    futures.add(patchBranchAsync(resultSetA, customRestApiReqPo, headers, branchPartyId));
                } else {
                    futures.add(createBranchAsync(resultSetA, customRestApiReqPo, headers));
                }
            }
            CompletableFuture<Void> allOf = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
            allOf.join();

            boolean hasError = false;
            boolean hasSuccess = false;

            for (CompletableFuture<CrCreateBankBranchErrors> future : futures) {
                CrCreateBankBranchErrors error = future.join();
                if (error != null) {
                    branchesErrorsLi.add(error);
                    if (error.getStatus().equals(Status.ERROR.getStatus())) {
                        hasError = true;
                    } else if (error.getStatus().equals(Status.SUCCESS.getStatus())) {
                        hasSuccess = true;
                    }
                }
            }
            log.info("banksErrorsLi---------{}", branchesErrorsLi.size());
            updateAsyncProcessStatusAndCrCloudJobStatus(hasError, hasSuccess, asyncStatus, branchesErrorsLi.isEmpty(), customRestApiReqPo, crCloudJobStatus);

            if (!branchesErrorsLi.isEmpty()) {
                crCreateBankBranchErrorsRepository.saveAll(branchesErrorsLi);
            }

        } catch (Exception e) {
            log.error("Error in createOrUpdateBranch()--->" + e.getMessage());
            if (asyncStatus != null) {
                asyncProcessStatusService.endProcess(asyncStatus.getAsyncProcessId(), customRestApiReqPo.getCldTemplateId(), customRestApiReqPo.getBatchName(), Status.ERROR.getStatus(), e.getMessage(), "ConvertRite", crCloudJobStatus.getJobId());
            }
        }
    }
    private String sanitizeInput(String input) {
        if (input == null) return null;
        return input.replaceAll("[^a-zA-Z0-9_-]", "");
    }
    private boolean isValidTableName(String tableName) {
        return tableName.matches("[a-zA-Z0-9_]+");
    }




    private void updateAsyncProcessStatusAndCrCloudJobStatus(boolean hasError, boolean hasSuccess, AsyncProcessStatus asyncStatus, boolean isErrorsListEmpty, CustomRestApiReqPo customRestApiReqPo, CrCloudJobStatus crCloudJobStatus) {
        if (hasError && hasSuccess && asyncStatus != null && !isErrorsListEmpty) {
            log.info("if---------");
            asyncProcessStatusService.endProcess(asyncStatus.getAsyncProcessId(), customRestApiReqPo.getCldTemplateId(), customRestApiReqPo.getBatchName(), Status.WARNING.getStatus(), null, "ConvertRite",crCloudJobStatus.getJobId());
        } else if (hasError && !hasSuccess && asyncStatus != null && !isErrorsListEmpty) {
            log.info("else if 1---------");
            asyncProcessStatusService.endProcess(asyncStatus.getAsyncProcessId(), customRestApiReqPo.getCldTemplateId(), customRestApiReqPo.getBatchName(), Status.ERROR.getStatus(), null, "ConvertRite",crCloudJobStatus.getJobId());
        } else if (!hasError && hasSuccess && asyncStatus != null && !isErrorsListEmpty) {
            log.info("else if 2---------");
            asyncProcessStatusService.endProcess(asyncStatus.getAsyncProcessId(), customRestApiReqPo.getCldTemplateId(), customRestApiReqPo.getBatchName(), Status.SUCCESS.getStatus(), null, "ConvertRite",crCloudJobStatus.getJobId());
        } else {
            log.info("else---------");
        }
    }

    private CompletableFuture<CrCreateBankBranchErrors> createBranchAsync(ResultSet resultSet, CustomRestApiReqPo customRestApiReqPo, HttpHeaders headers) throws Exception {
        return CompletableFuture.completedFuture(createBankBranch(resultSet, headers, customRestApiReqPo));
    }

    private CompletableFuture<CrCreateBankBranchErrors> patchBranchAsync(ResultSet resultSet, CustomRestApiReqPo customRestApiReqPo, HttpHeaders headers, Long branchPartyId) throws SQLException {
        return CompletableFuture.completedFuture(patchBankBranch(resultSet, headers, customRestApiReqPo, branchPartyId));
    }

    private CrCreateBankBranchErrors patchBankBranch(ResultSet rs, HttpHeaders headers, CustomRestApiReqPo customRestApiReqPo, Long branchPartyId) throws SQLException {
        log.info("=====patchBankBranch======");
        CrCreateBankBranchErrors error = null;
        try {
            CrUpdateBankBranchReqPo createBankBranchReqPo = new CrUpdateBankBranchReqPo();
            createBankBranchReqPo.setBankName(rs.getString("BANK_NAME"));
            createBankBranchReqPo.setBankBranchName(rs.getString("BRANCH_NAME"));
            createBankBranchReqPo.setBranchNumber(rs.getString("BRANCH_NUMBER"));
            createBankBranchReqPo.setEftSwiftCode(rs.getString("EFT_SWIFT_CODE"));
            HttpEntity<CrUpdateBankBranchReqPo> createBankBranchRequest = new HttpEntity<>(createBankBranchReqPo,
                    headers);
            ResponseEntity<?> createBankBranchRes = restTemplate.exchange(
                    customRestApiReqPo.getCloudUrl() + branchCloudUrl + "/" + branchPartyId,
                    HttpMethod.PATCH, createBankBranchRequest, Object.class);
            log.info("Response Body ---{}", createBankBranchRes.getBody().toString());

            HttpStatus statusCode = createBankBranchRes.getStatusCode();
            log.info("Status Code: " + statusCode);
            if (statusCode.value() == 200 || statusCode.value() == 201) {
                error = insertErrorOrSuccessRecordsOfBranches(rs, customRestApiReqPo, null, Status.SUCCESS.getStatus(), "PATCH");
            }
        } catch (Exception e) {
            log.error("Branch Updation Error -- ----> " + e.getMessage());
            error = insertErrorOrSuccessRecordsOfBranches(rs, customRestApiReqPo, e.getMessage(), Status.ERROR.getStatus(), "PATCH");
        }
        return error;
    }

    public void createOrUpdateBank(CustomRestApiReqPo customRestApiReqPo) throws Exception {
        log.info("Start of createOrUpdateBank Method in service ###");
        Connection con = null;
        List<CrCreateBankBranchErrors> banksErrorsLi = new ArrayList<>();
        AsyncProcessStatus asyncStatus = null;
        CrCloudJobStatus crCloudJobStatus=insertIntoCrCloudJobStatus(customRestApiReqPo, Status.IN_PROGRESS.getStatus());
        try {
            asyncStatus = asyncProcessStatusService.startProcess("CreateBank", customRestApiReqPo.getCldTemplateId(), customRestApiReqPo.getBatchName(), "ConvertRite");

            con = dynamicDataSourceBasedMultiTenantConnectionProvider.getConnection(String.valueOf(customRestApiReqPo.getPodId()));
            CrCloudTemplateHeadersView crCloudTemplateHeadersView = cloudTemplateHeadersViewRepository.findById(customRestApiReqPo.getCldTemplateId()).get();
            HttpHeaders headers = new HttpHeaders();
            headers.set("Accept", MediaType.APPLICATION_JSON_VALUE);
            headers.setContentType(MediaType.APPLICATION_JSON);
            headers.setBasicAuth(customRestApiReqPo.getCldUserName(), customRestApiReqPo.getCldPassword());

            String stagingTableName = crCloudTemplateHeadersView.getStagingTableName();
            PreparedStatement stmtA = con.prepareStatement("SELECT DISTINCT * FROM " + stagingTableName + " WHERE CR_BATCH_NAME = ?");
            stmtA.setString(1, customRestApiReqPo.getBatchName());
            ResultSet resultSetA = stmtA.executeQuery();

            List<CrBanksResPo> banksList = getAllCashBanks(headers, customRestApiReqPo);
            log.info("banksList-->" + banksList.size());

            Set<Long> bankPartyIds = new HashSet<>();
            for (CrBanksResPo account : banksList) {
                // log.info("-account.getBankPartyId()-->" +account.getBankPartyId());
                bankPartyIds.add(account.getBankPartyId());
            }
            log.info("-bankPartyIds-->" + bankPartyIds.size());

            List<CompletableFuture<CrCreateBankBranchErrors>> futures = new ArrayList<>();

            while (resultSetA.next()) {
                Long bankPartyId = null;
                if (resultSetA.getString("BANK_PARTY_ID") != null) {
                    bankPartyId = Long.valueOf(resultSetA.getString("BANK_PARTY_ID"));
                }
                log.info(bankPartyIds.contains(bankPartyId) + "--bankPartyId-->" + bankPartyId);

                if (bankPartyIds.contains(bankPartyId) && bankPartyId != null) {
                    futures.add(patchBankAsync(resultSetA, customRestApiReqPo, headers, bankPartyId));
                } else {
                    futures.add(createBankAsync(resultSetA, customRestApiReqPo, headers));
                }
            }

            // Wait for all futures to complete and gather results
            CompletableFuture<Void> allOf = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));

            // Block until all futures are done
            allOf.join();

            boolean hasError = false;
            boolean hasSuccess = false;
            // Now process the results
            for (CompletableFuture<CrCreateBankBranchErrors> future : futures) {
                CrCreateBankBranchErrors error = future.join();  // Get the result of each future
                if (error != null) {
                    banksErrorsLi.add(error);
                    if (error.getStatus().equals(Status.ERROR.getStatus())) {
                        hasError = true;
                    } else if (error.getStatus().equals(Status.SUCCESS.getStatus())) {
                        hasSuccess = true;
                    }
                }
            }
            log.info("banksErrorsLi---------{}" , banksErrorsLi.size());
            updateAsyncProcessStatusAndCrCloudJobStatus(hasError,hasSuccess,asyncStatus,banksErrorsLi.isEmpty(),customRestApiReqPo,crCloudJobStatus);

            if (!banksErrorsLi.isEmpty()) {
                crCreateBankBranchErrorsRepository.saveAll(banksErrorsLi);
            }

        } catch (Exception e) {
            e.printStackTrace();
            log.error("Error in createBank()--->" + e.getMessage());
            if (asyncStatus != null) {
                asyncProcessStatusService.endProcess(asyncStatus.getAsyncProcessId(), customRestApiReqPo.getCldTemplateId(), customRestApiReqPo.getBatchName(), Status.ERROR.getStatus(), e.getMessage(), "ConvertRite", crCloudJobStatus.getJobId());
            }
        }
    }

    private CrCloudJobStatus insertIntoCrCloudJobStatus(CustomRestApiReqPo customRestApiReqPo, String status) {
        CrCloudJobStatus crCloudJobStatus = new CrCloudJobStatus();
        crCloudJobStatus.setJobStatus(status);
        crCloudJobStatus.setCldTemplateId(customRestApiReqPo.getCldTemplateId());
        crCloudJobStatus.setBatchName(customRestApiReqPo.getBatchName());
        crCloudJobStatus.setObjectCode(customRestApiReqPo.getObjectName());
        crCloudJobStatus.setObjectId(customRestApiReqPo.getObjectId());
        crCloudJobStatus.setImportType("RESTAPI");

        crCloudJobStatus= crCloudJobStatusRepo.save(crCloudJobStatus);
        return crCloudJobStatus;
    }

    private CompletableFuture<CrCreateBankBranchErrors> createBankAsync(ResultSet resultSet, CustomRestApiReqPo customRestApiReqPo, HttpHeaders headers) throws Exception {
        return CompletableFuture.completedFuture(createBank(resultSet, headers, customRestApiReqPo));
    }

    private CompletableFuture<CrCreateBankBranchErrors> patchBankAsync(ResultSet resultSet, CustomRestApiReqPo customRestApiReqPo, HttpHeaders headers, Long bankPartyId) throws SQLException {
        return CompletableFuture.completedFuture(patchBank(resultSet, headers, customRestApiReqPo, bankPartyId));
    }

    private CrCreateBankBranchErrors patchBank(ResultSet rs, HttpHeaders headers, CustomRestApiReqPo customRestApiReqPo, Long bankPartyId) throws SQLException {
        log.info("=====patchBank======");
        CrUpdateBankReqPo bankAccountReqPo = null;
        CrCreateBankBranchErrors error = new CrCreateBankBranchErrors();
        try {
            bankAccountReqPo = new CrUpdateBankReqPo();
            bankAccountReqPo = constructInputJsonforBankPatch(rs);
            // Convert object to JSON string
            String jsonPayload = objectMapper.writeValueAsString(bankAccountReqPo);
            // Remove any unwanted CRLF characters and trim whitespace
            String cleanedJsonPayload = jsonPayload.replaceAll("\\r\\n|\\r|\\n", "").trim();

            log.info("Cleaned JSON Payload: " + cleanedJsonPayload);
            HttpEntity<String> entity = new HttpEntity<>(cleanedJsonPayload, headers);
            log.info("bankReqPo------>" + bankAccountReqPo);
            String url = customRestApiReqPo.getCloudUrl() + bankCloudUrl + "/" + bankPartyId.toString().trim();
            log.info("url------>" + url);
            ResponseEntity<?> patchBankAccountRes = restTemplate.exchange(url, HttpMethod.PATCH, entity, Object.class);
            log.info("patchBankRes--->" + patchBankAccountRes.getBody().toString());

            HttpStatus statusCode = patchBankAccountRes.getStatusCode();
            log.info("Status Code: " + statusCode);
            if (statusCode.value() == 200 || statusCode.value() == 201) {
                error = insertErrorOrSuccessRecordsOfBanks(customRestApiReqPo, rs, null, Status.SUCCESS.getStatus(), "PATCH");
            }
        } catch (Exception e) {
            log.error("Error while calling Create Bank PATCH API ----> " + e.getMessage());
            error = insertErrorOrSuccessRecordsOfBanks(customRestApiReqPo, rs, e.getMessage(), Status.ERROR.getStatus(), "PATCH");
        }
        return error;
    }

    private CrUpdateBankReqPo constructInputJsonforBankPatch(ResultSet rs) throws SQLException {
        CrUpdateBankReqPo req = new CrUpdateBankReqPo();
        log.info("BANK_PARTY_ID--->" + Long.valueOf(rs.getString("BANK_PARTY_ID")));
        // We are restricting toupdate BankName and BankNumber fields even oracle accepting bczwe treating those two fileds as uniq combination to update bank party idin statging table
//But uncommenting bank name bcz api is giving error if not passings
        req.setBankName(rs.getString("BANK_NAME"));
//        req.setBankNumber(rs.getString("BANK_NUMBER"));
        req.setBankNameAlt(rs.getString("BANK_NAME_ALT"));
        req.setDescription(rs.getString("DESCRIPTION"));
        req.setBankPartyId(Long.valueOf(rs.getString("BANK_PARTY_ID")));
        req.setTaxpayerIdNumber(rs.getString("TAXPAYER_ID_NUMBER"));
        req.setTaxRegistrationNumber(rs.getString("TAXPAYER_REGISTRATION_NUMBER"));
        return req;
    }


    private List<CrBankAccountResPo> getAllCashBankAccounts(HttpHeaders headers, CustomRestApiReqPo customRestApiReqPo) {
        RestTemplate restTemplate = new RestTemplate();
//        try {
        HttpEntity<String> entity = new HttpEntity<>(headers);
        ResponseEntity<Map<String, Object>> response = restTemplate.exchange(
                customRestApiReqPo.getCloudUrl() + bankAccountCloudUrl,
                HttpMethod.GET,
                entity,
                new ParameterizedTypeReference<Map<String, Object>>() {
                }
        );

        Map<String, Object> responseBody = response.getBody();
        // log.info("responseBody------>" + responseBody);

        if (responseBody.containsKey("items") && responseBody.get("items") instanceof List<?>) {
            List<?> items = (List<?>) responseBody.get("items");
            return items.stream()
                    .map(item -> objectMapper.convertValue(item, CrBankAccountResPo.class))
                    .collect(Collectors.toList());
        } else {
            log.warn("Unexpected response structure or empty list");
            return Collections.emptyList();
        }
//        } catch (Exception e) {
//            log.error("An error occurred while fetching cash bank accounts", e);
//            return Collections.emptyList();
//        }
    }

    private List<CrBanksResPo> getAllCashBanks(HttpHeaders headers, CustomRestApiReqPo customRestApiReqPo) {
        RestTemplate restTemplate = new RestTemplate();
        try {
            HttpEntity<String> entity = new HttpEntity<>(headers);
            ResponseEntity<Map<String, Object>> response = restTemplate.exchange(
                    customRestApiReqPo.getCloudUrl() + bankCloudUrl,
                    HttpMethod.GET,
                    entity,
                    new ParameterizedTypeReference<Map<String, Object>>() {
                    }
            );

            Map<String, Object> responseBody = response.getBody();
            //    log.info("responseBody------>" + responseBody);

            if (responseBody.containsKey("items") && responseBody.get("items") instanceof List<?>) {
                List<?> items = (List<?>) responseBody.get("items");
                return items.stream()
                        .map(item -> objectMapper.convertValue(item, CrBanksResPo.class))
                        .collect(Collectors.toList());
            } else {
                log.warn("Unexpected response structure or empty list");
                return Collections.emptyList();
            }
        } catch (Exception e) {
            log.error("An error occurred while fetching cash bank accounts", e);
            return Collections.emptyList();
        }
    }

    private List<CrBranchesResPo> getAllCashBranches(HttpHeaders headers, CustomRestApiReqPo customRestApiReqPo) {
        RestTemplate restTemplate = new RestTemplate();
        try {
            HttpEntity<String> entity = new HttpEntity<>(headers);
            ResponseEntity<Map<String, Object>> response = restTemplate.exchange(
                    customRestApiReqPo.getCloudUrl() + branchCloudUrl,
                    HttpMethod.GET,
                    entity,
                    new ParameterizedTypeReference<Map<String, Object>>() {
                    }
            );

            Map<String, Object> responseBody = response.getBody();
            //log.info("responseBody------>" + responseBody);

            if (responseBody.containsKey("items") && responseBody.get("items") instanceof List<?>) {
                List<?> items = (List<?>) responseBody.get("items");
                return items.stream()
                        .map(item -> objectMapper.convertValue(item, CrBranchesResPo.class))
                        .collect(Collectors.toList());
            } else {
                log.warn("Unexpected response structure or empty list");
                return Collections.emptyList();
            }
        } catch (Exception e) {
            log.error("An error occurred while fetching cash bank accounts", e);
            return Collections.emptyList();
        }
    }

    private CrCreateBankBranchErrors createBankBranch(ResultSet rsBr, HttpHeaders headers,
                                                      CustomRestApiReqPo customRestApiReqPo) throws Exception {
        log.info("=====createBankBranch======");
        CrCreateBankBranchErrors error = null;
        CrCreateBankBranchReqPo createBankBranchReqPo = null;
        try {
            createBankBranchReqPo = new CrCreateBankBranchReqPo();
            createBankBranchReqPo.setBankName(rsBr.getString("BANK_NAME"));
            createBankBranchReqPo.setBankBranchName(rsBr.getString("BRANCH_NAME"));
            createBankBranchReqPo.setBranchNumber(rsBr.getString("BRANCH_NUMBER"));
            createBankBranchReqPo.setCountryName(rsBr.getString("COUNTRY_NAME"));
            createBankBranchReqPo.setEftSwiftCode(rsBr.getString("EFT_SWIFT_CODE"));
            log.info("createBankBranchReqPo--->" + createBankBranchReqPo);
            HttpEntity<CrCreateBankBranchReqPo> createBankBranchRequest = new HttpEntity<>(createBankBranchReqPo,
                    headers);
            ResponseEntity<?> createBankBranchRes = restTemplate.exchange(
                    customRestApiReqPo.getCloudUrl() + branchCloudUrl,
                    HttpMethod.POST, createBankBranchRequest, Object.class);
            log.info("Response Body---{}", createBankBranchRes.getBody().toString());
            HttpStatus statusCode = createBankBranchRes.getStatusCode();
            log.info("Status Code: " + statusCode);
            if (statusCode.value() == 200 || statusCode.value() == 201) {
                error = insertErrorOrSuccessRecordsOfBranches(rsBr, customRestApiReqPo, null, Status.SUCCESS.getStatus(), "POST");
            }
        } catch (Exception e) {
            log.error("Branch Creation Error -- ----> " + e.getMessage());
            error = insertErrorOrSuccessRecordsOfBranches(rsBr, customRestApiReqPo, e.getMessage(), Status.ERROR.getStatus(), "POST");
        }
        return error;
    }

    private CrCreateBankBranchErrors insertErrorOrSuccessRecordsOfBranches(ResultSet rsBr, CustomRestApiReqPo customRestApiReqPo, String errorMessage, String status, String apiType) throws SQLException {
        CrCreateBankBranchErrors error = new CrCreateBankBranchErrors();
        error.setBankName(rsBr.getString("BANK_NAME"));
        error.setBranchName(rsBr.getString("BRANCH_NAME"));
        error.setBranchNumber(rsBr.getString("BRANCH_NUMBER"));
        error.setEftSwiftCode(rsBr.getString("EFT_SWIFT_CODE"));
        error.setErrorType(apiType);
        error.setCldTempId(customRestApiReqPo.getCldTemplateId());
        error.setErrorMessage(errorMessage);
        error.setStatus(status);
        error.setCrBatchName(customRestApiReqPo.getBatchName());
        error.setCreationDate(new java.sql.Date(new java.util.Date().getTime()));
        error.setCreatedBy("ConvertRite");
        return error;
    }

    private CrCreateBankBranchErrors createBank(ResultSet rs, HttpHeaders headers,
                                                CustomRestApiReqPo customRestApiReqPo) throws Exception {
        log.info("=====createBank======");
        CrCreateBankBranchErrors error = null;
        try {
            CrCreateBankReqPo createBankReqPo = new CrCreateBankReqPo();
            createBankReqPo.setBankName(rs.getString("BANK_NAME"));
            createBankReqPo.setCountryName(rs.getString("COUNTRY_NAME"));
            createBankReqPo.setCountryName("Italy");
            createBankReqPo.setBankNumber(rs.getString("BANK_NUMBER"));
            HttpEntity<CrCreateBankReqPo> createBankRequestEntity = new HttpEntity<>(createBankReqPo, headers);
            log.info("createBankReqPo----->" + createBankReqPo);
            ResponseEntity<?> createBankRes = restTemplate.exchange(
                    customRestApiReqPo.getCloudUrl() + "/fscmRestApi/resources/11.13.18.05/cashBanks", HttpMethod.POST,
                    createBankRequestEntity, Object.class);
            log.info("Response Body: " + createBankRes.getBody().toString());

            HttpStatus statusCode = createBankRes.getStatusCode();
            log.info("Status Code: " + statusCode);
            if (statusCode.value() == 200 || statusCode.value() == 201) {
                error = insertErrorOrSuccessRecordsOfBanks(customRestApiReqPo, rs, null, Status.SUCCESS.getStatus(), "POST");
            }
        } catch (Exception e) {
            log.error("Error while calling Create Bank Post API ----> " + e.getMessage());
            error = insertErrorOrSuccessRecordsOfBanks(customRestApiReqPo, rs, e.getMessage(), Status.ERROR.getStatus(), "POST");
        }
        return error;
    }

    private CrCreateBankBranchErrors insertErrorOrSuccessRecordsOfBanks(CustomRestApiReqPo customRestApiReqPo, ResultSet rs, String errorMessage, String status, String apiType) throws SQLException {
        CrCreateBankBranchErrors error = new CrCreateBankBranchErrors();
        error.setStatus(status);
        error.setBankName(rs.getString("BANK_NAME"));
        error.setCountryName(rs.getString("COUNTRY_NAME"));
        error.setErrorType(apiType);
        error.setCldTempId(customRestApiReqPo.getCldTemplateId());
        error.setErrorMessage(errorMessage);
        error.setCrBatchName(customRestApiReqPo.getBatchName());
        error.setCreationDate(new java.sql.Date(new java.util.Date().getTime()));
        error.setCreatedBy("ConvertRite");
        return error;
    }

    public void updateProjectDff(CustomRestApiReqPo customRestApiReqPo)
            throws Exception {
        log.info("Start of updateProjectDff method in service ##");
        Connection con = null;
        CrCloudTemplateHeadersView crCloudTemplateHeadersView = cloudTemplateHeadersViewRepository
                .findById(customRestApiReqPo.getCldTemplateId()).get();
        String stagingTableName = crCloudTemplateHeadersView.getStagingTableName();
        try {
            CrCloudJobStatus crCloudJobStatus=insertIntoCrCloudJobStatus(customRestApiReqPo, Status.COMPLETED.getStatus());
            log.info("TENANT-->" + customRestApiReqPo.getPodId());
            con = dynamicDataSourceBasedMultiTenantConnectionProvider.getConnection(String.valueOf(customRestApiReqPo.getPodId()));
            PreparedStatement stmnt = con.prepareStatement("select * from " + stagingTableName
                    + " where CR_BATCH_NAME='" + customRestApiReqPo.getBatchName() + "'");
            ResultSet rs = stmnt.executeQuery();
            List<CrProjDffError> projectErrorLi = new ArrayList<>();
            // Update ProjectDFF Rest Api call
            while (rs.next())
                projectErrorLi = updateProjectDffRestApi(customRestApiReqPo, rs, projectErrorLi);
            if (!projectErrorLi.isEmpty())
                crProjDffErrorRepository.saveAll(projectErrorLi);
        } finally {
            if (con != null)
                con.close();
        }
    }


    /*public void updateProjectDff(CustomRestApiReqPo customRestApiReqPo) throws Exception {
        log.info("Start of updateProjectDff method in service ##");
        Connection con = null;
        try {
            CrCloudTemplateHeadersView crCloudTemplateHeadersView = cloudTemplateHeadersViewRepository
                    .findById(customRestApiReqPo.getCldTemplateId()).get();
            String stagingTableName = crCloudTemplateHeadersView.getStagingTableName();
            CrCloudJobStatus crCloudJobStatus = insertIntoCrCloudJobStatus(customRestApiReqPo, Status.COMPLETED.getStatus());
            log.info("TENANT-->" + customRestApiReqPo.getPodId());
            con = dynamicDataSourceBasedMultiTenantConnectionProvider.getConnection(String.valueOf(customRestApiReqPo.getPodId()));
            String query = "SELECT * FROM " + stagingTableName + " WHERE CR_BATCH_NAME = ?";
            PreparedStatement stmnt = con.prepareStatement(query);
            stmnt.setString(1, customRestApiReqPo.getBatchName());
            ResultSet rs = stmnt.executeQuery();
            List<CrProjDffError> projectErrorLi = new ArrayList<>();
            while (rs.next()) {
                projectErrorLi = updateProjectDffRestApi(customRestApiReqPo, rs, projectErrorLi);
            }
            if (!projectErrorLi.isEmpty()) {
                crProjDffErrorRepository.saveAll(projectErrorLi);
            }
        } catch (SQLException e) {
            log.error("Error while updating project DFF: ", e);
            throw new Exception("Error while processing DFF update", e);
        } finally {
            if (con != null) {
                con.close();
            }
        }
    }
     */
    private List<CrProjDffError> updateProjectDffRestApi(CustomRestApiReqPo customRestApiReqPo, ResultSet rs, List<CrProjDffError> projectErrorLi) throws Exception {
        try {
            HttpHeaders headers = new HttpHeaders();
            headers.set("Accept", MediaType.APPLICATION_JSON_VALUE);
            headers.setContentType(MediaType.APPLICATION_JSON);
            headers.setBasicAuth(customRestApiReqPo.getCldUserName(), customRestApiReqPo.getCldPassword());

            List<CrUpdateProjectDffReqPo> li = new ArrayList<>();
            CrUpdateProjectDffReqPo updateProjectDffReqPo = new CrUpdateProjectDffReqPo();
            updateProjectDffReqPo.setParentProjectId(rs.getString("PARENT_PROJECT_ID"));
            updateProjectDffReqPo.setParentProjectNumber(rs.getString("PARENT_PROJECT_NUMBER"));
            li.add(updateProjectDffReqPo);
            CrUpdateProjectDffReqLiPo updateProjectDffReqLiPo = new CrUpdateProjectDffReqLiPo();
            updateProjectDffReqLiPo.setProjectDff(li);

            HttpEntity<CrUpdateProjectDffReqLiPo> updatePrjDffRequestEntity = new HttpEntity<>(
                    updateProjectDffReqLiPo, headers);
            ResponseEntity<?> createBankRes = restTemplate.exchange(
                    customRestApiReqPo.getCloudUrl() + "/fscmRestApi/resources/11.13.18.05/projects/"
                            + rs.getString("PROJECT_ID"),
                    HttpMethod.PATCH, updatePrjDffRequestEntity, Object.class);
            log.info(createBankRes.getBody().toString());
        } catch (Exception e) {
            log.error("Update Project DFF Error" + e.getMessage());
            CrProjDffError crProjDffError = new CrProjDffError();
            crProjDffError.setParentProjectId(rs.getString("PARENT_PROJECT_ID"));
            crProjDffError.setParentProjectNumber(rs.getString("PARENT_PROJECT_NUMBER"));
            crProjDffError.setProjectId(rs.getString("PROJECT_ID"));
            crProjDffError.setCrBatchName(customRestApiReqPo.getBatchName());
            crProjDffError.setErrorMessage(e.getMessage());
            crProjDffError.setCreationDate(new java.sql.Date(new java.util.Date().getTime()));
            crProjDffError.setCreatedBy("ConvertRite");
            projectErrorLi.add(crProjDffError);
        }
        return projectErrorLi;
    }


    public void supplierTaxProfileUpdate(CustomRestApiReqPo customRestApiReqPo) throws Exception {
        log.info("Start of supplierTaxProfileUpdate in service ###");
        Connection con = null;
        List<CrSupplierTaxProfileErrors> errorsList = new ArrayList<>();
        CrCloudTemplateHeadersView crCloudTemplateHeadersView = cloudTemplateHeadersViewRepository.findById(customRestApiReqPo.getCldTemplateId()).get();
        HttpHeaders headers = new HttpHeaders();
        headers.set("Accept", MediaType.APPLICATION_JSON_VALUE);
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setBasicAuth(customRestApiReqPo.getCldUserName(), customRestApiReqPo.getCldPassword());
        String stagingTableName = crCloudTemplateHeadersView.getStagingTableName();
        try {
            CrCloudJobStatus crCloudJobStatus=insertIntoCrCloudJobStatus(customRestApiReqPo, Status.COMPLETED.getStatus());
            log.info("TENANT-->" + customRestApiReqPo.getPodId());
            con = dynamicDataSourceBasedMultiTenantConnectionProvider.getConnection(String.valueOf(customRestApiReqPo.getPodId()));
            PreparedStatement stmnt = con.prepareStatement("select distinct party_tax_profile_id,allow_offset_tax_flag,tax_registartion_type from "
                    + stagingTableName + " where CR_BATCH_NAME='" + customRestApiReqPo.getBatchName() + "'");
            ResultSet rs = stmnt.executeQuery();
            while (rs.next()) {
                // TaxRegistrations RestApi call
                CrSupplierTaxProfileErrors stpError = supplierTaxProfileUpdateApi(rs, headers, customRestApiReqPo.getBatchName(), customRestApiReqPo);
                if (stpError != null) {
                    errorsList.add(stpError);
                }
            }
            if (!errorsList.isEmpty()) {
                crSupplierTaxProfileErrorsRepository.saveAll(errorsList);
            }
        } finally {
            if (con != null)
                con.close();
        }
    }




   /* public void supplierTaxProfileUpdate(CustomRestApiReqPo customRestApiReqPo) throws Exception {
        log.info("start of supplierTaxProfileUpdate in service ###");
        Connection con = null;
        List<CrSupplierTaxProfileErrors> errorsList = new ArrayList<>();
        CrCloudTemplateHeadersView crCloudTemplateHeadersView = cloudTemplateHeadersViewRepository.findById(customRestApiReqPo.getCldTemplateId()).get();
        HttpHeaders headers = new HttpHeaders();
        headers.set("Accept", MediaType.APPLICATION_JSON_VALUE);
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setBasicAuth(customRestApiReqPo.getCldUserName(), customRestApiReqPo.getCldPassword());
        String stagingTableName = crCloudTemplateHeadersView.getStagingTableName();

        try {
            CrCloudJobStatus crCloudJobStatus = insertIntoCrCloudJobStatus(customRestApiReqPo, Status.COMPLETED.getStatus());
            log.info("TENANT-->" + customRestApiReqPo.getPodId());
            con = dynamicDataSourceBasedMultiTenantConnectionProvider.getConnection(String.valueOf(customRestApiReqPo.getPodId()));
            String query = "SELECT DISTINCT party_tax_profile_id, allow_offset_tax_flag, tax_registration_type " +
                    "FROM " + stagingTableName + " WHERE CR_BATCH_NAME = ?";
            PreparedStatement stmnt = con.prepareStatement(query);
            stmnt.setString(1, customRestApiReqPo.getBatchName());
            ResultSet rs = stmnt.executeQuery();
            while (rs.next()) {
                CrSupplierTaxProfileErrors stpError = supplierTaxProfileUpdateApi(rs, headers, customRestApiReqPo.getBatchName(), customRestApiReqPo);
                if (stpError != null) {
                    errorsList.add(stpError);
                }
            }
            if (!errorsList.isEmpty()) {
                crSupplierTaxProfileErrorsRepository.saveAll(errorsList);
            }
        } catch (SQLException e) {
            log.error("Error while processing supplier tax profile update: ", e);
            throw new Exception("Error during supplier tax profile update", e);
        } finally {
            if (con != null) {
                con.close();
            }
        }
    }


    */
    private CrSupplierTaxProfileErrors supplierTaxProfileUpdateApi(ResultSet rs, HttpHeaders headers
            , String batchName, CustomRestApiReqPo customRestApiReqPo) {
        CrSupplierTaxProfileReqPo supTaxRegReqPo = new CrSupplierTaxProfileReqPo();
        String taxProfileId = null;
        ResponseEntity<Object> supTaxRegRes = null;
        CrSupplierTaxProfileErrors stpError = new CrSupplierTaxProfileErrors();
        try {
            String flag = rs.getString("allow_offset_tax_flag");
            taxProfileId = rs.getString("party_tax_profile_id");
            String regType = rs.getString("tax_registartion_type");
            if (flag != null && taxProfileId != null) {
                supTaxRegReqPo.setAllowOffsetTaxFlag(Boolean.valueOf(flag));
                supTaxRegReqPo.setRegistrationTypeCode(regType);
                HttpEntity<CrSupplierTaxProfileReqPo> reqData = new HttpEntity<>(supTaxRegReqPo, headers);
                supTaxRegRes = restTemplate.exchange(
                        customRestApiReqPo.getCloudUrl() + "/fscmRestApi/resources/11.13.18.05/partyTaxProfiles/" + taxProfileId,
                        HttpMethod.PATCH, reqData, Object.class);
                log.info("supTaxRegRes.getStatusCode().toString()->" + supTaxRegRes.getStatusCode().toString());

                stpError.setPartyTaxProfileId(taxProfileId);
                stpError.setStatusCode(supTaxRegRes.getStatusCode().toString());
                stpError.setCreatedBy("ConvertRite");
                stpError.setCreatedDate(new java.sql.Date(new java.util.Date().getTime()));
                stpError.setCrBatchName(batchName);
                log.info("getBatchName->" + stpError.getCrBatchName());
            }
        } catch (Exception e) {
            log.error("Error in Supplier Tax Profile  Service " + e.getMessage());
            stpError.setPartyTaxProfileId(taxProfileId);
            stpError.setStatusCode(supTaxRegRes.getStatusCode().toString());
            stpError.setErrorMessage(e.getMessage());
            stpError.setCreatedDate(new java.sql.Date(new java.util.Date().getTime()));
            stpError.setCrBatchName(batchName);
        }
        return stpError;
    }

   public void validateCcid(CustomRestApiReqPo customRestApiReqPo) throws Exception {
        log.info("Start of validateCcid method in service ##");
        AsyncProcessStatus processStatus = asyncProcessStatusRepository.save(initializeAsyncProcessStatus());
        log.info("Saved async process status to CR_ASYNC_PROCESS_STATUS table");

        Connection con = null;
        long totalCount = 0;
        CrCloudTemplateHeadersView crCloudTemplateHeadersView;

        ExecutorService executor = Executors.newFixedThreadPool(noOfThreads);
        List<Future<?>> futures = new ArrayList<>();
        AtomicBoolean errorOccurred = new AtomicBoolean(false);

        try {
            CrCloudJobStatus crCloudJobStatus=insertIntoCrCloudJobStatus(customRestApiReqPo, Status.COMPLETED.getStatus());
            // Retrieve the template header view
            crCloudTemplateHeadersView = cloudTemplateHeadersViewRepository
                    .findById(customRestApiReqPo.getCldTemplateId())
                    .orElseThrow(() -> new Exception("Template not found with ID: " + customRestApiReqPo.getCldTemplateId()));

            log.info("TENANT --> " + customRestApiReqPo.getPodId());

            // Establish a connection
            con = dynamicDataSourceBasedMultiTenantConnectionProvider.getConnection(String.valueOf(customRestApiReqPo.getPodId()));

            // Prepare and execute the query to get the total count
            String countQuery = "SELECT COUNT(DISTINCT " + customRestApiReqPo.getCcidColumnName() + ") FROM " + crCloudTemplateHeadersView.getStagingTableName()
                    + " WHERE CR_BATCH_NAME=?";
            try (PreparedStatement stmnt = con.prepareStatement(countQuery)) {
                stmnt.setString(1, customRestApiReqPo.getBatchName());
                try (ResultSet rs = stmnt.executeQuery()) {
                    if (rs.next()) {
                        totalCount = rs.getLong(1);
                    } else {
                        throw new Exception("Failed to retrieve the total count");
                    }
                }
            }

            long loopCount = (long) Math.ceil((double) totalCount / batchSize);
            log.info("loopCount >>>>>> " + loopCount);
            log.info("totalCount >>>>> " + totalCount);

            // Submit tasks for execution
            for (int i = 0; i < loopCount; i++) {
                if (errorOccurred.get()) {
                    log.error("Stopping further processing due to an error in one of the threads");
                    break;
                }

                long offset = (i * batchSize) + 1;
                long limit = offset + batchSize - 1;

                log.info("offset >>>> " + offset);
                log.info("limit >>>>>> " + limit);

                Future<?> future = executor.submit(() -> {
                    try {
                        validateAndCreateClass.validateAndCreateAccounts(
                                crCloudTemplateHeadersView.getStagingTableName(),
                                offset,
                                limit,
                                customRestApiReqPo,
                                crCloudTemplateHeadersView,
                                processStatus
                        );
                    } catch (Exception e) {
                        log.error("Error in batch processing", e);
                        errorOccurred.set(true);
                        throw new RuntimeException(e);
                    }
                });
                futures.add(future);
            }

            // Wait for all tasks to complete or handle errors
            for (Future<?> future : futures) {
                try {
                    future.get();
                } catch (Exception e) {
                    executor.shutdownNow();
                    throw new Exception(e.getMessage());
                }
            }


            // Update the CVR and async process status only if no error occurred
            if (!errorOccurred.get()) {
                processJobDaoImpl.cvrUpdate(
                        crCloudTemplateHeadersView.getStagingTableName(),
                        crCloudTemplateHeadersView.getSourceTemplateId(),
                        customRestApiReqPo.getBatchName(),
                        customRestApiReqPo.getLedgerColumnName(),
                        customRestApiReqPo.getCcidColumnName()
                );
                updateAsyncProcessStatus(processStatus.getAsyncProcessId(), "Completed");
            }

        } catch (Exception ex) {
            // Ensure the async process status is updated even if an exception occurs
            updateAsyncProcessStatus(processStatus.getAsyncProcessId(), "Error", ex);
            log.error("Error in validateCcid", ex);
            throw new Exception(ex);

        } finally {
            // Ensure the connection is closed and the executor is shutdown
            if (con != null) {
                try {
                    con.close();
                } catch (SQLException e) {
                    log.error("Error closing connection", e);
                }
            }
            executor.shutdown();
        }
    }


   /* public void validateCcid(CustomRestApiReqPo customRestApiReqPo) throws Exception {
        log.info("Start of validateCcid method in service ##");
        if (customRestApiReqPo.getCcidColumnName() == null || customRestApiReqPo.getCcidColumnName().isEmpty()) {
            throw new IllegalArgumentException("CcidColumnName is required and cannot be empty.");
        }
        if (customRestApiReqPo.getBatchName() == null || customRestApiReqPo.getBatchName().isEmpty()) {
            throw new IllegalArgumentException("BatchName is required and cannot be empty.");
        }
        AsyncProcessStatus processStatus = asyncProcessStatusRepository.save(initializeAsyncProcessStatus());
        log.info("Saved async process status to CR_ASYNC_PROCESS_STATUS table");
        Connection con = null;
        long totalCount = 0;
        CrCloudTemplateHeadersView crCloudTemplateHeadersView;
        ExecutorService executor = Executors.newFixedThreadPool(noOfThreads);
        List<Future<?>> futures = new ArrayList<>();
        AtomicBoolean errorOccurred = new AtomicBoolean(false);
        try {
            CrCloudJobStatus crCloudJobStatus = insertIntoCrCloudJobStatus(customRestApiReqPo, Status.COMPLETED.getStatus());
            crCloudTemplateHeadersView = cloudTemplateHeadersViewRepository
                    .findById(customRestApiReqPo.getCldTemplateId())
                    .orElseThrow(() -> new Exception("Template not found with ID: " + customRestApiReqPo.getCldTemplateId()));
            log.info("TENANT --> " + customRestApiReqPo.getPodId());
            con = dynamicDataSourceBasedMultiTenantConnectionProvider.getConnection(String.valueOf(customRestApiReqPo.getPodId()));
            String countQuery = "SELECT COUNT(DISTINCT " + customRestApiReqPo.getCcidColumnName() + ") FROM " + crCloudTemplateHeadersView.getStagingTableName()
                    + " WHERE CR_BATCH_NAME=?";
            try (PreparedStatement stmnt = con.prepareStatement(countQuery)) {
                stmnt.setString(1, customRestApiReqPo.getBatchName());
                try (ResultSet rs = stmnt.executeQuery()) {
                    if (rs.next()) {
                        totalCount = rs.getLong(1);
                    } else {
                        throw new Exception("Failed to retrieve the total count");
                    }
                }
            }
            long loopCount = (long) Math.ceil((double) totalCount / batchSize);
            log.info("loopCount >>>>>> " + loopCount);
            log.info("totalCount >>>>> " + totalCount);
            for (int i = 0; i < loopCount; i++) {
                if (errorOccurred.get()) {
                    log.error("Stopping further processing due to an error in one of the threads");
                    break;
                }
                long offset = (i * batchSize) + 1;
                long limit = offset + batchSize - 1;

                log.info("offset >>>> " + offset);
                log.info("limit >>>>>> " + limit);

                Future<?> future = executor.submit(() -> {
                    try {
                        validateAndCreateClass.validateAndCreateAccounts(
                                crCloudTemplateHeadersView.getStagingTableName(),
                                offset,
                                limit,
                                customRestApiReqPo,
                                crCloudTemplateHeadersView,
                                processStatus
                        );
                    } catch (Exception e) {
                        log.error("Error in batch processing", e);
                        errorOccurred.set(true);
                        throw new RuntimeException(e);
                    }
                });
                futures.add(future);
            }
            for (Future<?> future : futures) {
                try {
                    future.get();
                } catch (Exception e) {
                    executor.shutdownNow();
                    throw new Exception(e.getMessage());
                }
            }
            if (!errorOccurred.get()) {
                processJobDaoImpl.cvrUpdate(
                        crCloudTemplateHeadersView.getStagingTableName(),
                        crCloudTemplateHeadersView.getSourceTemplateId(),
                        customRestApiReqPo.getBatchName(),
                        customRestApiReqPo.getLedgerColumnName(),
                        customRestApiReqPo.getCcidColumnName()
                );
                updateAsyncProcessStatus(processStatus.getAsyncProcessId(), "Completed");
            }

        } catch (Exception ex) {
            updateAsyncProcessStatus(processStatus.getAsyncProcessId(), "Error", ex);
            log.error("Error in validateCcid", ex);
            throw new Exception(ex);

        } finally {
            if (con != null) {
                try {
                    con.close();
                } catch (SQLException e) {
                    log.error("Error closing connection", e);
                }
            }
            executor.shutdown();
        }
    }
    */


    private AsyncProcessStatus initializeAsyncProcessStatus() {

        AsyncProcessStatus status = new AsyncProcessStatus();
        status.setAsyncProcessStatus("Starting");
        status.setCreatedDate(new Date());
        status.setAsyncStartTime(new Date());
        status.setAsyncProcessName("ValidateCcid");
        return status;
    }

    private void updateAsyncProcessStatus(Long asyncProcessId, String status) {
        log.info("update Async process status#####");
        Optional<AsyncProcessStatus> processStatusOptional = asyncProcessStatusRepository.findById(asyncProcessId);
        processStatusOptional.ifPresent(statusRecord -> {
            statusRecord.setAsyncProcessStatus(status);
            statusRecord.setUpdatedDate(new Date());
            statusRecord.setAsyncEndTime(new Date());
            asyncProcessStatusRepository.save(statusRecord);
        });
    }

    private void updateAsyncProcessStatus(Long asyncProcessId, String status, Exception ex) {
        log.info("update Async process status#####");
        Optional<AsyncProcessStatus> processStatusOptional = asyncProcessStatusRepository.findById(asyncProcessId);
        processStatusOptional.ifPresent(statusRecord -> {
            statusRecord.setAsyncProcessStatus(status);
            statusRecord.setUpdatedDate(new Date());
            statusRecord.setErrorMessage(ex.getMessage());
            asyncProcessStatusRepository.save(statusRecord);
        });
    }

    private void validateAndCreateAccounts(ResultSet rs, CustomRestApiReqPo customRestApiReqPo,
                                           CrCloudTemplateHeadersView crCloudTemplateHeadersView) throws ValidationException, Exception {
        log.info("Start of validateAndCreateAccounts in service ###");
        String ccidColumnName = customRestApiReqPo.getCcidColumnName();
        String ledgerNameCol = customRestApiReqPo.getLedgerColumnName();
        ValidateAndCreateAccountsResponse response = new ValidateAndCreateAccountsResponse();
        AccountCombinationServiceStub stub = new AccountCombinationServiceStub(
                customRestApiReqPo.getCloudUrl() + "/fscmService/AccountCombinationService");
        ValidateAndCreateAccounts validateCreateAccounts = new ValidateAndCreateAccounts();
        ServiceClient client = stub._getServiceClient();
        HttpTransportPropertiesImpl.Authenticator auth = new HttpTransportPropertiesImpl.Authenticator();
        auth.setUsername(customRestApiReqPo.getCldUserName());
        auth.setPassword(customRestApiReqPo.getCldPassword());
        client.getOptions().setProperty(org.apache.axis2.transport.http.HTTPConstants.AUTHENTICATE, auth);
        client.getOptions().setProperty(org.apache.axis2.transport.http.HTTPConstants.CHUNKED, false);

        stub._getServiceClient().getOptions().setProperty(HTTPConstants.SO_TIMEOUT, 600000);
        stub._getServiceClient().getOptions().setProperty(HTTPConstants.CONNECTION_TIMEOUT, 600000);

        int length = 0;
        int count = 0;
        String delimeter = "";
        while (rs.next()) {
            count += 1;
            AccountValidationInput accountValidationInput = new AccountValidationInput();
            String targetCcid = rs.getString(ccidColumnName);
            String[] segmentValuesArr = null;
            if (count == 1)
                delimeter = utils.findDelimeter(targetCcid);
            if (".".equalsIgnoreCase(delimeter))
                segmentValuesArr = targetCcid.split("\\" + delimeter);
            else {
                segmentValuesArr = targetCcid.split(delimeter);
            }
            length = segmentValuesArr.length;
            for (int i = 0; i < segmentValuesArr.length; i++) {
                Method m = getMethod(i + 1);
                // log.info(m.getName() + ":::methodName");
                m.invoke(accountValidationInput, segmentValuesArr[i]);

            }
            accountValidationInput.setLedgerName(rs.getString(ledgerNameCol));
            accountValidationInput.setEnabledFlag(true);
            validateCreateAccounts.addValidationInputRowList(accountValidationInput);
        }

        if (validateCreateAccounts.getValidationInputRowList() == null)
            throw new ValidationException("No Target CCID's Found for this particular Batch");
        response = stub.validateAndCreateAccounts(validateCreateAccounts);
        AccountValidationOutput[] accountValidationOutArr = response.getResult();

        List<CrValidateCvrCcid> crValidateCvrCcidLi = new ArrayList<>();
        for (AccountValidationOutput accountValidationOutput : accountValidationOutArr) {
            // AccountValidationOutput accountValidationObj=new AccountValidationOutput();
            String targetCcid = "";
            long ccid = accountValidationOutput.getCcId();
            log.info("soap ccid ::" + ccid);
            for (int j = 0; j < length; j++) {
                // String s=accountValidationOutput.getSegment1();
                String s = "";
                Method m1 = getMethod1(j + 1);
                // log.info(m1.getName() + ":::methodName");
                Object obj = m1.invoke(accountValidationOutput);
                if (obj != null)
                    s = obj.toString();
                // String s = getMethod1(j + 1).invoke(accountValidationOutput).toString();
                if (j != length - 1)
                    targetCcid += s + delimeter;
                else
                    targetCcid += s;

            }

            CrValidateCvrCcid validateCvrCcid = new CrValidateCvrCcid();
            validateCvrCcid.setCcid(targetCcid);
            validateCvrCcid.setCloudStagingTableName(crCloudTemplateHeadersView.getStagingTableName());
            validateCvrCcid.setCrBatchName(customRestApiReqPo.getBatchName());
            validateCvrCcid.setErrorMessage(accountValidationOutput.getError());
            validateCvrCcid.setErrorCode(accountValidationOutput.getErrorCode());
            validateCvrCcid.setStatus(accountValidationOutput.getStatus());
            validateCvrCcid.setRequestId(customRestApiReqPo.getRequestId());
            validateCvrCcid.setCreationDate(new java.sql.Date(new java.util.Date().getTime()));
            validateCvrCcid.setCreatedBy("ConvertRite");
            validateCvrCcid.setLastUpdatedDate(new java.sql.Date(new java.util.Date().getTime()));
            validateCvrCcid.setLastUpdateBy("ConvertRite");

            crValidateCvrCcidLi.add(validateCvrCcid);
        }
        List<CrValidateCvrCcid> cmValidateResLi = validateCvrCcidRepository.saveAll(crValidateCvrCcidLi);

        processJobDaoImpl.cvrUpdate(crCloudTemplateHeadersView.getStagingTableName(), crCloudTemplateHeadersView.getSourceTemplateId(), customRestApiReqPo.getBatchName(), ledgerNameCol, ccidColumnName);

    }

    private static Method getMethod(int index) throws NoSuchMethodException {
        Class<AccountValidationInput> clazz = AccountValidationInput.class;
        return clazz.getMethod("setSegment" + index, String.class);
    }

    private static Method getMethod1(int index) throws NoSuchMethodException, SecurityException {
        Class<AccountValidationOutput> clazz = AccountValidationOutput.class;
        return clazz.getMethod("getSegment" + index);
    }

    public void createOrUpdateBankAccount(CustomRestApiReqPo customRestApiReqPo) {
        Connection con = null;
        List<CrBankAccountErrors> bankAccountErrorsLi = new ArrayList<>();
        AsyncProcessStatus asyncStatus = null;
        CrCloudJobStatus crCloudJobStatus=insertIntoCrCloudJobStatus(customRestApiReqPo, Status.IN_PROGRESS.getStatus());
        try {
            asyncStatus = asyncProcessStatusService.startProcess("BankAccounts", customRestApiReqPo.getCldTemplateId(), customRestApiReqPo.getBatchName(), "ConvertRite");

            con = dynamicDataSourceBasedMultiTenantConnectionProvider.getConnection(String.valueOf(customRestApiReqPo.getPodId()));
            CrCloudTemplateHeadersView crCloudTemplateHeadersView = cloudTemplateHeadersViewRepository.findById(customRestApiReqPo.getCldTemplateId()).get();
            HttpHeaders headers = new HttpHeaders();
            headers.set("Accept", MediaType.APPLICATION_JSON_VALUE);
            headers.setContentType(MediaType.APPLICATION_JSON);
            headers.setBasicAuth(customRestApiReqPo.getCldUserName(), customRestApiReqPo.getCldPassword());

            String stagingTableName = crCloudTemplateHeadersView.getStagingTableName();
            PreparedStatement stmtA = con.prepareStatement("SELECT DISTINCT * FROM " + stagingTableName + " WHERE CR_BATCH_NAME = ?");
            stmtA.setString(1, customRestApiReqPo.getBatchName());
            ResultSet resultSetA = stmtA.executeQuery();

            List<CrBankAccountResPo> accountsList = getAllCashBankAccounts(headers, customRestApiReqPo);
            log.info("accountsList-->" + accountsList.size());

            Set<Long> accountIds = new HashSet<>();
            for (CrBankAccountResPo account : accountsList) {
                accountIds.add(account.getBankAccountId());
            }
            log.info("-accountIds-->" + accountIds.size());

            List<CompletableFuture<CrBankAccountErrors>> futures = new ArrayList<>();

            while (resultSetA.next()) {
                Long bankAccountId = null;
                if (resultSetA.getString("BANK_ACCOUNT_ID") != null) {
                    bankAccountId = Long.valueOf(resultSetA.getString("BANK_ACCOUNT_ID"));
                }
                log.info(accountIds.contains(bankAccountId) + "-bankAccountId-->" + bankAccountId);

                if (accountIds.contains(bankAccountId) && bankAccountId != null) {
                    futures.add(patchBankAccountAsync(resultSetA, customRestApiReqPo, headers, bankAccountId));
                } else {
                    futures.add(createBankAccountAsync(resultSetA, customRestApiReqPo, headers));
                }
            }

            // Wait for all futures to complete and gather results
            CompletableFuture<Void> allOf = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));

            // Block until all futures are done
            allOf.join();
            boolean hasError = false;
            boolean hasSuccess = false;
            // Now process the results
            for (CompletableFuture<CrBankAccountErrors> future : futures) {
                CrBankAccountErrors error = future.join();  // Get the result of each future
                if (error != null) {
                    bankAccountErrorsLi.add(error);
                    if (error.getStatus().equals(Status.ERROR.getStatus())) {
                        hasError = true;
                    } else if (error.getStatus().equals(Status.SUCCESS.getStatus())) {
                        hasSuccess = true;
                    }
                }
            }
            log.info("bankAccountErrorsLi---------{}" , bankAccountErrorsLi.size());
            updateAsyncProcessStatusAndCrCloudJobStatus(hasError,hasSuccess,asyncStatus,bankAccountErrorsLi.isEmpty(),customRestApiReqPo,crCloudJobStatus);

            if (!bankAccountErrorsLi.isEmpty()) {
                crBankAccountErrorsRepository.saveAll(bankAccountErrorsLi);
            }

        } catch (Exception e) {
            log.error("Error in createBankAccount()--->" + e.getMessage());
            e.printStackTrace();
            if (asyncStatus != null) {
                asyncProcessStatusService.endProcess(asyncStatus.getAsyncProcessId(), customRestApiReqPo.getCldTemplateId(), customRestApiReqPo.getBatchName(), Status.ERROR.getStatus(), e.getMessage(), "ConvertRite", crCloudJobStatus.getJobId());
            }
        }
    }

    private CompletableFuture<CrBankAccountErrors> patchBankAccountAsync(ResultSet rsBa, CustomRestApiReqPo customRestApiReqPo, HttpHeaders headers, Long bankAccountId) throws JsonProcessingException {
        return CompletableFuture.completedFuture(patchBankAccount(rsBa, customRestApiReqPo, headers, bankAccountId));
    }

    @Async
    public CompletableFuture<CrBankAccountErrors> createBankAccountAsync(ResultSet rsBa, CustomRestApiReqPo customRestApiReqPo, HttpHeaders headers) throws JsonProcessingException, SQLException {
        return CompletableFuture.completedFuture(createBankAccount(rsBa, customRestApiReqPo, headers));
    }

    private CrBankAccountErrors patchBankAccount(ResultSet resultSetA, CustomRestApiReqPo customRestApiReqPo, HttpHeaders headers, Long bankAccountId) throws JsonProcessingException {
        log.info("=========patchBankAccount========");
        CrUpdateBankAccountReqPo bankAccountReqPo = null;
        List<CrBankAccountErrors> createBankAccountErrorsLi = new ArrayList<>();
        CrBankAccountErrors error = new CrBankAccountErrors();
        try {
            bankAccountReqPo = new CrUpdateBankAccountReqPo();
            bankAccountReqPo = constructInputJsonforPatch(resultSetA);
            // Convert object to JSON string
            String jsonPayload = objectMapper.writeValueAsString(bankAccountReqPo);
            // Remove any unwanted CRLF characters and trim whitespace
            String cleanedJsonPayload = jsonPayload.replaceAll("\\r\\n|\\r|\\n", "").trim();

            log.info("Cleaned JSON Payload: " + cleanedJsonPayload);
            HttpEntity<String> createBankAccountRequest = new HttpEntity<>(cleanedJsonPayload, headers);
            log.info("bankAccountReqPo------>" + bankAccountReqPo);
            String url = customRestApiReqPo.getCloudUrl() + bankAccountCloudUrl + "/" + bankAccountId.toString().trim();
            log.info("url------>" + url);
            ResponseEntity<?> patchBankAccountRes = restTemplate.exchange(url,
                    HttpMethod.PATCH, createBankAccountRequest, Object.class);
            log.info("patchBankAccountRes--->" + patchBankAccountRes.getBody().toString());

            HttpStatus statusCode = patchBankAccountRes.getStatusCode();
            if (statusCode.value() == 200 || statusCode.value() == 201) {
                error = insertPatchErrorOrSuccessRecords(customRestApiReqPo, bankAccountReqPo, null, Status.SUCCESS.getStatus(), "PATCH", bankAccountId.toString());
            }
        } catch (Exception e) {
            log.error("Error whileCalling Update BankAccount PATCH API ---->" + e.getMessage());
            error = insertPatchErrorOrSuccessRecords(customRestApiReqPo, bankAccountReqPo, e.getMessage(), Status.ERROR.getStatus(), "PATCH", bankAccountId.toString());
        }
        return error;
    }

    private CrBankAccountErrors insertPatchErrorOrSuccessRecords(CustomRestApiReqPo customRestApiReqPo, CrUpdateBankAccountReqPo bankAccountReqPo, String errorMessage, String status, String apiType, String bankAccountId) {
        CrBankAccountErrors error = new CrBankAccountErrors();
        error.setErrorType(apiType);
        error.setStatus(status);
        error.setErrorMessage(errorMessage);
        error.setCrBatchName(customRestApiReqPo.getBatchName());
        error.setCreationDate(new java.sql.Date(new java.util.Date().getTime()));
        error.setCreatedBy("ConvertRite");
        error.setCldTempId(customRestApiReqPo.getCldTemplateId());
        error.setBankAccountId(bankAccountId);
        error.setAccountType(bankAccountReqPo.getAccountType());
        error.setApUseAllowedFlag(bankAccountReqPo.getApUseAllowedFlag().toString());
        error.setArUseAllowedFlag(bankAccountReqPo.getArUseAllowedFlag().toString());
        error.setBankAccountName(bankAccountReqPo.getBankAccountName());
        //fusion is allowing updation of bank account number but we should not allow as in general bank account number is unchanged
        //error.setBankAccountNumber(bankAccountReqPo.getBankAccountNumber());
        error.setBankBranchName(bankAccountReqPo.getBankBranchName());
        error.setCountryName(bankAccountReqPo.getCountryName());
        error.setCurrencyCode(bankAccountReqPo.getCurrencyCode());
        error.setLegalEntityName(bankAccountReqPo.getLegalEntityName());
        error.setMultiCurrencyAllowedFlag(bankAccountReqPo.getMultiCurrencyAllowedFlag().toString());
        error.setNettingAccountFlag(bankAccountReqPo.getNettingAccountFlag().toString());
        error.setPayUseAllowedFlag(bankAccountReqPo.getPayUseAllowedFlag().toString());
        error.setZeroAmountAllowed(bankAccountReqPo.getZeroAmountAllowed());
        return error;
    }

    private CrBankAccountErrors createBankAccount(ResultSet rsBa, CustomRestApiReqPo customRestApiReqPo, HttpHeaders headers) throws JsonProcessingException, SQLException {
        log.info("=========createBankAccount========");
        CrBankAccount crCreateBankAccountReqPo = constructInputJson(rsBa);
        CrBankAccountErrors error = null;
        try {
            // Convert object to JSON string
            String jsonPayload = objectMapper.writeValueAsString(crCreateBankAccountReqPo);
            // Remove any unwanted CRLF characters and trim whitespace
            String cleanedJsonPayload = jsonPayload.replaceAll("\\r\\n|\\r|\\n", "").trim();

            log.info("Cleaned JSON Payload: " + cleanedJsonPayload);

            HttpEntity<String> createBankAccountRequest = new HttpEntity<>(cleanedJsonPayload, headers);
            String url = customRestApiReqPo.getCloudUrl() + bankAccountCloudUrl;
            log.info("Request URL: " + url);

            ResponseEntity<?> createBankBranchRes = restTemplate.exchange(url, HttpMethod.POST, createBankAccountRequest, Object.class);
            log.info("Response Body: " + createBankBranchRes.getBody().toString());

            HttpStatus statusCode = createBankBranchRes.getStatusCode();
            log.info("Status Code: " + statusCode);
            if (statusCode.value() == 200 || statusCode.value() == 201) {
                error = insertErrorOrSuccessRecords(customRestApiReqPo, crCreateBankAccountReqPo, null, Status.SUCCESS.getStatus(), "POST");
            }
        } catch (Exception e) {
            log.error("Error while calling Create BankAccount Post API ----> " + e.getMessage());
            error = insertErrorOrSuccessRecords(customRestApiReqPo, crCreateBankAccountReqPo, e.getMessage(), Status.ERROR.getStatus(), "POST");
        }
        return error;
    }

    private CrBankAccountErrors insertErrorOrSuccessRecords(CustomRestApiReqPo customRestApiReqPo, CrBankAccount crCreateBankAccountReqPo, String errorMessage, String status, String apiType) {
        CrBankAccountErrors error = new CrBankAccountErrors();
        error.setStatus(status);
        error.setErrorType(apiType);
        error.setErrorMessage(errorMessage);
        error.setCrBatchName(customRestApiReqPo.getBatchName());
        error.setCreationDate(new java.sql.Date(new java.util.Date().getTime()));
        error.setCreatedBy("ConvertRite");
        error.setCldTempId(customRestApiReqPo.getCldTemplateId());
        error.setAccountHolderName(crCreateBankAccountReqPo.getAccountHolderName());
        error.setAccountType(crCreateBankAccountReqPo.getAccountType());
        error.setApUseAllowedFlag(crCreateBankAccountReqPo.getApUseAllowedFlag());
        error.setArUseAllowedFlag(crCreateBankAccountReqPo.getArUseAllowedFlag());
        error.setBankAccountName(crCreateBankAccountReqPo.getBankAccountName());
        error.setBankAccountNumber(crCreateBankAccountReqPo.getBankAccountNumber());
        error.setBankBranchName(crCreateBankAccountReqPo.getBankBranchName());
        error.setBankName(crCreateBankAccountReqPo.getBankName());
        error.setCountryName(crCreateBankAccountReqPo.getCountryName());
        error.setCurrencyCode(crCreateBankAccountReqPo.getCurrencyCode());
        error.setLegalEntityName(crCreateBankAccountReqPo.getLegalEntityName());
        error.setMultiCurrencyAllowedFlag(crCreateBankAccountReqPo.getMultiCurrencyAllowedFlag());
        error.setNettingAccountFlag(crCreateBankAccountReqPo.getNettingAccountFlag());
        error.setPayUseAllowedFlag(crCreateBankAccountReqPo.getPayUseAllowedFlag());
        error.setZeroAmountAllowed(crCreateBankAccountReqPo.getZeroAmountAllowed());
        return error;
    }

    private CrBankAccount constructInputJson(ResultSet rsBa) throws SQLException {
        CrBankAccount crCreateBankAccountReqPo = new CrBankAccount();
        log.info("ACCOUNT_TYPE--->" + rsBa.getString("ACCOUNT_TYPE"));
        log.info("BRANCH_NUMBER--->" + rsBa.getString("BRANCH_NUMBER"));
        log.info("BANK_NUMBER--->" + rsBa.getString("BANK_NUMBER"));
        log.info("CURRENCY_CODE--->" + rsBa.getString("CURRENCY_CODE"));
        crCreateBankAccountReqPo.setAccountHolderName("BAG BUSINESS UNIT");
        crCreateBankAccountReqPo.setAccountHolderName(rsBa.getString("ACCOUNT_HOLDER_NAME"));
        crCreateBankAccountReqPo.setApUseAllowedFlag(rsBa.getString("AP_USE_ALLOWED_FLAG"));
        crCreateBankAccountReqPo.setArUseAllowedFlag(rsBa.getString("AR_USE_ALLOWED_FLAG"));
        crCreateBankAccountReqPo.setBankAccountName(rsBa.getString("BANK_ACCOUNT_NAME"));
        crCreateBankAccountReqPo.setBankAccountNumber(rsBa.getString("BANK_ACCOUNT_NUMBER"));
        crCreateBankAccountReqPo.setCurrencyCode(rsBa.getString("CURRENCY_CODE"));
        crCreateBankAccountReqPo.setCurrencyCode(rsBa.getString("CURRENCY_CODE"));
        crCreateBankAccountReqPo.setMultiCurrencyAllowedFlag(rsBa.getString("MULTI_CURRENCY_ALLOWED_FLAG"));
        crCreateBankAccountReqPo.setNettingAccountFlag(rsBa.getString("NETTING_ACCOUNT_FLAG"));
        crCreateBankAccountReqPo.setPayUseAllowedFlag(rsBa.getString("PAY_USE_ALLOWED_FLAG"));
        crCreateBankAccountReqPo.setZeroAmountAllowed(rsBa.getString("ZERO_AMOUNT_ALLOWED"));
        crCreateBankAccountReqPo.setBankName(rsBa.getString("BANK_NAME"));
        crCreateBankAccountReqPo.setBankBranchName(rsBa.getString("BANK_BRANCH_NAME"));
        crCreateBankAccountReqPo.setLegalEntityName(rsBa.getString("LEGAL_ENTITY_NAME"));
        crCreateBankAccountReqPo.setAccountType(rsBa.getString("ACCOUNT_TYPE"));
        crCreateBankAccountReqPo.setCountryName(rsBa.getString("COUNTRY_NAME"));

        List<BankAccountUse> bankAccountUses = new ArrayList<>();
        BankAccountUse bankAccountUse = new BankAccountUse();

        bankAccountUse.setBusinessUnitName(rsBa.getString("ACCOUNT_HOLDER_NAME"));
        bankAccountUses.add(bankAccountUse);
        crCreateBankAccountReqPo.setBankAccountUses(bankAccountUses);
        return crCreateBankAccountReqPo;
    }

    private CrUpdateBankAccountReqPo constructInputJsonforPatch(ResultSet rsBa) throws SQLException {
        CrUpdateBankAccountReqPo crCreateBankAccountReqPo = new CrUpdateBankAccountReqPo();

        log.info("ACCOUNT_TYPE--->" + rsBa.getString("ACCOUNT_TYPE"));
        log.info("BRANCH_NUMBER--->" + rsBa.getString("BRANCH_NUMBER"));
        log.info("BANK_NUMBER--->" + rsBa.getString("BANK_NUMBER"));
        log.info("CURRENCY_CODE--->" + rsBa.getString("CURRENCY_CODE"));
        log.info("LEGAL_ENTITY_NAME--->" + rsBa.getString("LEGAL_ENTITY_NAME"));
        crCreateBankAccountReqPo.setApUseAllowedFlag(Boolean.valueOf(rsBa.getString("AP_USE_ALLOWED_FLAG")));
        crCreateBankAccountReqPo.setArUseAllowedFlag(Boolean.valueOf(rsBa.getString("AR_USE_ALLOWED_FLAG")));
        crCreateBankAccountReqPo.setBankAccountName(rsBa.getString("BANK_ACCOUNT_NAME"));
        crCreateBankAccountReqPo.setBankAccountNumber(rsBa.getString("BANK_ACCOUNT_NUMBER"));
        crCreateBankAccountReqPo.setCurrencyCode(rsBa.getString("CURRENCY_CODE"));
        crCreateBankAccountReqPo.setCurrencyCode(rsBa.getString("CURRENCY_CODE"));
        crCreateBankAccountReqPo.setMultiCurrencyAllowedFlag(Boolean.valueOf(rsBa.getString("MULTI_CURRENCY_ALLOWED_FLAG")));
        crCreateBankAccountReqPo.setNettingAccountFlag(Boolean.valueOf(rsBa.getString("NETTING_ACCOUNT_FLAG")));
        crCreateBankAccountReqPo.setPayUseAllowedFlag(Boolean.valueOf(rsBa.getString("PAY_USE_ALLOWED_FLAG")));
        crCreateBankAccountReqPo.setZeroAmountAllowed(rsBa.getString("ZERO_AMOUNT_ALLOWED"));
        crCreateBankAccountReqPo.setBankBranchName(rsBa.getString("BANK_BRANCH_NAME"));
        crCreateBankAccountReqPo.setLegalEntityName(rsBa.getString("LEGAL_ENTITY_NAME"));
        crCreateBankAccountReqPo.setAccountType(rsBa.getString("ACCOUNT_TYPE"));
        crCreateBankAccountReqPo.setCountryName(rsBa.getString("COUNTRY_NAME"));
        crCreateBankAccountReqPo.setAccountHolderNameAlt(rsBa.getString("ACCOUNT_HOLDER_NAME_ALT"));
        crCreateBankAccountReqPo.setAccountingConversionRateType(rsBa.getString("ACCOUNTING_CONVERSION_RATE_TYPE"));
        crCreateBankAccountReqPo.setAccountSuffix(rsBa.getString("ACCOUNT_SUFFIX"));
        crCreateBankAccountReqPo.setAgencyLocationCode(rsBa.getString("AGENCY_LOCATION_CODE"));
        crCreateBankAccountReqPo.setBankAccountId(rsBa.getLong("BANK_ACCOUNT_ID"));        //
        crCreateBankAccountReqPo.setBankAccountNameAlt(rsBa.getString("BANK_ACCOUNT_NAME_ALT"));
        crCreateBankAccountReqPo.setBankAccountNumberElectronic(rsBa.getString("BANK_ACCOUNT_NUMBER_ELECTRONIC"));
        crCreateBankAccountReqPo.setBankExchangeRateType(rsBa.getString("BANK_EXCHANGE_RATE_TYPE"));
        crCreateBankAccountReqPo.setBankNumber(rsBa.getString("BANK_NUMBER"));
        crCreateBankAccountReqPo.setBranchNumber(rsBa.getString("BRANCH_NUMBER"));
        crCreateBankAccountReqPo.setCheckDigits(rsBa.getString("CHECK_DIGITS"));
        crCreateBankAccountReqPo.setDataSecurityFlag(Boolean.valueOf(rsBa.getString("DATA_SECURITY_FLAG")));
        crCreateBankAccountReqPo.setDescription(rsBa.getString("DESCRIPTION"));
        crCreateBankAccountReqPo.setEFTUserNumber(rsBa.getString("EFT_USER_NUMBER"));
        crCreateBankAccountReqPo.setEndDate(rsBa.getString("END_DATE"));
        crCreateBankAccountReqPo.setGlReconStartDate(rsBa.getString("GL_RECON_START_DATE"));
        crCreateBankAccountReqPo.setIbanNumber(rsBa.getString("IBAN_NUMBER"));
        crCreateBankAccountReqPo.setMaskedIBAN(rsBa.getString("MASKED_IBAN"));
        crCreateBankAccountReqPo.setMaximumCheckAmount(rsBa.getDouble("MAXIMUM_CHECK_AMOUNT"));
        crCreateBankAccountReqPo.setMaximumOutlay(rsBa.getDouble("MAXIMUM_OUTLAY"));
        crCreateBankAccountReqPo.setMinimumCheckAmount(rsBa.getDouble("MINIMUM_CHECK_AMOUNT"));
        crCreateBankAccountReqPo.setMultiCashReconEnabledFlag(Boolean.valueOf(rsBa.getString("MULTI_CASH_RECON_ENABLED_FLAG")));
        crCreateBankAccountReqPo.setParsingRuleSetName(rsBa.getString("PARSING_RULE_SET_NAME"));
        crCreateBankAccountReqPo.setPooledFlag(Boolean.valueOf(rsBa.getString("POOLED_FLAG")));
        crCreateBankAccountReqPo.setReconStartDate(rsBa.getString("RECON_START_DATE"));
        crCreateBankAccountReqPo.setReversalProcessingMethod(rsBa.getString("REVERSAL_PROCESSING_METHOD"));
        crCreateBankAccountReqPo.setRulesetName(rsBa.getString("RULESET_NAME"));
        crCreateBankAccountReqPo.setSecondaryAccountReference(rsBa.getString("SECONDARY_ACCOUNT_REFERENCE"));
        crCreateBankAccountReqPo.setTargetBalance(rsBa.getDouble("TARGET_BALANCE"));
        crCreateBankAccountReqPo.setToleranceRuleName(rsBa.getString("TOLERANCE_RULE_NAME"));
        crCreateBankAccountReqPo.setTransactionCalendar(rsBa.getString("TRANSACTION_CALENDAR"));

        return crCreateBankAccountReqPo;
    }

    public void updateCldStagingTable(String updateType, CustomRestApiReqPo customRestApiReqPo) throws SQLException {
        try {
            log.info("==============updateCldStagingTable=================" + customRestApiReqPo);
            ResultSet res = null;
            HttpHeaders headers = new HttpHeaders();
            headers.set("Accept", MediaType.APPLICATION_JSON_VALUE);
            headers.setContentType(MediaType.APPLICATION_JSON);
            headers.setBasicAuth(customRestApiReqPo.getCldUserName(), customRestApiReqPo.getCldPassword());

            log.info("TENANT-->" + customRestApiReqPo.getPodId());
            CrCloudTemplateHeadersView crCloudTemplateHeadersView = cloudTemplateHeadersViewRepository
                    .findById(customRestApiReqPo.getCldTemplateId()).get();
            String stagingTableName = crCloudTemplateHeadersView.getStagingTableName();
            Connection con = dynamicDataSourceBasedMultiTenantConnectionProvider.getConnection(String.valueOf(customRestApiReqPo.getPodId()));

            if (updateType.equalsIgnoreCase("branches")) {
                List<CrBranchesResPo> branchesList = getAllCashBranches(headers, customRestApiReqPo);
                log.info("branchesList--->" + branchesList.size());
                String query = "SELECT * FROM " + stagingTableName + " WHERE cr_batch_name = '" + customRestApiReqPo.getBatchName() + "'";
                PreparedStatement stmnt = con.prepareStatement(query);
                ResultSet rs = stmnt.executeQuery();
                while (rs.next()) {
                    String rsBranchName = rs.getString("BRANCH_NAME");
                    String rsBranchNumber = rs.getString("BRANCH_NUMBER");
                    String rsPartyId = rs.getString("BRANCH_PARTY_ID");
                    String rsCountry = rs.getString("COUNTRY_NAME");
                    log.info("rsBranchNumber--->" + rsBranchNumber);
                    log.info("rsBranchName--->" + rsBranchName);
                    log.info("rsCountry--->" + rsCountry);
                    log.info("rsPartyId--->" + rsPartyId);
                    for (CrBranchesResPo branch : branchesList) {
                        if (rsCountry != null && rsBranchName != null && branch.getBankBranchName() != null &&
                                branch.getBankBranchName().trim().equalsIgnoreCase(rsBranchName.trim()) &&
                                rsBranchNumber != null && branch.getBranchNumber() != null &&
                                branch.getBranchNumber().equals(rsBranchNumber) &&
                                branch.getCountryName().trim().equalsIgnoreCase(rsCountry.trim())) {
                            log.info("==========if============");
                            // Update the bank party id in the staging table
                            String updateQuery = "UPDATE " + stagingTableName + " SET BRANCH_PARTY_ID = " + branch.getBranchPartyId() + " WHERE BRANCH_NUMBER = '" + rsBranchNumber + "' AND BRANCH_NAME = '" + rsBranchName + "' AND cr_batch_name = '" + customRestApiReqPo.getBatchName() + "'";
                            PreparedStatement updateStmnt = con.prepareStatement(updateQuery);
                            log.info("updateQuery--->" + updateQuery);
                            updateStmnt.executeUpdate();
                            updateStmnt.close();
                        }
                    }
                }
            }
            if (updateType.equalsIgnoreCase("banks")) {
                List<CrBanksResPo> banksList = getAllCashBanks(headers, customRestApiReqPo);
                log.info("banksList--->" + banksList.size());
                String query = "SELECT * FROM " + stagingTableName + " WHERE cr_batch_name = '" + customRestApiReqPo.getBatchName() + "'";
                PreparedStatement stmnt = con.prepareStatement(query);
                ResultSet rs = stmnt.executeQuery();
                while (rs.next()) {
                    String rsBankName = rs.getString("BANK_NAME");
                    String rsBankNumber = rs.getString("BANK_NUMBER");
                    String rsPartyId = rs.getString("BANK_PARTY_ID");
                    String rsCountry = rs.getString("COUNTRY_NAME");
                    log.info("rsBankNumber--->" + rsBankNumber);
                    log.info("rsPartyId--->" + rsPartyId);
                    log.info("rsCountry--->" + rsCountry);
                    for (CrBanksResPo bank : banksList) {
                        if (rsCountry != null && rsBankName != null && bank.getBankName() != null && bank.getBankName().equals(rsBankName) && rsBankNumber != null && bank.getBankNumber() != null && bank.getBankNumber().equals(rsBankNumber) && bank.getCountryName().equals(rsCountry)) {
                            log.info("==========if============");
                            // Update the bank party id in the staging table
                            String updateQuery = "UPDATE " + stagingTableName + " SET BANK_PARTY_ID = " + bank.getBankPartyId() + " WHERE BANK_NUMBER = '" + rsBankNumber + "' AND BANK_NAME = '" + rsBankName + "' AND cr_batch_name = '" + customRestApiReqPo.getBatchName() + "'";
                            PreparedStatement updateStmnt = con.prepareStatement(updateQuery);
                            log.info("updateQuery--->" + updateQuery);
                            updateStmnt.executeUpdate();
                            updateStmnt.close();
                        }
                    }
                }
            }

            if (updateType.equalsIgnoreCase("accounts")) {
                List<CrBankAccountResPo> accountsList = getAllCashBankAccounts(headers, customRestApiReqPo);
                log.info("accountsList--->" + accountsList.size());
                String query = "SELECT * FROM " + stagingTableName + " WHERE cr_batch_name = '" + customRestApiReqPo.getBatchName() + "'";
                PreparedStatement stmnt = con.prepareStatement(query);
                ResultSet rs = stmnt.executeQuery();
                while (rs.next()) {
                    String rsBankNumber = rs.getString("BANK_NUMBER");
                    String rsBranchNumber = rs.getString("BRANCH_NUMBER");
                    String rsAccountNumber = rs.getString("BANK_ACCOUNT_NUMBER");
                    log.info("rsBankNumber--->" + rsBankNumber);
                    log.info("rsBranchNumber--->" + rsBranchNumber);
                    log.info("rsAccountNumber--->" + rsAccountNumber);

                    for (CrBankAccountResPo bankAccount : accountsList) {
                        if (rsBankNumber != null && rsBranchNumber != null && rsAccountNumber != null && bankAccount.getBranchNumber() != null && bankAccount.getBankAccountNumber() != null) {
                            log.info("==========if=======1=====");
                            //  (bank.getBankNumber().equals(rsBankNumber) && bank.getBankNumber() != null &&
                            if (bankAccount.getBranchNumber().equals(rsBranchNumber) && (bankAccount.getBankAccountNumber().equals(rsAccountNumber))) {
                                log.info("==========if=======2=====");
                                // Update the bank account id in the staging table
                                String updateQuery = "UPDATE " + stagingTableName + " SET BANK_ACCOUNT_ID = " + bankAccount.getBankAccountId() + " WHERE BANK_NUMBER = '" + rsBankNumber + "' AND BRANCH_NUMBER = '" + rsBranchNumber + "' AND cr_batch_name = '" + customRestApiReqPo.getBatchName() + "'";
                                PreparedStatement updateStmnt = con.prepareStatement(updateQuery);
                                log.info("updateQuery--->" + updateQuery);
                                updateStmnt.executeUpdate();
                                updateStmnt.close();
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error("Exception in updateCldStagingTable-->" + e.getMessage());
            // e.printStackTrace();
        }
    }



   /* public void updateCldStagingTable(String updateType, CustomRestApiReqPo customRestApiReqPo) throws SQLException {
        try {
            log.info("==============updateCldStagingTable=================" + customRestApiReqPo);
            HttpHeaders headers = new HttpHeaders();
            headers.set("Accept", MediaType.APPLICATION_JSON_VALUE);
            headers.setContentType(MediaType.APPLICATION_JSON);
            headers.setBasicAuth(customRestApiReqPo.getCldUserName(), customRestApiReqPo.getCldPassword());
            log.info("TENANT-->" + customRestApiReqPo.getPodId());
            CrCloudTemplateHeadersView crCloudTemplateHeadersView = cloudTemplateHeadersViewRepository
                    .findById(customRestApiReqPo.getCldTemplateId()).orElseThrow(() -> new SQLException("Template not found"));
            String stagingTableName = crCloudTemplateHeadersView.getStagingTableName();
            Connection con = dynamicDataSourceBasedMultiTenantConnectionProvider.getConnection(String.valueOf(customRestApiReqPo.getPodId()));
            if ("branches".equalsIgnoreCase(updateType)) {
                List<CrBranchesResPo> branchesList = getAllCashBranches(headers, customRestApiReqPo);
                log.info("branchesList--->" + branchesList.size());
                String query = "SELECT * FROM " + stagingTableName + " WHERE cr_batch_name = ?";
                try (PreparedStatement stmnt = con.prepareStatement(query)) {
                    stmnt.setString(1, customRestApiReqPo.getBatchName());
                    ResultSet rs = stmnt.executeQuery();
                    while (rs.next()) {
                        String rsBranchName = rs.getString("BRANCH_NAME");
                        String rsBranchNumber = rs.getString("BRANCH_NUMBER");
                        String rsCountry = rs.getString("COUNTRY_NAME");
                        log.info("rsBranchNumber--->" + rsBranchNumber);
                        log.info("rsBranchName--->" + rsBranchName);
                        log.info("rsCountry--->" + rsCountry);
                        for (CrBranchesResPo branch : branchesList) {
                            if (rsCountry != null && rsBranchName != null && branch.getBankBranchName() != null &&
                                    branch.getBankBranchName().trim().equalsIgnoreCase(rsBranchName.trim()) &&
                                    rsBranchNumber != null && branch.getBranchNumber() != null &&
                                    branch.getBranchNumber().equals(rsBranchNumber) &&
                                    branch.getCountryName().trim().equalsIgnoreCase(rsCountry.trim())) {
                                log.info("==========if============");
                                String updateQuery = "UPDATE " + stagingTableName + " SET BRANCH_PARTY_ID = ? WHERE BRANCH_NUMBER = ? AND BRANCH_NAME = ? AND cr_batch_name = ?";
                                try (PreparedStatement updateStmnt = con.prepareStatement(updateQuery)) {
                                    updateStmnt.setInt(1, Math.toIntExact(branch.getBranchPartyId()));
                                    updateStmnt.setString(2, rsBranchNumber);
                                    updateStmnt.setString(3, rsBranchName);
                                    updateStmnt.setString(4, customRestApiReqPo.getBatchName());
                                    updateStmnt.executeUpdate();
                                }
                            }
                        }
                    }
                }
            }
            if ("banks".equalsIgnoreCase(updateType)) {
                List<CrBanksResPo> banksList = getAllCashBanks(headers, customRestApiReqPo);
                log.info("banksList--->" + banksList.size());
                String query = "SELECT * FROM " + stagingTableName + " WHERE cr_batch_name = ?";
                try (PreparedStatement stmnt = con.prepareStatement(query)) {
                    stmnt.setString(1, customRestApiReqPo.getBatchName());
                    ResultSet rs = stmnt.executeQuery();
                    while (rs.next()) {
                        String rsBankName = rs.getString("BANK_NAME");
                        String rsBankNumber = rs.getString("BANK_NUMBER");
                        String rsCountry = rs.getString("COUNTRY_NAME");
                        log.info("rsBankNumber--->" + rsBankNumber);
                        log.info("rsCountry--->" + rsCountry);
                        for (CrBanksResPo bank : banksList) {
                            if (rsCountry != null && rsBankName != null && bank.getBankName() != null &&
                                    bank.getBankName().equals(rsBankName) &&
                                    rsBankNumber != null && bank.getBankNumber() != null &&
                                    bank.getBankNumber().equals(rsBankNumber) &&
                                    bank.getCountryName().equals(rsCountry)) {
                                log.info("==========if============");
                                String updateQuery = "UPDATE " + stagingTableName + " SET BANK_PARTY_ID = ? WHERE BANK_NUMBER = ? AND BANK_NAME = ? AND cr_batch_name = ?";
                                try (PreparedStatement updateStmnt = con.prepareStatement(updateQuery)) {
                                    updateStmnt.setInt(1, Math.toIntExact(bank.getBankPartyId()));
                                    updateStmnt.setString(2, rsBankNumber);
                                    updateStmnt.setString(3, rsBankName);
                                    updateStmnt.setString(4, customRestApiReqPo.getBatchName());
                                    updateStmnt.executeUpdate();
                                }
                            }
                        }
                    }
                }
            }
            if ("accounts".equalsIgnoreCase(updateType)) {
                List<CrBankAccountResPo> accountsList = getAllCashBankAccounts(headers, customRestApiReqPo);
                log.info("accountsList--->" + accountsList.size());
                String query = "SELECT * FROM " + stagingTableName + " WHERE cr_batch_name = ?";
                try (PreparedStatement stmnt = con.prepareStatement(query)) {
                    stmnt.setString(1, customRestApiReqPo.getBatchName());
                    ResultSet rs = stmnt.executeQuery();
                    while (rs.next()) {
                        String rsBankNumber = rs.getString("BANK_NUMBER");
                        String rsBranchNumber = rs.getString("BRANCH_NUMBER");
                        String rsAccountNumber = rs.getString("BANK_ACCOUNT_NUMBER");

                        log.info("rsBankNumber--->" + rsBankNumber);
                        log.info("rsBranchNumber--->" + rsBranchNumber);
                        log.info("rsAccountNumber--->" + rsAccountNumber);

                        for (CrBankAccountResPo bankAccount : accountsList) {
                            if (rsBankNumber != null && rsBranchNumber != null && rsAccountNumber != null &&
                                    bankAccount.getBranchNumber() != null && bankAccount.getBankAccountNumber() != null) {
                                if (bankAccount.getBranchNumber().equals(rsBranchNumber) && bankAccount.getBankAccountNumber().equals(rsAccountNumber)) {
                                    log.info("==========if=======2=====");
                                    String updateQuery = "UPDATE " + stagingTableName + " SET BANK_ACCOUNT_ID = ? WHERE BANK_NUMBER = ? AND BRANCH_NUMBER = ? AND cr_batch_name = ?";
                                    try (PreparedStatement updateStmnt = con.prepareStatement(updateQuery)) {
                                        updateStmnt.setInt(1, Math.toIntExact(bankAccount.getBankAccountId()));
                                        updateStmnt.setString(2, rsBankNumber);
                                        updateStmnt.setString(3, rsBranchNumber);
                                        updateStmnt.setString(4, customRestApiReqPo.getBatchName());
                                        updateStmnt.executeUpdate();
                                    }
                                }
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error("Exception in updateCldStagingTable-->" + e.getMessage());
            throw new SQLException("Error in updateCldStagingTable: " + e.getMessage(), e);
        }
    }

    */
}
