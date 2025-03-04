package com.rite.products.convertrite.utils;

import com.rite.products.convertrite.exception.ValidationException;
import com.rite.products.convertrite.model.AsyncProcessStatus;
import com.rite.products.convertrite.model.CrCloudTemplateHeadersView;
import com.rite.products.convertrite.model.CrValidateCvrCcid;
import com.rite.products.convertrite.multitenancy.config.tenant.hibernate.DynamicDataSourceBasedMultiTenantConnectionProvider;
import com.rite.products.convertrite.po.CustomRestApiReqPo;
import com.rite.products.convertrite.respository.AsyncProcessStatusRepository;
import com.rite.products.convertrite.respository.CrValidateCvrCcidRepository;
import com.rite.products.convertrite.respository.ProcessJobDaoImpl;
import com.rite.products.convertrite.stubs.accountcombinationservice.AccountCombinationServiceStub;
import lombok.extern.slf4j.Slf4j;
import org.apache.axis2.client.ServiceClient;
import org.apache.axis2.transport.http.HTTPConstants;
import org.apache.axis2.transport.http.impl.httpclient4.HttpTransportPropertiesImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;
@Component
@Slf4j
public class ValidateAndCreateClass {
    @Autowired
    CrValidateCvrCcidRepository validateCvrCcidRepository;
    @Autowired
    ProcessJobDaoImpl processJobDaoImpl;
    @Autowired
    Utils utils;
    @Autowired
    AsyncProcessStatusRepository asyncProcessStatusRepository;
    @Autowired
    DynamicDataSourceBasedMultiTenantConnectionProvider dynamicDataSourceBasedMultiTenantConnectionProvider;

//    @Async
//    public AsyncProcessStatus validateAndCreateAccounts(ResultSet rs, CustomRestApiReqPo customRestApiReqPo,
//                                                        CrCloudTemplateHeadersView crCloudTemplateHeadersView, AsyncProcessStatus asyncProcessStatus) throws ValidationException, Exception {
//        log.info("Start of validateAndCreateAccounts in service ###");
//
//
//        String ccidColumnName = customRestApiReqPo.getCcidColumnName();
//        String ledgerNameCol = customRestApiReqPo.getLedgerColumnName();
//        AccountCombinationServiceStub.ValidateAndCreateAccountsResponse response = new AccountCombinationServiceStub.ValidateAndCreateAccountsResponse();
//        AccountCombinationServiceStub stub = new AccountCombinationServiceStub(
//                customRestApiReqPo.getCloudUrl() + "/fscmService/AccountCombinationService");
//        AccountCombinationServiceStub.ValidateAndCreateAccounts validateCreateAccounts = new AccountCombinationServiceStub.ValidateAndCreateAccounts();
//        ServiceClient client = stub._getServiceClient();
//        HttpTransportPropertiesImpl.Authenticator auth = new HttpTransportPropertiesImpl.Authenticator();
//        auth.setUsername(customRestApiReqPo.getCldUserName());
//        auth.setPassword(customRestApiReqPo.getCldPassword());
//        client.getOptions().setProperty(org.apache.axis2.transport.http.HTTPConstants.AUTHENTICATE, auth);
//        client.getOptions().setProperty(org.apache.axis2.transport.http.HTTPConstants.CHUNKED, false);
//
//        stub._getServiceClient().getOptions().setProperty(HTTPConstants.SO_TIMEOUT, 600000);
//        stub._getServiceClient().getOptions().setProperty(HTTPConstants.CONNECTION_TIMEOUT, 600000);
//
//        int length = 0;
//        int count = 0;
//        String delimeter = "";
//        while (rs.next()) {
//            count += 1;
//            AccountCombinationServiceStub.AccountValidationInput accountValidationInput = new AccountCombinationServiceStub.AccountValidationInput();
//            String targetCcid = rs.getString(ccidColumnName);
//            String[] segmentValuesArr = null;
//            if (count == 1)
//                delimeter = utils.findDelimeter(targetCcid);
//            if (".".equalsIgnoreCase(delimeter))
//                segmentValuesArr = targetCcid.split("\\" + delimeter);
//            else {
//                segmentValuesArr = targetCcid.split(delimeter);
//            }
//            length = segmentValuesArr.length;
//            for (int i = 0; i < segmentValuesArr.length; i++) {
//                Method m = getMethod(i + 1);
//                // log.info(m.getName() + ":::methodName");
//                m.invoke(accountValidationInput, segmentValuesArr[i]);
//
//            }
//            accountValidationInput.setLedgerName(rs.getString(ledgerNameCol));
//            accountValidationInput.setEnabledFlag(true);
//            validateCreateAccounts.addValidationInputRowList(accountValidationInput);
//        }
//
//        if (validateCreateAccounts.getValidationInputRowList() == null)
//            throw new ValidationException("No Target CCID's Found for this particular Batch");
//        response = stub.validateAndCreateAccounts(validateCreateAccounts);
//        AccountCombinationServiceStub.AccountValidationOutput[] accountValidationOutArr = response.getResult();
//
//        List<CrValidateCvrCcid> crValidateCvrCcidLi = new ArrayList<>();
//        for (AccountCombinationServiceStub.AccountValidationOutput accountValidationOutput : accountValidationOutArr) {
//            // AccountValidationOutput accountValidationObj=new AccountValidationOutput();
//            String targetCcid = "";
//            long ccid = accountValidationOutput.getCcId();
//            log.info("soap ccid ::" + ccid);
//            for (int j = 0; j < length; j++) {
//                // String s=accountValidationOutput.getSegment1();
//                String s = "";
//                Method m1 = getMethod1(j + 1);
//                // log.info(m1.getName() + ":::methodName");
//                Object obj = m1.invoke(accountValidationOutput);
//                if (obj != null)
//                    s = obj.toString();
//                // String s = getMethod1(j + 1).invoke(accountValidationOutput).toString();
//                if (j != length - 1)
//                    targetCcid += s + delimeter;
//                else
//                    targetCcid += s;
//
//            }
//
//            CrValidateCvrCcid validateCvrCcid = new CrValidateCvrCcid();
//            validateCvrCcid.setCcid(targetCcid);
//            validateCvrCcid.setCloudStagingTableName(crCloudTemplateHeadersView.getStagingTableName());
//            validateCvrCcid.setCrBatchName(customRestApiReqPo.getBatchName());
//            validateCvrCcid.setErrorMessage(accountValidationOutput.getError());
//            validateCvrCcid.setErrorCode(accountValidationOutput.getErrorCode());
//            validateCvrCcid.setStatus(accountValidationOutput.getStatus());
//            validateCvrCcid.setRequestId(customRestApiReqPo.getRequestId());
//            validateCvrCcid.setCreationDate(new java.sql.Date(new java.util.Date().getTime()));
//            validateCvrCcid.setCreatedBy("ConvertRite");
//            validateCvrCcid.setLastUpdatedDate(new java.sql.Date(new java.util.Date().getTime()));
//            validateCvrCcid.setLastUpdateBy("ConvertRite");
//
//            crValidateCvrCcidLi.add(validateCvrCcid);
//        }
//        List<CrValidateCvrCcid> cmValidateResLi = validateCvrCcidRepository.saveAll(crValidateCvrCcidLi);
//        Optional<AsyncProcessStatus> processStatusOptional = asyncProcessStatusRepository.findById(asyncProcessStatus.getAsyncProcessId());
//        AsyncProcessStatus updateProcess = new AsyncProcessStatus();
//        if (processStatusOptional.isPresent()) {
//            AsyncProcessStatus processStatus = processStatusOptional.get();
//            processStatus.setAsyncProcessStatus("Processing");
//            processStatus.setUpdatedDate(new Date());
//            updateProcess = asyncProcessStatusRepository.save(processStatus);
//        }
//
//        return updateProcess;
//    }
//
//    private static Method getMethod(int index) throws NoSuchMethodException {
//        Class<AccountCombinationServiceStub.AccountValidationInput> clazz = AccountCombinationServiceStub.AccountValidationInput.class;
//        return clazz.getMethod("setSegment" + index, String.class);
//    }
//
//    private static Method getMethod1(int index) throws NoSuchMethodException, SecurityException {
//        Class<AccountCombinationServiceStub.AccountValidationOutput> clazz = AccountCombinationServiceStub.AccountValidationOutput.class;
//        return clazz.getMethod("getSegment" + index);
//    }

    public AsyncProcessStatus validateAndCreateAccounts(String stagingTableName, long offset, long limit, CustomRestApiReqPo customRestApiReqPo,
                                                        CrCloudTemplateHeadersView crCloudTemplateHeadersView, AsyncProcessStatus asyncProcessStatus) throws Exception {
        log.info("Start of validateAndCreateAccounts in service ###");
        updateAsyncProcessStatus(asyncProcessStatus.getAsyncProcessId(), "Processing");
        String query = "SELECT * FROM ("
                + "SELECT DISTINCT " + customRestApiReqPo.getCcidColumnName() + ", "
                + customRestApiReqPo.getLedgerColumnName() + ", ROWNUM rnum FROM ("
                + "SELECT DISTINCT " + customRestApiReqPo.getCcidColumnName() + ", "
                + customRestApiReqPo.getLedgerColumnName() + " FROM " + stagingTableName
                + " WHERE CR_BATCH_NAME = ? ORDER BY " + customRestApiReqPo.getCcidColumnName()
                + ") WHERE ROWNUM <= ?"
                + ") WHERE rnum >= ?";

//        log.info("sql>>>>>" + sql);
        try (Connection con = dynamicDataSourceBasedMultiTenantConnectionProvider.getConnection(String.valueOf(customRestApiReqPo.getPodId()));
             PreparedStatement preStmnt = con.prepareStatement(query)) {

            preStmnt.setString(1, customRestApiReqPo.getBatchName());
            preStmnt.setLong(2, limit);
            preStmnt.setLong(3, offset);

            try (ResultSet rs = preStmnt.executeQuery()) {
                AccountCombinationServiceStub stub = createStub(customRestApiReqPo.getCloudUrl(),
                        customRestApiReqPo.getCldUserName(),
                        customRestApiReqPo.getCldPassword());
                AccountCombinationServiceStub.ValidateAndCreateAccounts validateCreateAccounts = new AccountCombinationServiceStub.ValidateAndCreateAccounts();

                String delimiter = "";
                int count = 0;
                String[] segmentValuesArr = null;

                while (rs.next()) {
                    count++;
                    String targetCcid = rs.getString(customRestApiReqPo.getCcidColumnName());

                    if (count == 1) {
                        delimiter = utils.findDelimeter(targetCcid);
                    }

                    segmentValuesArr = splitCcid(targetCcid, delimiter);
                    AccountCombinationServiceStub.AccountValidationInput accountValidationInput = createAccountValidationInput(segmentValuesArr, rs.getString(customRestApiReqPo.getLedgerColumnName()));
//                    log.info(accountValidationInput.toString());
                    validateCreateAccounts.addValidationInputRowList(accountValidationInput);
                }

                if (validateCreateAccounts.getValidationInputRowList() == null || validateCreateAccounts.getValidationInputRowList().length == 0) {
                    throw new ValidationException("No Target CCID's Found for this particular Batch");
                }
                log.info("connecting to cloud");
                AccountCombinationServiceStub.ValidateAndCreateAccountsResponse response = stub.validateAndCreateAccounts(validateCreateAccounts);
                log.info("getting data from cloud");
                List<CrValidateCvrCcid> crValidateCvrCcidList = processValidationOutputs(response.getResult(), segmentValuesArr.length, delimiter, customRestApiReqPo, crCloudTemplateHeadersView);
                log.info("start saving into CR_VALIDATE_CVR_CCID");
                validateCvrCcidRepository.saveAll(crValidateCvrCcidList);
                log.info("end saving into CR_VALIDATE_CVR_CCID");

            }
        }

        return asyncProcessStatus;
    }


    private AccountCombinationServiceStub createStub(String cloudUrl, String username, String password) throws Exception {
        AccountCombinationServiceStub stub = new AccountCombinationServiceStub(cloudUrl + "/fscmService/AccountCombinationService");
        ServiceClient client = stub._getServiceClient();
        HttpTransportPropertiesImpl.Authenticator auth = new HttpTransportPropertiesImpl.Authenticator();
        auth.setUsername(username);
        auth.setPassword(password);
        client.getOptions().setProperty(HTTPConstants.AUTHENTICATE, auth);
        client.getOptions().setProperty(HTTPConstants.CHUNKED, false);
        client.getOptions().setProperty(HTTPConstants.SO_TIMEOUT, 600000);
        client.getOptions().setProperty(HTTPConstants.CONNECTION_TIMEOUT, 600000);
        return stub;
    }

    private String[] splitCcid(String targetCcid, String delimiter) {
        if (".".equalsIgnoreCase(delimiter)) {
            return targetCcid.split("\\.");
        } else {
            return targetCcid.split(delimiter);
        }
    }

    private AccountCombinationServiceStub.AccountValidationInput createAccountValidationInput(String[] segmentValuesArr, String ledgerName) throws Exception {
        AccountCombinationServiceStub.AccountValidationInput accountValidationInput = new AccountCombinationServiceStub.AccountValidationInput();
        for (int i = 0; i < segmentValuesArr.length; i++) {
            Method m = getMethod(i + 1);
            m.invoke(accountValidationInput, segmentValuesArr[i]);
        }
        accountValidationInput.setLedgerName(ledgerName);
        accountValidationInput.setEnabledFlag(true);
        return accountValidationInput;
    }

    private Method getMethod(int index) throws NoSuchMethodException {
        return AccountCombinationServiceStub.AccountValidationInput.class.getMethod("setSegment" + index, String.class);
    }

    private List<CrValidateCvrCcid> processValidationOutputs(AccountCombinationServiceStub.AccountValidationOutput[] accountValidationOutArr,
                                                             int length, String delimiter, CustomRestApiReqPo customRestApiReqPo,
                                                             CrCloudTemplateHeadersView crCloudTemplateHeadersView) throws Exception {
        List<CrValidateCvrCcid> crValidateCvrCcidList = new ArrayList<>();
        for (AccountCombinationServiceStub.AccountValidationOutput accountValidationOutput : accountValidationOutArr) {
            String targetCcid = buildTargetCcid(accountValidationOutput, length, delimiter);
            CrValidateCvrCcid validateCvrCcid = createCrValidateCvrCcid(accountValidationOutput, targetCcid, customRestApiReqPo, crCloudTemplateHeadersView);
            crValidateCvrCcidList.add(validateCvrCcid);
        }
        return crValidateCvrCcidList;
    }

    private String buildTargetCcid(AccountCombinationServiceStub.AccountValidationOutput accountValidationOutput, int length, String delimiter) throws Exception {
        StringBuilder targetCcidBuilder = new StringBuilder();
        for (int j = 0; j < length; j++) {
            Method m = getMethod1(j + 1);
            Object segment = m.invoke(accountValidationOutput);
            if (segment != null) {
                targetCcidBuilder.append(segment.toString());
            }
            if (j < length - 1) {
                targetCcidBuilder.append(delimiter);
            }
        }
        return targetCcidBuilder.toString();
    }

    private Method getMethod1(int index) throws NoSuchMethodException {
        return AccountCombinationServiceStub.AccountValidationOutput.class.getMethod("getSegment" + index);
    }

    private CrValidateCvrCcid createCrValidateCvrCcid(AccountCombinationServiceStub.AccountValidationOutput accountValidationOutput, String targetCcid,
                                                      CustomRestApiReqPo customRestApiReqPo, CrCloudTemplateHeadersView crCloudTemplateHeadersView) {
        CrValidateCvrCcid validateCvrCcid = new CrValidateCvrCcid();
        validateCvrCcid.setCcid(targetCcid);
        validateCvrCcid.setCloudStagingTableName(crCloudTemplateHeadersView.getStagingTableName());
        validateCvrCcid.setCrBatchName(customRestApiReqPo.getBatchName());
        validateCvrCcid.setErrorMessage(accountValidationOutput.getError());
        validateCvrCcid.setErrorCode(accountValidationOutput.getErrorCode());
        validateCvrCcid.setStatus(accountValidationOutput.getStatus());
        validateCvrCcid.setRequestId(customRestApiReqPo.getRequestId());
        validateCvrCcid.setCreationDate(new java.sql.Date(new Date().getTime()));
        validateCvrCcid.setCreatedBy("ConvertRite");
        validateCvrCcid.setLastUpdatedDate(new java.sql.Date(new Date().getTime()));
        validateCvrCcid.setLastUpdateBy("ConvertRite");
        return validateCvrCcid;
    }

    private void updateAsyncProcessStatus(Long asyncProcessId, String status) {
        log.info("updating Async process status");
        Optional<AsyncProcessStatus> processStatusOptional = asyncProcessStatusRepository.findById(asyncProcessId);
        processStatusOptional.ifPresent(statusRecord -> {
            statusRecord.setAsyncProcessStatus(status);
            statusRecord.setUpdatedDate(new Date());
            statusRecord.setAsyncEndTime(new Date());
            asyncProcessStatusRepository.save(statusRecord);
        });
    }

}
