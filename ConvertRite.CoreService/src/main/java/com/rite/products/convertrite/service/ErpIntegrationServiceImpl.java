package com.rite.products.convertrite.service;

import java.io.File;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.servlet.http.HttpServletResponse;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rite.products.convertrite.exception.BadRequestException;
import com.rite.products.convertrite.model.*;
import com.rite.products.convertrite.multitenancy.model.Pod;
import com.rite.products.convertrite.multitenancy.repository.PodRepository;
import com.rite.products.convertrite.po.*;
import com.rite.products.convertrite.respository.*;
import org.apache.axiom.attachments.ByteArrayDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.http.*;
import org.springframework.stereotype.Service;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.Session;
import com.opencsv.CSVWriter;
import com.rite.products.convertrite.Validations.Validations;
import com.rite.products.convertrite.exception.ValidationException;
import com.rite.products.convertrite.stubs.erp.ErpIntegrationServiceStub;
import com.rite.products.convertrite.stubs.erp.ErpIntegrationServiceStub.Base64BinaryDataHandler;
import com.rite.products.convertrite.stubs.erp.ErpIntegrationServiceStub.DocumentDetails;
import com.rite.products.convertrite.stubs.erp.ErpIntegrationServiceStub.DownloadExportOutput;
import com.rite.products.convertrite.stubs.erp.ErpIntegrationServiceStub.DownloadExportOutputResponse;
import com.rite.products.convertrite.stubs.erp.ErpIntegrationServiceStub.EssJob;
import com.rite.products.convertrite.stubs.erp.ErpIntegrationServiceStub.ExportBulkData;
import com.rite.products.convertrite.stubs.erp.ErpIntegrationServiceStub.ExportBulkDataResponse;
import com.rite.products.convertrite.stubs.erp.ErpIntegrationServiceStub.GetESSExecutionDetails;
import com.rite.products.convertrite.stubs.erp.ErpIntegrationServiceStub.GetESSExecutionDetailsResponse;
import com.rite.products.convertrite.stubs.erp.ErpIntegrationServiceStub.LoadAndImportData;
import com.rite.products.convertrite.stubs.erp.ErpIntegrationServiceStub.LoadAndImportDataResponse;
import com.rite.products.convertrite.stubs.erp.ErpIntegrationServiceStub.UploadFileToUcm;
import com.rite.products.convertrite.stubs.erp.ErpIntegrationServiceStub.UploadFileToUcmResponse;
import com.rite.products.convertrite.utils.DataSourceUtil;
import com.rite.products.convertrite.utils.Utils;
import com.rite.products.convertrite.utils.Constants;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.RestTemplate;

@Service
public class ErpIntegrationServiceImpl implements ErpIntegrationService {

    private static final Logger log = LoggerFactory.getLogger(ErpIntegrationServiceImpl.class);
    @Value("${convertrite-admin-host}")
    String ConvertriteAdminHost;
    @Value("${covertrite-core-custom-restapi}")
    private String customRestApi;
    @Value("${context-path}")
    private String path;
    @Value("${erp_url}")
    private String erpurl;
    @Autowired
    CrProjectsObjectsRepo crProjectsObjectsRepo;

    @Autowired
    private PodRepository podRepository;

    @Autowired
    DataSourceUtil dataSourceUtil;
    @Autowired
    Utils util;
    @Autowired
    XxrErpIntegrationRepository xxrErpIntegrationRepository;
    @Autowired
    XxrObjectCodeGroupingLinesRepository xxrObjectCodeGroupingLinesRepository;
    @Autowired
    CrObjectGroupLinesRepo crObjectGroupLinesRepo;
    @Autowired
    XxrTemplateRelationRepository xxrTemplateRelationRepository;
    @Autowired
    XxrLookUpValuesRepository xxrLookUpValuesRepository;
    @Autowired
    CrCloudJobStatusRepo crCloudJobStatusRepo;
    @Autowired
    XxrErpIntegrationMetaDataRepository xxrErpIntegrationMetaDataRepository;
    @Autowired
    XxrCldTempHdrsViewRepository xxrCldTempHdrsViewRepository;
    @Autowired
    CrCloudTemplateHeadersViewRepository crCloudTemplateHeadersViewRepository;
    @Autowired
    CloudTemplateHeaderDaoImpl cloudTemplateHeaderDaoImpl;
    @Autowired
    CrTemplateRelationRepository crTemplateRelationRepository;
    @Autowired
    Utils utils;
    @Value("${file.upload-dir}")
    private String fileUploadDir;
    @Value("${import-callback-url}")
    private String callBackUrl;

    @Override
    public LoadandImportDataResPo loadAndImportData(LoadandImportDataReqPo loadandImportDataReqPo)
            throws ValidationException, Exception {
        log.info("Start of  loadAndImportData Method#####");

        Path target = null;
        List<XxrCloudTemplateHeader> cloudTemplateHeaderList = new ArrayList<>();
        Long metaDataTableId = null;
        String metaDataTableName = "";
        Long cloudTemplateId = null;
        // String clobString = "";
        Long objectId = null;
        Long parentObjectId = null;
        Long groupId = null;
        LoadandImportDataResPo loadImportDataRes = new LoadandImportDataResPo();
        String csvName = "";
        Long resultId = null;
        try {
            XxrErpIntegration xxrErpIntegration = xxrErpIntegrationRepository.findByXxrBatchNameAndCloudTemplateId(
                    loadandImportDataReqPo.getBatchName(), loadandImportDataReqPo.getCloudTemplateId());
            if (xxrErpIntegration != null)
                throw new ValidationException("Load Import already executed for this batchName");
            cloudTemplateId = loadandImportDataReqPo.getCloudTemplateId();
            XxrCldTempHdrsView xxrCldTempHdrsView = xxrCldTempHdrsViewRepository.findByTemplateId(cloudTemplateId);
            // XxrCloudTemplateHeader xxrCloudTemplateHeader =
            // cloudTemplateHeaderList.get(0);
            objectId = xxrCldTempHdrsView.getObjectId();
            parentObjectId = xxrCldTempHdrsView.getParentObjectId();
            String objecCode = xxrCldTempHdrsView.getObjectCode();
            String parentObjectCode = xxrCldTempHdrsView.getParentObjectCode();
            csvName = util.getCtlFileName(objecCode);
            if (xxrCldTempHdrsView != null)
                metaDataTableId = xxrCldTempHdrsView.getMetaDataTableId();
            if (metaDataTableId != null) {
                metaDataTableName = xxrCldTempHdrsView.getMetaDataTableName();
            }
            // Create csv from clob in temp directory
            target = Files.createTempDirectory(null);
            log.debug("target:::::" + target);
            groupId = xxrObjectCodeGroupingLinesRepository.getGroupIdbyObjectId(objectId);
            XxrTemplateRelation xxrTemplateRelation = null;
            String zipfilePath = "";
            if (groupId != null) {
                // create csv files and zip them
                zipfilePath = target + File.separator + util.getCtlFileName(objecCode) + ".zip";
                xxrTemplateRelation = creatingCsvFilesAndZip(loadandImportDataReqPo, target, groupId);
            } else {
                writingToCsv(cloudTemplateId, target.toString(), csvName);
                zipfilePath = target + File.separator + metaDataTableName + ".zip";
                // zip csv file
                Utils.zipFile(target.toString(), metaDataTableName, csvName + ".csv");
            }
            // String url =
            // "https://ucf3-ztzb-fa-ext.oracledemos.com:443/fscmService/ErpIntegrationService";
            XxrErpIntegrationMetaData erpMetaData = xxrErpIntegrationMetaDataRepository
                    .findByParentObjectIdAndObjectId(parentObjectId, objectId);
            if (erpMetaData == null)
                throw new Exception(
                        "There is no configuration for LoadImport MetaData for these objectId & parentObjectId");

            String soapUrl = util.getCloudUrl(xxrCldTempHdrsView) + erpurl;
            ErpIntegrationServiceStub erpIntegrationServiceStub = new ErpIntegrationServiceStub(soapUrl);
            // BasicAuthentication for soap service
            util.basicAuthentication(erpIntegrationServiceStub._getServiceClient(), xxrCldTempHdrsView);

            // soapservice call for loadAndImportData
//			resultId = loadAndImportService(erpIntegrationServiceStub, loadandImportDataReqPo, zipfilePath,
//					erpMetaData);

            /*
             * if ("Customer".equalsIgnoreCase(parentObjectCode)) { //Independent soap
             * service call for uploadFileToUcm, ExportBulkData Application Tables resultId
             * = loadAndImportIndependentService(erpIntegrationServiceStub,
             * loadandImportDataReqPo, zipfilePath,erpMetaData); } else { // soapservice
             * call for loadAndImportData resultId =
             * loadAndImportService(erpIntegrationServiceStub, loadandImportDataReqPo,
             * zipfilePath,erpMetaData); }
             */
            // save details into ERPINEGRATION table
            saveErpIntegrationDetails(loadandImportDataReqPo, resultId, erpMetaData);

            /*
             * if (xxrTemplateRelation != null) { xxrTemplateRelation.setIsZipped("Y");
             * xxrTemplateRelationRepository.save(xxrTemplateRelation); }
             */
            loadImportDataRes.setResultId(resultId);
            loadImportDataRes.setMessage("Sucessfully load and import submitted");

        } catch (ValidationException e) {
            // e.printStackTrace();
            log.error(e.getMessage());
            throw new ValidationException(e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            log.error(e.getMessage());
            throw new Exception(e.getMessage());
        }
        return loadImportDataRes;
    }

    private XxrTemplateRelation creatingCsvFilesAndZip(LoadandImportDataReqPo loadandImportDataReqPo, Path target,
                                                       Long groupId) throws Exception {
        String clobString = "";
        Long objectId = null;
        XxrTemplateRelation xxrTemplateRelation = null;
        try {
            Predicate<XxrTemplateRelation> filterTemplateGrouping = x -> {
                String[] strArr = x.getTemplateIds().split(",");
                Long cldTemplateId = loadandImportDataReqPo.getCloudTemplateId();
                boolean flag = false;
                for (String s : strArr) {
                    long templateId = Long.parseLong(s);
                    if (templateId == cldTemplateId) {
                        flag = true;
                        break;
                    }
                }
                return flag;
            };
            List<XxrTemplateRelation> xxrTemplateRelationLi = xxrTemplateRelationRepository
                    .getTemplatRelations(groupId);
            if (xxrTemplateRelationLi.stream().filter(filterTemplateGrouping).count() <= 0)
                throw new ValidationException(
                        "This Template is not part of template grouping,Please add into template grouping");
            xxrTemplateRelation = xxrTemplateRelationLi.stream().filter(filterTemplateGrouping).findFirst().get();
            if (xxrTemplateRelation != null) {
                String templateArr[] = xxrTemplateRelation.getTemplateIds().split(",");
                List<String> listOfCsvContents = new ArrayList<>();
                List<String> fileNames = new ArrayList<>();
                for (String tempId : templateArr) {
                    long templateId = Long.parseLong(tempId);
                    XxrCldTempHdrsView xxrCldTempHdrsView = xxrCldTempHdrsViewRepository.findByTemplateId(templateId);
                    // XxrCloudTemplateHeader cloudTemplateHeader = cloudTemplateHeaderLi.get(0);
                    objectId = xxrCldTempHdrsView.getObjectId();
                    String objectCode = xxrCldTempHdrsView.getObjectCode();
                    // util.getFileName(objectCode);
                    String sql = "select xxr_conversion_utils_pkg.fbdi_filegen(" + templateId + ") from dual";
                    clobString = util.getClobString(sql);
                    // System.out.println(clobString+"clobString##########");
                    listOfCsvContents.add(clobString);
                    // fileNames.add(util.getFileName(objectCode));
                    fileNames.add(util.getCtlFileName(objectCode));
                }
                //   Utils.zipMultipleFiles(listOfCsvContents, target.toString(), fileNames, map);
            }
        } catch (Exception e) {
            throw new Exception(e.getMessage());
        }
        return xxrTemplateRelation;
    }

    private ZipCsvFilesResPo creatingCsvFilesAndZipV1(LoadandImportDataReqPo loadandImportDataReqPo, Path target,
                                                      Long groupId, ChannelSftp channelSftp) throws ValidationException, Exception {
        String clobString = "";
        Long objectId = null;
        XxrTemplateRelation xxrTemplateRelation = null;
        String zipPath = null;
        ZipCsvFilesResPo zipCsvFilesResPo = new ZipCsvFilesResPo();
        try {
            Predicate<XxrTemplateRelation> filterTemplateGrouping = x -> {
                String[] strArr = x.getTemplateIds().split(",");
                Long cldTemplateId = loadandImportDataReqPo.getCloudTemplateId();
                boolean flag = false;
                for (String s : strArr) {
                    long templateId = Long.parseLong(s);
                    if (templateId == cldTemplateId) {
                        flag = true;
                        break;
                    }
                }
                return flag;
            };
            List<XxrTemplateRelation> xxrTemplateRelationLi = xxrTemplateRelationRepository
                    .getTemplatRelations(groupId);
            if (xxrTemplateRelationLi.stream().filter(filterTemplateGrouping).count() <= 0)
                throw new ValidationException(
                        "This Template is not part of template grouping,Please add into template grouping");
            xxrTemplateRelation = xxrTemplateRelationLi.stream().filter(filterTemplateGrouping).findFirst().get();
            if (xxrTemplateRelation != null) {
                String templateArr[] = xxrTemplateRelation.getTemplateIds().split(",");
                // checking wether loadimport is executed for one of template in grouping
                Long[] arr = new Long[templateArr.length];
                for (int i = 0; i < templateArr.length; i++) {
                    arr[i] = Long.parseLong(templateArr[i]);
                }
                List<Long> listofTemplates = Arrays.asList(arr);
                XxrErpIntegration xxrErpIntegration = xxrErpIntegrationRepository
                        .findByXxrBatchNameAndCloudTemplateIdIn(loadandImportDataReqPo.getBatchName(), listofTemplates);
                if (xxrErpIntegration != null)
                    throw new ValidationException("FBDI Template Grouping applied");

                List<String> listOfCsvContents = new ArrayList<>();
                List<String> fileNames = new ArrayList<>();
                for (String tempId : templateArr) {
                    long templateId = Long.parseLong(tempId);
                    //XxrCldTempHdrsView xxrCldTempHdrsView = xxrCldTempHdrsViewRepository.findByTemplateId(templateId);
                    CrCloudTemplateHeadersView crCloudTemplateHeadersView = crCloudTemplateHeadersViewRepository.findById(templateId).get();
                    objectId = crCloudTemplateHeadersView.getObjectId();
                    String objectCode = crCloudTemplateHeadersView.getObjectCode();
                    String stagingTableName = crCloudTemplateHeadersView.getStagingTableName();
                    // util.getFileName(objectCode);

//					clobString = cloudTemplateHeaderDaoImpl.downloadFbdiImport(templateId,
//							loadandImportDataReqPo.getBatchName(),
//							stagingTableName + "_" + loadandImportDataReqPo.getBatchName().toUpperCase() + ".csv", channelSftp);
                    clobString = cloudTemplateHeaderDaoImpl.downloadFbdiData(templateId, loadandImportDataReqPo.getBatchName());
                    // System.out.println(clobString+"clobString##########");
                    listOfCsvContents.add(clobString);
                    // fileNames.add(util.getFileName(objectCode));
                    fileNames.add(util.getCtlFileName(objectCode));
                }
                //    zipPath = Utils.zipMultipleFiles(listOfCsvContents, target.toString(), fileNames, map);
            }
        } catch (ValidationException e) {
            throw new ValidationException(e.getMessage());
        } catch (Exception e) {
            throw new Exception(e.getMessage());
        }
        zipCsvFilesResPo.setXxrTemplateRelation(xxrTemplateRelation);
        zipCsvFilesResPo.setZipFilePath(zipPath);
        return zipCsvFilesResPo;
    }

    private void writingToCsv(Long cloudTemplateId, String target, String csvName) throws Exception {
        String clobString = "";
        String sql = "select xxr_conversion_utils_pkg.fbdi_filegen(" + cloudTemplateId + ") from dual";
        clobString = util.getClobString(sql);
        String filePath = target + File.separator + csvName + ".csv";
        File file = new File(filePath);
        FileWriter outputfile = new FileWriter(file);
        // create CSVWriter with ',' as separator
        CSVWriter writerObj = new CSVWriter(outputfile, CSVWriter.DEFAULT_SEPARATOR, CSVWriter.NO_QUOTE_CHARACTER,
                CSVWriter.NO_ESCAPE_CHARACTER, CSVWriter.DEFAULT_LINE_END);
        if (!Validations.isNullOrEmpty(clobString))
            writerObj.writeNext(clobString.split(","));
        writerObj.flush();
        writerObj.close();
    }

    private void writingToCsvV1(Long cloudTemplateId, String target, String csvName,
                                LoadandImportDataReqPo loadandImportDataReqPo, CrCloudTemplateHeadersView xxrCldTempHdrsView,
                                ChannelSftp channelSftp) throws Exception {
        String clobString = "";
        HttpServletResponse response = null;
        clobString = cloudTemplateHeaderDaoImpl.downloadFbdiData(cloudTemplateId, loadandImportDataReqPo.getBatchName());

        String filePath = target + File.separator + csvName + ".csv";
        File file = new File(filePath);
        FileWriter outputfile = new FileWriter(file);
        // create CSVWriter with ',' as separator
        CSVWriter writerObj = new CSVWriter(outputfile, CSVWriter.DEFAULT_SEPARATOR, CSVWriter.NO_QUOTE_CHARACTER,
                CSVWriter.NO_ESCAPE_CHARACTER, CSVWriter.DEFAULT_LINE_END);
        if (!Validations.isNullOrEmpty(clobString))
            writerObj.writeNext(clobString.split(","));
        writerObj.flush();
        writerObj.close();
    }

    private Long loadAndImportIndependentService(ErpIntegrationServiceStub erpIntegrationServiceStub,
                                                 LoadandImportDataReqPo loadImportDataReq, String zipfilePath, XxrErpIntegrationMetaData erpMetaData)
            throws Exception {
        log.info("Start of loadAndImportService Method######");
        Long result = null;
        try {
            DataSource source = new ByteArrayDataSource(Files.readAllBytes(Paths.get(zipfilePath)),
                    "text/plain;charset=UTF-8");
            DataHandler dataHandler = new DataHandler(source);
            Base64BinaryDataHandler base64BinaryDataHandler = new Base64BinaryDataHandler();
            base64BinaryDataHandler.setBase64BinaryDataHandler(dataHandler);

            // Creating DocumentDetails
            DocumentDetails documentDetails = new DocumentDetails();
            documentDetails.setContentType("zip");
            documentDetails.setContent(base64BinaryDataHandler);
            documentDetails.setDocumentAccount(erpMetaData.getDocumentAccount());
            documentDetails.setDocumentAuthor("ConvertRite");
            documentDetails.setDocumentSecurityGroup(erpMetaData.getDocumentSecurityGroup());
            String objecCode = xxrLookUpValuesRepository.getValueById(erpMetaData.getObjectId());
            documentDetails.setDocumentTitle(objecCode + ".zip");
            documentDetails.setFileName(objecCode + ".zip");

            UploadFileToUcm uploadFileToUcm0 = new UploadFileToUcm();
            uploadFileToUcm0.setDocument(documentDetails);
            // upload File to UCM service call
            UploadFileToUcmResponse uploadFileRes = erpIntegrationServiceStub.uploadFileToUcm(uploadFileToUcm0);
            String documentId = uploadFileRes.getResult();
            String parameterList = loadImportDataReq.getParameterList();
            if (parameterList.contains("{0}")) {
                parameterList = parameterList.replace("{0}", documentId);
//				log.info(parameterList);
            }
            ExportBulkData exportBulkData = new ExportBulkData();
            exportBulkData.setJobName(erpMetaData.getJobName());
            exportBulkData.setCallbackURL("#NULL");
            exportBulkData.setParameterList(parameterList);
            exportBulkData.setNotificationCode("30");
            ExportBulkDataResponse exportBulkRes = erpIntegrationServiceStub.exportBulkData(exportBulkData);
            result = exportBulkRes.getResult();

        } catch (Exception e) {
            // e.printStackTrace();
            throw new Exception(e.getMessage());
        }
        return result;
    }

    private Long loadAndImportService(ErpIntegrationServiceStub erpIntegrationServiceStub,
                                      LoadandImportDataReqPo loadImportDataReq, String zipfilePath, XxrErpIntegrationMetaData erpMetaData, ObjectInfoWithPodClodConfigPo objectData, Map<String, String> objectInfos
    )
            throws Exception {
        log.info("Start of loadAndImportService Method######");
        Long result = null;
        try {

            DataSource source = new ByteArrayDataSource(Files.readAllBytes(Paths.get(zipfilePath)),
                    "text/plain;charset=UTF-8");
            DataHandler dataHandler = new DataHandler(source);
            Base64BinaryDataHandler base64BinaryDataHandler = new Base64BinaryDataHandler();
            base64BinaryDataHandler.setBase64BinaryDataHandler(dataHandler);

            // Creating DocumentDetails
            DocumentDetails documentDetails = new DocumentDetails();
            documentDetails.setContentType("zip");
            documentDetails.setContent(base64BinaryDataHandler);

            LoadAndImportData loadAndImportData = new LoadAndImportData();
            EssJob[] essJob = new EssJob[1];
            EssJob job = new EssJob();
            log.info("Object Name:-->" + loadImportDataReq.getObjectName());
            job.setParameterList(objectInfos.get("ParameterList"));
            documentDetails.setDocumentAccount(objectInfos.get("DocumentAccount"));
            documentDetails.setDocumentSecurityGroup(objectInfos.get("DocumentSecurityGroup"));
            job.setJobName(objectInfos.get("JobName"));
            loadAndImportData.setInterfaceDetails(objectInfos.get("InterfaceDetails"));
            documentDetails.setDocumentAuthor("ConvertRite");
            documentDetails.setDocumentTitle(objectData.getObjectCode() + ".zip");
            documentDetails.setFileName(objectData.getObjectCode() + ".zip");

            // creating job details
            essJob[0] = job;
            loadAndImportData.setJobList(essJob);
            loadAndImportData.setDocument(documentDetails);
            Pod pod = podRepository.findByPodId(loadImportDataReq.getPodId());
            if ("N".equalsIgnoreCase(pod.getScheduledJobFlag())) {
                log.info("callback url " + callBackUrl + "?tenantId=" + loadImportDataReq.getPodId());
                loadAndImportData.setCallbackURL(callBackUrl + "?tenantId=" + loadImportDataReq.getPodId());
            }
            LoadAndImportDataResponse resp = erpIntegrationServiceStub.loadAndImportData(loadAndImportData);

            result = resp.getResult();
            log.info("result-->" + result);
        } catch (Exception e) {
            throw new Exception(e.getMessage());
        }
        return result;
    }

    private void saveErpIntegrationDetails(LoadandImportDataReqPo loadImportDataReq, Long result,
                                           XxrErpIntegrationMetaData erpMetaData) throws Exception {
        XxrErpIntegration erpIntegration = new XxrErpIntegration();
        try {
            erpIntegration.setContentType("zip");
            erpIntegration.setCloudTemplateId(loadImportDataReq.getCloudTemplateId());
            erpIntegration.setDocumentAccount(erpMetaData.getDocumentAccount());
            erpIntegration.setDocumentAuthor("ConvertRite");
            erpIntegration.setDocumentSecurityGroup(erpMetaData.getDocumentSecurityGroup());
            String objecCode = xxrLookUpValuesRepository.getValueById(erpMetaData.getObjectId());
            erpIntegration.setDocumentTitle(objecCode + ".zip");
            erpIntegration.setFileName(objecCode + ".zip");
            erpIntegration.setInterfaceDetails(erpMetaData.getInterfaceDetails());
            erpIntegration.setJobName(erpMetaData.getJobName());
            erpIntegration.setParameterList(loadImportDataReq.getParameterList());
            erpIntegration.setResult(result);
            erpIntegration.setXxrBatchName(loadImportDataReq.getBatchName());
            erpIntegration.setCreationDate(new java.sql.Date(new java.util.Date().getTime()));
            erpIntegration.setCreatedBy("ConvertRite");
            erpIntegration.setLastUpdatedDate(new java.sql.Date(new java.util.Date().getTime()));
            erpIntegration.setLastUpdateBy("ConvertRite");
            xxrErpIntegrationRepository.save(erpIntegration);
        } catch (Exception e) {
            // e.printStackTrace();
            throw new Exception(e.getMessage());
        }
    }

    @Override
    public LoadImportJobStatusResPo getJobStatus(Long resultId, Long cldTemplateId) throws Exception {
        log.info("Start of getJobStatus Method######");
        LoadImportJobStatusResPo loadImportJobStatusRes = new LoadImportJobStatusResPo();
        try {
            // String url =
            // "https://ucf3-ztzb-fa-ext.oracledemos.com:443/fscmService/ErpIntegrationService";
            XxrCldTempHdrsView xxrCldTempHdrsView = xxrCldTempHdrsViewRepository.findByTemplateId(cldTemplateId);
            String soapUrl = util.getCloudUrl(xxrCldTempHdrsView) + erpurl;
            ErpIntegrationServiceStub erpIntegrationServiceStub = new ErpIntegrationServiceStub(soapUrl);

            // BasicAuthentication for soap service
            util.basicAuthentication(erpIntegrationServiceStub._getServiceClient(), xxrCldTempHdrsView);
            // Execution details
            GetESSExecutionDetails executionDetails = new GetESSExecutionDetails();
            executionDetails.setRequestId(resultId);

            GetESSExecutionDetailsResponse response = erpIntegrationServiceStub
                    .getESSExecutionDetails(executionDetails);
            loadImportJobStatusRes.setResult(response.getResult());
            loadImportJobStatusRes.setMessage("Retrieved Job Status successfully");
        } catch (Exception e) {
            throw new Exception(e.getMessage());
        }
        return loadImportJobStatusRes;
    }

    @Override
    public List<XxrErpIntegration> getErpIntegrationDetails(ErpIntegrationPageReqPo erpIntegrationPageReqPo,
                                                            HttpHeaders httpHeaders) throws Exception {
        log.info("Start of getErpIntegrationDetails Method#####");
        List<XxrErpIntegration> erpIntegrationLi = new ArrayList<>();
        try {

            Pageable pageable = PageRequest.of(erpIntegrationPageReqPo.getPageNo(),
                    erpIntegrationPageReqPo.getPageSize(),
                    Sort.by(erpIntegrationPageReqPo.getSortDirection(), erpIntegrationPageReqPo.getSortBy()));
            Page<XxrErpIntegration> pageContent = xxrErpIntegrationRepository.findAll(pageable);
            httpHeaders.set("pagecount", String.valueOf(pageContent.getTotalPages()));
            httpHeaders.set("totalcount", String.valueOf(pageContent.getTotalElements()));
            if (pageContent.hasContent())
                erpIntegrationLi = pageContent.getContent();

        } catch (Exception e) {
            throw new Exception(e.getMessage());
        }
        return erpIntegrationLi;
    }

    @Override
    public byte[] downloadExportOutput(String requestId, Long cldTemplateId, HttpServletResponse response)
            throws Exception {
        log.info("Start of downloadExportOutput Method#####");
        // ResponseBuilder response = null ;

        response.setContentType("application/zip");
        response.setStatus(HttpServletResponse.SC_OK);
        response.addHeader("Content-Disposition", "attachment; filename=" + requestId + ".zip");
        // Response resp=null;
        // ByteArrayOutputStream baos = new ByteArrayOutputStream();
        byte[] buf = null;
        try {
            DownloadExportOutput downloadExportOutput = new DownloadExportOutput();
            downloadExportOutput.setRequestId(requestId);
            // String url =
            // "https://ucf3-ztzb-fa-ext.oracledemos.com:443/fscmService/ErpIntegrationService";
            XxrCldTempHdrsView xxrCldTempHdrsView = xxrCldTempHdrsViewRepository.findByTemplateId(cldTemplateId);
            String soapUrl = util.getCloudUrl(xxrCldTempHdrsView) + erpurl;
            ErpIntegrationServiceStub erpIntegrationServiceStub = new ErpIntegrationServiceStub(soapUrl);

            // BasicAuthentication for soap service
            util.basicAuthentication(erpIntegrationServiceStub._getServiceClient(), xxrCldTempHdrsView);

            DownloadExportOutputResponse downloadExportOutputResponse = erpIntegrationServiceStub
                    .downloadExportOutput(downloadExportOutput);
            // System.out.println(DownloadExportOutputResponse.MY_QNAME+"##########MY_QNAME");
            // QName qName=new QName("ns0:","Content");

            DocumentDetails[] documentDetails = downloadExportOutputResponse.getResult();

            DocumentDetails details = documentDetails[0];

            DataHandler dataHandler = details.getContent().getBase64BinaryDataHandler();
            log.info("dataHandler::::" + dataHandler);
            buf = dataHandler.getDataSource().getInputStream().readAllBytes();
            /*
             * OMElement omEle= downloadExportOutputResponse.getOMElement(new
             * QName("Content"), org.apache.axiom.om.OMAbstractFactory.getOMFactory());
             * System.out.println(omEle+"###########omEle");
             *
             * System.out.println(omEle.getAttribute(new
             * QName("ns0:Content"))+"contegentTag");
             *
             * OMElement child = (OMElement) omEle.getFirstOMChild();
             * System.out.println(child+"###########child"); OMAttribute attr =
             * child.getAttribute(new QName("href"));
             *
             * //Content ID processing String contentID = attr.getAttributeValue();
             * contentID = contentID.trim(); if (contentID.substring(0,
             * 3).equalsIgnoreCase("cid")) { contentID = contentID.substring(4); }
             *
             * MessageContext msgCtx = MessageContext.getCurrentMessageContext();
             * Attachments attachment = msgCtx.getAttachmentMap(); DataHandler dataHandler =
             * attachment.getDataHandler(contentID);
             */
            /*
             * ZipOutputStream zos = new ZipOutputStream(baos); ZipEntry entry = new
             * ZipEntry(requestId+".zip"); entry.setSize(buf.length);
             * zos.putNextEntry(entry); zos.write(buf); zos.closeEntry(); zos.close();
             */
            /*
             * java.io.FileOutputStream out_zip_file = new
             * java.io.FileOutputStream(requestId+".zip");
             * out_zip_file.write(baos.toByteArray()); response= Response.ok((Object)
             * out_zip_file); response.header("Content-Disposition",
             * "attachment; filename="+requestId+".zip"); //
             * response.setContentType("application/zip"); resp=response.build();
             * out_zip_file.flush(); out_zip_file.close();
             */

        } catch (Exception e) {
            // e.printStackTrace();
            throw new Exception(e.getMessage());
        }

        return buf;
    }

    @Override
    public LoadandImportDataResPo loadAndImportDataV1(LoadandImportDataReqPo loadandImportDataReqPo, String bearerToken)
            throws Exception, ValidationException {
        log.info("Start of loadAndImportDataV1 #######");
        Path target = null;
        List<XxrCloudTemplateHeader> cloudTemplateHeaderList = new ArrayList<>();
        Long metaDataTableId = null;
        String metaDataTableName = "";
        Long cloudTemplateId = null;
        // String clobString = "";
        Long objectId = null;
        Long parentObjectId = null;
        Long groupId = null;
        LoadandImportDataResPo loadImportDataRes = new LoadandImportDataResPo();
        String csvName = "";
        Long resultId = null;
        String zipfilePath = "";
        Session jschSession = null;
        ChannelSftp channelSftp = null;

        try {
            ObjectMapper mapper = new ObjectMapper();
            List<ObjectInfoWithPodClodConfigPo> objectsInfoDetailsList = null;
            List<CloudLoginDetails> cloudLoginDetailsList = null;
            List<ObjectInfoWithPodClodConfigPo> newJsonNode = null;
            cloudTemplateId = loadandImportDataReqPo.getCloudTemplateId();
            CrCloudTemplateHeadersView crCldTempHdrsView = crCloudTemplateHeadersViewRepository.findById(cloudTemplateId).get();
            String objectName = loadandImportDataReqPo.getObjectName();
            if (objectName.toLowerCase().equals("all")) {
                objectName = crProjectsObjectsRepo.getAllByCloudTemplateId(loadandImportDataReqPo.getCloudTemplateId()).get(0).objectName;
            }

            JsonNode node = getObjectsWithInformation(loadandImportDataReqPo, objectName, bearerToken);
            JsonNode objectsDetailsNode = node.get("objectsDetails");
            JsonNode cloudLoginDetailsNode = node.get("cloudLoginDetails");
            newJsonNode = mapper.treeToValue(objectsDetailsNode, List.class);
            if (newJsonNode.size() > 0) {
                objectsInfoDetailsList = mapper.convertValue(objectsDetailsNode, new TypeReference<List<ObjectInfoWithPodClodConfigPo>>() {
                });
                cloudLoginDetailsList = mapper.convertValue(cloudLoginDetailsNode, new TypeReference<List<CloudLoginDetails>>() {
                });
                //System.out.println("URL-->"+objectsInfoDetailsList);
                System.out.println("URL-->" + cloudLoginDetailsList);
            } else {
                System.out.println("else-->" + "No data Found");
                //return  "No data Found";
            }
//			//xxrErpIntegrationRepository -- object information
//			XxrErpIntegration xxrErpIntegration = xxrErpIntegrationRepository.findByXxrBatchNameAndCloudTemplateId(
//					loadandImportDataReqPo.getBatchName(), loadandImportDataReqPo.getCloudTemplateId());
//			if (xxrErpIntegration != null)
//				throw new ValidationException("Already BatchName exists");

            objectId = crCldTempHdrsView.getObjectId();
            parentObjectId = crCldTempHdrsView.getParentObjectId();
            String objecCode = crCldTempHdrsView.getObjectCode();
            String parentObjectCode = crCldTempHdrsView.getParentObjectCode();
            csvName = util.getCtlFileName(objecCode);
            if (crCldTempHdrsView != null)
                metaDataTableId = crCldTempHdrsView.getMetaDataTableId();
            if (metaDataTableId != null) {
                metaDataTableName = crCldTempHdrsView.getMetaDataTableName();
                log.info("metaDataTableName:::::" + metaDataTableName);
            }
            // Create csv from clob in temp directory
            target = Files.createTempDirectory(null);
            log.debug("target:::::" + target);
//			//xxrObjectCodeGroupingLinesRepository --Ignore as of now
            //	groupId = xxrObjectCodeGroupingLinesRepository.getGroupIdbyObjectId(objectId);
            groupId = crObjectGroupLinesRepo.getGroupIdbyObjectId(objectId);
            ZipCsvFilesResPo zipCsvFilesResPo = new ZipCsvFilesResPo();

            // connecting to Ftp location and fetching file
            jschSession = utils.setupJschSession();
            channelSftp = (ChannelSftp) jschSession.openChannel("sftp");
            channelSftp.connect();
            channelSftp.cd(fileUploadDir);
            //Skip multile files zip as of now
            if (groupId != null) {
                // create csv files and zip them
                zipCsvFilesResPo = creatingCsvFilesAndZipV1(loadandImportDataReqPo, target, groupId, channelSftp);
                //zipCsvFilesResPo = creatingCsvFilesAndZipV2(loadandImportDataReqPo, target, groupId, channelSftp);
                zipfilePath = zipCsvFilesResPo.getZipFilePath();
            }
            //do this for single zip file
            else {
                writingToCsvV1(cloudTemplateId, target.toString(), csvName, loadandImportDataReqPo, crCldTempHdrsView,
                        channelSftp);
                zipfilePath = target + File.separator + metaDataTableName + ".zip";
                // zip csv file
                Utils.zipFile(target.toString(), metaDataTableName, csvName + ".csv");
            }
            // String url =
            // "https://ucf3-ztzb-fa-ext.oracledemos.com:443/fscmService/ErpIntegrationService";
            //xxrErpIntegrationMetaDataRepository --CrObjectInformation
            XxrErpIntegrationMetaData erpMetaData = xxrErpIntegrationMetaDataRepository
                    .findByParentObjectIdAndObjectId(parentObjectId, objectId);
            if (erpMetaData == null)
                throw new Exception(
                        "There is no configuration for LoadImport MetaData for these objectId & parentObjectId");
            //String soapUrl = util.getCloudUrl(crCldTempHdrsView) + erpurl;
            String soapUrl = "https://fa-etao-dev20-saasfademo1.ds-fa.oraclepdemos.com/fscmService/ErpIntegrationService";
            ErpIntegrationServiceStub erpIntegrationServiceStub = new ErpIntegrationServiceStub(soapUrl);
            // BasicAuthentication for soap service
            util.basicAuthentication(erpIntegrationServiceStub._getServiceClient(), null);

            // soapservice call for loadAndImportData
//			resultId = loadAndImportService(erpIntegrationServiceStub, loadandImportDataReqPo, zipfilePath,
//					erpMetaData);

            // save details into ERPINEGRATION table
            //Ignore as of now
            saveErpIntegrationDetails(loadandImportDataReqPo, resultId, erpMetaData);

            loadImportDataRes.setResultId(resultId);
            loadImportDataRes.setMessage("Sucessfully load and import submitted");

        } catch (ValidationException e) {
            // e.printStackTrace();
            log.error(e.getMessage());
            throw new ValidationException(e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            log.error(e.getMessage());
            throw new Exception(e.getMessage());
        } finally {
            if (!Validations.isNullOrEmpty(zipfilePath))
                Files.deleteIfExists(Paths.get(zipfilePath));

            if (channelSftp != null) {
                channelSftp.exit();
                channelSftp.disconnect();
            }
            if (jschSession != null)
                jschSession.disconnect();

        }
        return null;
    }

    private JsonNode getObjectsWithInformation(LoadandImportDataReqPo loadandImportDataReqPo, String objectName, String bearerToken) throws Exception {
        HttpHeaders header = new HttpHeaders();
        header.setAccept(Arrays.asList(MediaType.APPLICATION_JSON));
        System.out.println("bearerToken-->" + bearerToken);
        header.set("Authorization", bearerToken);
        HttpEntity<String> entity = new HttpEntity<String>(header);
        RestTemplate restTemplate = new RestTemplate();
        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = null;
        JsonNode name = null;
        try {
            String url = ConvertriteAdminHost + "/api/convertriteadmin/getObjectsWithInformation?podId=" + loadandImportDataReqPo.getPodId() + "&objectName=" + objectName;
            ResponseEntity<String> objects = restTemplate.exchange(url, HttpMethod.GET, entity, String.class);
            log.info("url-->" + url);
            root = mapper.readTree(objects.getBody());
            name = root.path("payload");
            log.info("Responce-->" + name);
        } catch (Exception e) {
            log.error(e.getMessage());
            throw new Exception(e.getMessage());
        }
        return name;
    }


    @Override
    @Transactional(rollbackFor = {Exception.class})
    public LoadandImportDataResPo loadAndImportDataV2(LoadandImportDataReqPo loadandImportDataReqPo, String bearerToken) {
        log.info("Start of loadAndImportDataV2 #######");
        Path target = null;
        Long metaDataTableId = null;
        String metaDataTableName = "";
        Long cloudTemplateId = null;
        Long objectId = null;
        Long parentObjectId = null;
        Long groupId = null;
        String objectIds = null;
        LoadandImportDataResPo loadImportDataRes = new LoadandImportDataResPo();
        String csvName = "";
        Long resultId = null;
        String zipfilePath = "";
        Session jschSession = null;
        ChannelSftp channelSftp = null;
        XxrErpIntegrationMetaData erpMetaData = null;
        String userName = null;
        String password = null;
        String csvPath = "";
        String cldUrl = "";
        List<CrCldTemplateHdrsObjectGroupLinesRes> crCloudTemplateHeadersObjectGroupLines = null;
        CrCloudJobStatus crCloudJobStatus = new CrCloudJobStatus();
        ObjectInfoWithPodClodConfigPo objectInfoWithPodClodConfigPo=null;
        boolean isFirstObjectInGroup=true;
        try {
            CrCloudJobStatus crCldJobStatus = crCloudJobStatusRepo.findByCldTemplateIdAndBatchName(loadandImportDataReqPo.getCloudTemplateId(), loadandImportDataReqPo.getBatchName());
            if (crCldJobStatus != null) {
                loadImportDataRes.setMessage("BatchName already exists");
                log.error("=============BatchName already exists=============={}",loadandImportDataReqPo.getBatchName());
                return loadImportDataRes;
            }

            List<ObjectInfoWithPodClodConfigPo> objectsInfoDetailsList = null;
            List<CloudLoginDetails> cloudLoginDetailsList = null;
            cloudTemplateId = loadandImportDataReqPo.getCloudTemplateId();
            CrCloudTemplateHeadersView crCldTempHdrsView = crCloudTemplateHeadersViewRepository.findById(cloudTemplateId).get();
            objectId = crCldTempHdrsView.getObjectId();
            String objectName = loadandImportDataReqPo.getObjectName();
            if (objectName.toLowerCase().equals("all")) {
                objectName = crProjectsObjectsRepo.getAllByCloudTemplateId(loadandImportDataReqPo.getCloudTemplateId()).get(0).objectName;
            }
            List<Long> cldTemplateIdList = new ArrayList<>();
            if (objectId != null) {
                groupId = crObjectGroupLinesRepo.getGroupIdbyObjectId(objectId);
                log.info("groupId--->" + groupId);
            }
            if (groupId != null) {
                crCloudTemplateHeadersObjectGroupLines = crCloudTemplateHeadersViewRepository.getCldRemplateHdrsbyGroupId(groupId);
                // objectName = "";
                objectIds = "";
                for (CrCldTemplateHdrsObjectGroupLinesRes cldtempObjectGroupLines : crCloudTemplateHeadersObjectGroupLines) {
                    CrCloudTemplateHeadersView cldtempHdrs = cldtempObjectGroupLines.getCloudTemplateHeadersView();
                    if (cldtempHdrs == null || cldtempHdrs.getSourceTemplateId() == null) {
                        String objecName = cldtempObjectGroupLines.getObjectGroupLinesView().getObjectName();
                        loadImportDataRes.setMessage("Missing object " + objecName + " from object grouping");
                        log.info("===Missing object {} from object grouping====" + objecName);
                        return loadImportDataRes;
                    }
                    cldTemplateIdList.add(cldtempHdrs.getTemplateId());
                    objectIds = objectIds + cldtempHdrs.getObjectId() + ", ";
                }
            }
            if(objectIds!=null){
                objectIds = Utils.replaceLastComma(objectIds);
            }else{
                objectIds=objectId.toString();
            }
            CrObjectInformationPo crObjectInformationPo = utils.getObjectsWithInformation(loadandImportDataReqPo.getPodId(), objectIds.trim(), bearerToken);
            Map<String, String> objectInfos = new HashMap<>();
            if (crObjectInformationPo != null) {
                objectsInfoDetailsList = crObjectInformationPo.getObjectInfoWithPodClodConfigPos();
                 objectInfoWithPodClodConfigPo= objectsInfoDetailsList.get(0);
                if (objectInfoWithPodClodConfigPo.getCrObjectInformation().size() == 0) {
                    throw new BadRequestException("Object Information is missing");
                } else {
                    for (CrObjectInformation objectInformation : objectInfoWithPodClodConfigPo.getCrObjectInformation()) {
                        objectInfos.put(objectInformation.getInfoType(), objectInformation.getInfoValue());
                    }
                }
                cloudLoginDetailsList = crObjectInformationPo.getCloudLoginDetails();
                log.info("cloudLoginDetailsList-->" + cloudLoginDetailsList.size());
            } else {
                log.info("else-->" + "No data Found for ObjectInformation");
                loadImportDataRes.setMessage("No data Found for ObjectInformation");
                return loadImportDataRes;
            }
            if (cloudLoginDetailsList.size() > 0) {
                userName = cloudLoginDetailsList.get(0).getUsername();
                password = cloudLoginDetailsList.get(0).getPassword();
                cldUrl = cloudLoginDetailsList.get(0).getUrl();
            } else {
                log.error("No Pod Cloud Login Details Found");
                loadImportDataRes.setMessage("Pod Cloud Login Details Not Found");
                return loadImportDataRes;
            }

            if (groupId != null) {
                List<CrCloudJobStatus> cloudJobStatusList = crCloudJobStatusRepo.findByBatchNameAndCldTemplateIdIn(loadandImportDataReqPo.getBatchName(), cldTemplateIdList);

                if (cloudJobStatusList != null && cloudJobStatusList.size() > 0) {
                    loadImportDataRes.setMessage("Objects Grouping Applied with requestId->" + cloudJobStatusList.get(0).getLoadRequestId());
                    log.info("=============Objects Grouping Applied with requestId->{}==============" + cloudJobStatusList.get(0).getLoadRequestId());
                    Long loadReqId = cloudJobStatusList.get(0).getLoadRequestId();
                    isFirstObjectInGroup=false;
                    insertIntoCrCloudJobStatus(loadReqId,objectsInfoDetailsList,objectInfos,loadandImportDataReqPo,crCloudTemplateHeadersObjectGroupLines, isFirstObjectInGroup);

                    return loadImportDataRes;
                }
            }
            //calling custom restapi for loading data
            if ("RESTAPI".equalsIgnoreCase(objectInfoWithPodClodConfigPo.getLoaderEndpoint())) {
                CustomRestApiReqPo customRestApiReqPo = new CustomRestApiReqPo();
                customRestApiReqPo.setCldTemplateId(loadandImportDataReqPo.getCloudTemplateId());
                customRestApiReqPo.setBatchName(loadandImportDataReqPo.getBatchName());
                customRestApiReqPo.setObjectName(objectName);
                customRestApiReqPo.setObjectId(crCldTempHdrsView.getObjectId());
                customRestApiReqPo.setCldUserName(userName);
                customRestApiReqPo.setCldPassword(password);
                customRestApiReqPo.setPodId(loadandImportDataReqPo.getPodId());
                customRestApiReqPo.setCloudUrl(cldUrl);
                customRestApiReqPo.setRestApiUrl(objectInfos.get("RESTAPI_URL"));
                return customRestApiForCldDataLoad(customRestApiReqPo);
            }
            List<String> expectedObjectInfoKeys = List.of("ParameterList", "DocumentAccount", "DocumentSecurityGroup", "JobName", "InterfaceDetails");
            if (loadandImportDataReqPo.getParameterList() != null && !loadandImportDataReqPo.getParameterList().isEmpty()) {
                expectedObjectInfoKeys = List.of("DocumentAccount", "DocumentSecurityGroup", "JobName", "InterfaceDetails");
                objectInfos.put("ParameterList", loadandImportDataReqPo.getParameterList());
            }
            if (!objectInfos.keySet().containsAll(expectedObjectInfoKeys)) {
                throw new BadRequestException("Mandatory Object Information is missing");
            }
            target = Files.createTempDirectory(null);
            if (objectsInfoDetailsList.size() > 0) {
                csvName = objectInfoWithPodClodConfigPo.getCtlFileName();
            } else {
                log.error("Object Details Not Found");
                loadImportDataRes.setMessage("Object Details Not Found");
                return loadImportDataRes;
            }
            if (crCldTempHdrsView != null)
                metaDataTableId = crCldTempHdrsView.getMetaDataTableId();
            if (metaDataTableId != null) {
                metaDataTableName = crCldTempHdrsView.getMetaDataTableName();
                log.info("metaDataTableName:::::" + metaDataTableName);
            }
            csvPath = target.toString() + File.separator + csvName + ".csv";
            zipfilePath = target + File.separator + metaDataTableName + ".zip";
            String soapUrl = cldUrl + erpurl;
            ErpIntegrationServiceStub erpIntegrationServiceStub = new ErpIntegrationServiceStub(soapUrl);
            // BasicAuthentication for soap service
            util.basicAuthentication1(erpIntegrationServiceStub._getServiceClient(), userName, password);
            if (groupId == null) {
                writingToCsvV1(loadandImportDataReqPo.getCloudTemplateId(), target.toString(), csvName, loadandImportDataReqPo, null,
                        null);
                if (!Constants.PROJECT_CONTRACTS.equalsIgnoreCase(crCldTempHdrsView.getParentObjectCode())) {
                    Utils.zipFile(target.toString(), metaDataTableName, csvName + ".csv");
                }
            }
            if (Constants.PROJECT_CONTRACTS.equalsIgnoreCase(crCldTempHdrsView.getParentObjectCode())) {
                //for project contracts no need to zip csv file
                resultId = loadAndImportProjectContracts(erpIntegrationServiceStub, loadandImportDataReqPo, csvPath, crCldTempHdrsView, objectInfoWithPodClodConfigPo, objectInfos, cloudLoginDetailsList.get(0));
            } else {
                if (groupId != null) {
                    zipfilePath = generateCsvFile(crCloudTemplateHeadersObjectGroupLines.stream().map(x -> x.getCloudTemplateHeadersView()).collect(Collectors.toList()), loadandImportDataReqPo.getBatchName(), target.toString(), objectsInfoDetailsList, groupId);
                    log.info("zipfilePath---->" + zipfilePath);
                }

                Optional<ObjectInfoWithPodClodConfigPo> objectInfoWithPodClodConfig= Utils.getObjectWithLeastSequence(objectsInfoDetailsList);
                log.info("objectInfoWithPodClodConfigPo---->"+objectInfoWithPodClodConfig.isPresent());
                if(objectInfoWithPodClodConfig.isPresent()){
                    // soap service call for loadAndImportData
                    resultId = loadAndImportService(erpIntegrationServiceStub, loadandImportDataReqPo, zipfilePath,
                            erpMetaData, objectInfoWithPodClodConfig.get(), objectInfos);
                }else{
                    loadImportDataRes.setMessage("Object Sequence is missing for the Objects Selected.Please add sequence to all objects through Object Setup Data Screen (Admin Module)");
                    return loadImportDataRes;
                }

                if(resultId!=null && groupId!=null && isFirstObjectInGroup){
                    insertIntoCrCloudJobStatus(resultId,objectsInfoDetailsList,objectInfos,loadandImportDataReqPo,crCloudTemplateHeadersObjectGroupLines,isFirstObjectInGroup);
                    loadImportDataRes.setResultId(resultId);
                    loadImportDataRes.setMessage("Data Loaded successfully");
                    return loadImportDataRes;
                }
            }

            crCloudJobStatus.setObjectCode(objectInfoWithPodClodConfigPo.getObjectCode());
            crCloudJobStatus.setObjectId(objectInfoWithPodClodConfigPo.getObjectId());
            crCloudJobStatus.setCldTemplateId(loadandImportDataReqPo.getCloudTemplateId());
            crCloudJobStatus.setImportType("FBDI"); //This needs to be HDL for HCM objects
            crCloudJobStatus.setDocumentAuthor("ConvertRite");
            crCloudJobStatus.setDocumentTitle(objectInfoWithPodClodConfigPo.getObjectCode() + ".zip");
            crCloudJobStatus.setDocumentSecurityGroup(objectInfos.get("DocumentSecurityGroup"));
            crCloudJobStatus.setDocumentAccount(objectInfos.get("DocumentAccount"));
            crCloudJobStatus.setJobName(objectInfos.get("JobName"));
            crCloudJobStatus.setParameterList(objectInfos.get("ParameterList"));
            crCloudJobStatus.setInterfaceId(objectInfos.get("InterfaceDetails"));
            crCloudJobStatus.setLoadRequestId(resultId);
            crCloudJobStatus.setJobStatus("Processing");
            crCloudJobStatus.setBatchName(loadandImportDataReqPo.getBatchName());
            crCloudJobStatus.setCreationDate(new java.sql.Date(new java.util.Date().getTime()));
            crCloudJobStatus.setCreatedBy("ConvertRite");
            crCloudJobStatus.setLastUpdatedDate(new java.sql.Date(new java.util.Date().getTime()));
            crCloudJobStatus.setLastUpdatedBy("ConvertRite");
            crCloudJobStatusRepo.save(crCloudJobStatus);
            //Queuing sync
            loadImportDataRes.setResultId(resultId);
            loadImportDataRes.setMessage("Data Loaded successfully");
            log.info("resultId-->" + resultId);
        } catch (Exception e) {
            log.error(e.getMessage());
            e.printStackTrace();
            loadImportDataRes.setMessage(e.getMessage());
            loadImportDataRes.setError("Data Loading Failed");
            if (groupId != null) {
                crTemplateRelationRepository.deleteAllByGroupId(groupId);
            }
        }
        return loadImportDataRes;
    }

    private void insertIntoCrCloudJobStatus(Long loadReqId, List<ObjectInfoWithPodClodConfigPo> objectsInfoDetailsList, Map<String, String> objectInfos, LoadandImportDataReqPo loadandImportDataReqPo, List<CrCldTemplateHdrsObjectGroupLinesRes> crCloudTemplateHeadersObjectGroupLines, boolean isFirstObjectInGroup) {
        List<CrCloudJobStatus> crCloudJobStatusList = new ArrayList<>();
        for (CrCldTemplateHdrsObjectGroupLinesRes cldtempObjectGroupLines : crCloudTemplateHeadersObjectGroupLines) {
            CrCloudTemplateHeadersView cldtempHdrs = cldtempObjectGroupLines.getCloudTemplateHeadersView();
            Long cloudTempId = loadandImportDataReqPo.getCloudTemplateId();

            if (cldtempHdrs.getTemplateId() == cloudTempId) {
                Optional<ObjectInfoWithPodClodConfigPo> objectInfo = getObjectById(objectsInfoDetailsList, cldtempHdrs.getObjectId());
                objectInfo.ifPresent(object -> {
                    CrCloudJobStatus crCloudJobStatus = new CrCloudJobStatus();
                    crCloudJobStatus.setObjectCode(object.getObjectCode());
                    crCloudJobStatus.setObjectId(object.getObjectId());
                    crCloudJobStatus.setCldTemplateId(cloudTempId);
                    crCloudJobStatus.setImportType("FBDI"); // This needs to be HDL for HCM objects
                    crCloudJobStatus.setDocumentAuthor("ConvertRite");
                    crCloudJobStatus.setDocumentTitle(object.getObjectCode() + ".zip");
                    crCloudJobStatus.setDocumentSecurityGroup(objectInfos.get("DocumentSecurityGroup"));
                    crCloudJobStatus.setDocumentAccount(objectInfos.get("DocumentAccount"));
                    crCloudJobStatus.setJobName(objectInfos.get("JobName"));
                    crCloudJobStatus.setParameterList(loadandImportDataReqPo.getParameterList());
                    crCloudJobStatus.setInterfaceId(objectInfos.get("InterfaceDetails"));
                    crCloudJobStatus.setLoadRequestId(loadReqId);
                    crCloudJobStatus.setJobStatus("Processing");
                    crCloudJobStatus.setBatchName(loadandImportDataReqPo.getBatchName());
                    java.sql.Date currentDate = new java.sql.Date(new java.util.Date().getTime());
                    crCloudJobStatus.setCreationDate(currentDate);
                    crCloudJobStatus.setCreatedBy("ConvertRite");
                    crCloudJobStatus.setLastUpdatedDate(currentDate);
                    crCloudJobStatus.setLastUpdatedBy("ConvertRite");
                    crCloudJobStatus.setAdditionalInfo("Objects Grouping Applied");
                    if(isFirstObjectInGroup){
                        crCloudJobStatus.setAdditionalInfo("Cloud Load Import is done with the Object Details of ID -"+object.getObjectId());
                    }
                    log.info("=============crCloudJobStatus==============" + loadReqId);
                    crCloudJobStatusList.add(crCloudJobStatus);
                });
            }
        }
        crCloudJobStatusRepo.saveAll(crCloudJobStatusList);
    }

    public Optional<ObjectInfoWithPodClodConfigPo> getObjectById(List<ObjectInfoWithPodClodConfigPo> list, Long objectId) {
        return list.stream()
                .peek(obj -> log.info(objectId + " -Object ID -" + obj.getObjectId()))
                .filter(obj -> obj.getObjectId().equals(objectId))
                .findFirst();
    }

    private String generateCsvFile(List<CrCloudTemplateHeadersView> cldTempIdsList, String batchName, String filePath, List<ObjectInfoWithPodClodConfigPo> objectsInfoDetailsList, Long groupId) throws Exception {
        List<String> fileNames = new ArrayList<>();
        Map<String, String> map = new HashMap();
        // Sort the list based on object ID
        Collections.sort(cldTempIdsList, new Comparator<CrCloudTemplateHeadersView>() {
            @Override
            public int compare(CrCloudTemplateHeadersView o1, CrCloudTemplateHeadersView o2) {
                // Compare based on the object ID
                return Long.compare(o1.getObjectId(), o2.getObjectId());
            }
        });
        Collections.sort(objectsInfoDetailsList, new Comparator<ObjectInfoWithPodClodConfigPo>() {
            @Override
            public int compare(ObjectInfoWithPodClodConfigPo o1, ObjectInfoWithPodClodConfigPo o2) {
                // Compare based on the object ID
                return Long.compare(o1.getObjectId(), o2.getObjectId());
            }
        });
        for (int i = 0; i < cldTempIdsList.size(); i++) {
            if (cldTempIdsList.get(i).getTemplateId() != null) {
                if (cldTempIdsList.get(i).getObjectId().equals(objectsInfoDetailsList.get(i).getObjectId())) {
                    String clobString = cloudTemplateHeaderDaoImpl.downloadFbdiData(cldTempIdsList.get(i).getTemplateId(), batchName);
                    map.put(objectsInfoDetailsList.get(i).getCtlFileName(), clobString);
                }
                fileNames.add(objectsInfoDetailsList.get(i).getCtlFileName());
            }
        }
        String zipPath = Utils.zipMultipleFiles(filePath, fileNames, map);
        return zipPath;
    }

    private Long loadAndImportProjectContracts(ErpIntegrationServiceStub erpIntegrationServiceStub,
                                               LoadandImportDataReqPo loadImportDataReq, String csvPath, CrCloudTemplateHeadersView crCldTempHdrsView,
                                               ObjectInfoWithPodClodConfigPo objectData, Map<String, String> objectInfos, CloudLoginDetails cloudLoginDetails) throws Exception {
        log.info("Start of loadAndImportProjectContracts Method ##########");
        // upload file to ucm
        Long documentId = uploadCsvFileToUcm(csvPath, loadImportDataReq, erpIntegrationServiceStub, objectInfos);
        HttpHeaders headers = new HttpHeaders();
        headers.set("Accept", MediaType.APPLICATION_JSON_VALUE);
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setBasicAuth(cloudLoginDetails.getUsername(), cloudLoginDetails.getPassword());
        String mappingNumber = "";
        if (Constants.CONTRACT_LINES.equalsIgnoreCase(crCldTempHdrsView.getObjectCode())) {
            // ImportMappings RestApi call
            mappingNumber = importMappingsRestApiCall(headers, crCldTempHdrsView, cloudLoginDetails, loadImportDataReq);
        }
        // ImportActivities RestApi Call
        importActvitiesRestApiCall(headers, crCldTempHdrsView, cloudLoginDetails, loadImportDataReq, mappingNumber);

        return documentId;
    }

    private String importMappingsRestApiCall(HttpHeaders headers, CrCloudTemplateHeadersView crCldTempHdrsView,
                                             CloudLoginDetails cloudLoginDetails, LoadandImportDataReqPo loadImportDataReq) {
        RestTemplate restTemplate = new RestTemplate();
        ResponseEntity<CrImportMappingResPo> impMappingRes = restTemplate.exchange(
                cloudLoginDetails.getUrl() + "/crmRestApi/resources/11.13.18.05/importMappings?q=ObjectCode="
                        + crCldTempHdrsView.getObjectCode(),
                HttpMethod.GET, new HttpEntity<Object>(headers), CrImportMappingResPo.class);
        log.info(impMappingRes.getBody().getItems().get(0).getMappingNumber());
        return impMappingRes.getBody().getItems().get(0).getMappingNumber();
    }

    private Long uploadCsvFileToUcm(String csvfilePath, LoadandImportDataReqPo loadImportDataReq,
                                    ErpIntegrationServiceStub erpIntegrationServiceStub, Map<String, String> objectInfos)
            throws Exception {
        DataSource source = new ByteArrayDataSource(Files.readAllBytes(Paths.get(csvfilePath)),
                "text/plain;charset=UTF-8");
        DataHandler dataHandler = new DataHandler(source);
        Base64BinaryDataHandler base64BinaryDataHandler = new Base64BinaryDataHandler();
        base64BinaryDataHandler.setBase64BinaryDataHandler(dataHandler);

        // Creating DocumentDetails
        DocumentDetails documentDetails = new DocumentDetails();
        documentDetails.setContentType("csv");
        documentDetails.setContent(base64BinaryDataHandler);
        documentDetails.setDocumentAccount(objectInfos.get("DocumentAccount"));
        documentDetails.setDocumentSecurityGroup(objectInfos.get("DocumentSecurityGroup"));
        documentDetails.setDocumentAuthor("ConvertRite");
        String objectName = loadImportDataReq.getObjectName();

        String csvName = (objectName + "_" + loadImportDataReq.getBatchName() + ".csv").replaceAll("\\s+", "");
        documentDetails.setDocumentTitle(csvName);
        documentDetails.setFileName(csvName);
        documentDetails.setDocumentName(csvName);

        UploadFileToUcm uploadFileToUcm0 = new UploadFileToUcm();
        uploadFileToUcm0.setDocument(documentDetails);
        // upload File to UCM service call
        UploadFileToUcmResponse uploadFileRes = erpIntegrationServiceStub.uploadFileToUcm(uploadFileToUcm0);
        return Long.parseLong(uploadFileRes.getResult());
    }

    private void importActvitiesRestApiCall(HttpHeaders headers, CrCloudTemplateHeadersView crCldTempHdrsView,
                                            CloudLoginDetails cloudLoginDetails, LoadandImportDataReqPo loadImportDataReq, String mappingNumber) {
        log.info("Start of importActvitiesRestApiCall ###");
        String contentId = (crCldTempHdrsView.getObjectCode() + "_" + loadImportDataReq.getBatchName() + ".csv").replaceAll("\\s+", "");
        List<CrDataFilesReqPo> li = new ArrayList<>();
        CrDataFilesReqPo dtFilesReqPo = new CrDataFilesReqPo();
        dtFilesReqPo.setInputFileContentId(contentId.toUpperCase());
        li.add(dtFilesReqPo);
        RestTemplate restTemplate = new RestTemplate();
        CrImportActivitiesReqPo imptActivitiesReqPo = new CrImportActivitiesReqPo();
        imptActivitiesReqPo.setName((crCldTempHdrsView.getObjectCode() + "_" + loadImportDataReq.getBatchName()).replaceAll("\\s+", ""));
        imptActivitiesReqPo.setActivate("YES");
        imptActivitiesReqPo.setHighVolume("NO");
        if (Constants.CONTRACT_LINES.equalsIgnoreCase(crCldTempHdrsView.getObjectCode()))
            imptActivitiesReqPo.setImportMapping(mappingNumber);
        imptActivitiesReqPo.setObjectCode(crCldTempHdrsView.getObjectCode().replaceAll("\\s+", ""));
        imptActivitiesReqPo.setImportMode("CREATE_RECORD");
        imptActivitiesReqPo.setDataFiles(li);
        HttpEntity<CrImportActivitiesReqPo> importActvtisRequestEntity = new HttpEntity<>(imptActivitiesReqPo, headers);
        ResponseEntity<?> impActvtyRes = restTemplate.exchange(
                cloudLoginDetails.getUrl() + "/crmRestApi/resources/11.13.18.05/importActivities", HttpMethod.POST,
                importActvtisRequestEntity, Object.class);
        log.info(impActvtyRes.getBody().toString());

    }

    private LoadandImportDataResPo customRestApiForCldDataLoad(CustomRestApiReqPo customRestApiReqPo) {
        LoadandImportDataResPo loadandImportDataResPo = new LoadandImportDataResPo();
        String restApiUrl = customRestApi + customRestApiReqPo.getRestApiUrl();
        log.info(restApiUrl + "->RestApiUrl");
        //headers
        HttpHeaders headers = new HttpHeaders();
        headers.set("Accept", MediaType.APPLICATION_JSON_VALUE);
        headers.set("X-TENANT-ID", String.valueOf(customRestApiReqPo.getPodId()));
        //Calling custom RESTAPI for cloud import
        HttpEntity<CustomRestApiReqPo> requestEntity = new HttpEntity<>(customRestApiReqPo,
                headers);
        RestTemplate restTemplate = new RestTemplate();
       ResponseEntity<?> response = restTemplate.exchange(restApiUrl, HttpMethod.POST, requestEntity, String.class);
        log.info("Rest API response----{}",response.getStatusCode());
        loadandImportDataResPo.setMessage("Data imported successfully to cloud");
        return loadandImportDataResPo;
    }

    @Override
    public BasicResponsePo getCldImportStatus(Long cldTemplateId, String batchName) {
        /*Long groupId = null;
        List<Long> cldTemplateIdList = new ArrayList<>();
        BasicResponsePo basicResponsePo = new BasicResponsePo();
        CrCloudTemplateHeadersView crCldTempHdrsView = crCloudTemplateHeadersViewRepository.findById(cldTemplateId).get();
        groupId = crObjectGroupLinesRepo.getGroupIdbyObjectId(crCldTempHdrsView.getObjectId());
        if (groupId != null) {
            List<CrCldTemplateHdrsObjectGroupLinesRes> crCloudTemplateHeadersObjectGroupLines = crCloudTemplateHeadersViewRepository.getCldRemplateHdrsbyGroupId(groupId);
            for (CrCldTemplateHdrsObjectGroupLinesRes cldtempObjectGroupLines : crCloudTemplateHeadersObjectGroupLines) {
                CrCloudTemplateHeadersView cldtempHdrs = cldtempObjectGroupLines.getCloudTemplateHeadersView();
                if (cldtempHdrs != null)
                    cldTemplateIdList.add(cldtempHdrs.getTemplateId());
            }
        } else {
            cldTemplateIdList.add(cldTemplateId);
        }
*/
        BasicResponsePo basicResponsePo = new BasicResponsePo();
        CrCloudJobStatus cloudJobStatus = crCloudJobStatusRepo.findByCldTemplateIdAndBatchName(cldTemplateId, batchName);
        basicResponsePo.setMessage("Successfully retrieved Cloud Import Status");
        basicResponsePo.setPayload(cloudJobStatus);
        return basicResponsePo;
    }
}
