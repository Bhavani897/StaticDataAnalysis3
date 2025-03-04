package com.rite.products.convertrite.service;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.opencsv.CSVWriter;
import com.rite.products.convertrite.enums.Status;
import com.rite.products.convertrite.model.*;
import com.rite.products.convertrite.po.*;
import com.rite.products.convertrite.respository.*;
import org.apache.axiom.attachments.ByteArrayDataSource;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Service;

import com.rite.products.convertrite.exception.ValidationException;
import com.rite.products.convertrite.hcm.stubs.GenericSoapServiceStub;
import com.rite.products.convertrite.hcm.stubs.GenericSoapServiceStub.Document_type0;
import com.rite.products.convertrite.hcm.stubs.GenericSoapServiceStub.Field_type0;
import com.rite.products.convertrite.hcm.stubs.GenericSoapServiceStub.Fil;
import com.rite.products.convertrite.hcm.stubs.GenericSoapServiceStub.Generic;
import com.rite.products.convertrite.hcm.stubs.GenericSoapServiceStub.GenericRequest;
import com.rite.products.convertrite.hcm.stubs.GenericSoapServiceStub.GenericResponse;
import com.rite.products.convertrite.hcm.stubs.GenericSoapServiceStub.Servce;
import com.rite.products.convertrite.hcm.stubs.HCMDataLoaderStub;
import com.rite.products.convertrite.hcm.stubs.HCMDataLoaderStub.GetDataSetStatus;
import com.rite.products.convertrite.hcm.stubs.HCMDataLoaderStub.GetDataSetStatusResponse;
import com.rite.products.convertrite.hcm.stubs.HCMDataLoaderStub.ImportAndLoadData;
import com.rite.products.convertrite.hcm.stubs.HCMDataLoaderStub.ImportAndLoadDataResponse;
import com.rite.products.convertrite.utils.Utils;

@Service
public class HcmDataImportServiceImpl implements HcmDataImportService {

    private static final Logger log = LoggerFactory.getLogger(HcmDataImportServiceImpl.class);

    @Autowired
    Utils utils;
    @Autowired
    CrCloudJobStatusRepo crCloudJobStatusRepo;
    @Autowired
    CrCloudTemplateHeadersRepository crCloudTemplateHeadersRepository;
    @Autowired
    CrProjectsObjectsRepo crProjectsObjectsRepo;
    @Autowired
    CrCloudTemplateHeadersViewRepository crCloudTemplateHeadersViewRepository;
    @Autowired
    CrFileDetailsRepo crFileDetailsRepo;
    @Autowired
    CloudTemplateHeaderDaoImpl cloudTemplateHeaderDaoImpl;
    @Autowired
    XxrCldTempHdrsViewRepository xxrCldTempHdrsViewRepository;
    @Autowired
    XxrHcmDataLoaderRepository xxrHcmDataLoaderRepository;
    @Autowired
    XxrCloudTemplateHeadersRepository xxrCloudTemplateHeadersRepository;
    @Value("${hcm_soapurl}")
    private String hcmUrl;
    @Value("${generic_soapurl}")
    private String genericSoapUrl;

    @Override
    public DataSetStausResPo getDataSetStatus(String contentId, String processId, Long cldTemplateId) throws ValidationException, Exception {
        log.info("Start of getDataSetStatus in service####");
        DataSetStausResPo dataSetStausResPo = new DataSetStausResPo();
        GetDataSetStatusResponse getDataSetStatusResponse = new GetDataSetStatusResponse();
        try {
            XxrCldTempHdrsView xxrCldTempHdrsView = xxrCldTempHdrsViewRepository.findByTemplateId(cldTemplateId);
            GetDataSetStatus getDataSetStatus = new GetDataSetStatus();
            getDataSetStatus.setParameters("ContentId=" + contentId + ",ProcessId=" + processId);
            String soapUrl = utils.getCloudUrl(xxrCldTempHdrsView) + hcmUrl;
            HCMDataLoaderStub hCMDataLoaderStub = new HCMDataLoaderStub(soapUrl);
            // BasicAuthentication for soap service
            utils.basicAuthentication(hCMDataLoaderStub._getServiceClient(), xxrCldTempHdrsView);
            getDataSetStatusResponse = hCMDataLoaderStub.getDataSetStatus(getDataSetStatus);
            dataSetStausResPo.setResultStatus(getDataSetStatusResponse.getResult());

        } catch (ValidationException e) {
            throw new ValidationException(e.getMessage());
        } catch (Exception e) {
            throw new Exception(e.getMessage());
        }
        return dataSetStausResPo;
    }

    @Override
    public HcmLoadAndImportDataRes hcmLoadAndImportData(HcmLoadandImportDataReqPo hcmLoadandImportDataReqPo, String bearerToken, HttpServletResponse response) throws ValidationException, Exception {
        log.info("Start of hcmLoadAndImportData in service####");
        HcmLoadAndImportDataRes hcmLoadAndImportDataRes = new HcmLoadAndImportDataRes();
        String contentId = "";
        String processId = "";
        try {
            String[] cloudTemplateIdArray = hcmLoadandImportDataReqPo.getCloudTemplateId().split(",");

            log.info("First cloudTemplateId: " + Long.valueOf(cloudTemplateIdArray[0].trim()));

            // Fetch the first cloud template header view
            Optional<CrCloudTemplateHeadersView> crCloudTemplateHeadersView = crCloudTemplateHeadersViewRepository.findById(Long.valueOf(cloudTemplateIdArray[0].trim()));
            CrCloudTemplateHeadersView crCldTempHdrsView = crCloudTemplateHeadersView.get();
            // upload zipped ".dat" file to UCM location
            contentId = uploadZipToUcm(hcmLoadandImportDataReqPo, crCldTempHdrsView, bearerToken, response);
//			log.info("contentId::" + contentId);
            // Hcm Data loading to cloud
            processId = hcmCloudDataLoader(contentId, crCldTempHdrsView, hcmLoadandImportDataReqPo.getPodId(), bearerToken);
            // save details to hcmdataloder table
            saveHcmDataLoaderDetails(hcmLoadandImportDataReqPo, contentId, processId);
            hcmLoadAndImportDataRes.setContentId(contentId);
            hcmLoadAndImportDataRes.setProcessId(processId);

        } catch (ValidationException e) {
            throw new ValidationException(e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception(e.getMessage());
        }
        return hcmLoadAndImportDataRes;
    }

    private void saveHcmDataLoaderDetails(HcmLoadandImportDataReqPo hcmLoadandImportDataReqPo, String contentId, String processId) {

        String[] cloudTemplateIdArray = hcmLoadandImportDataReqPo.getCloudTemplateId().toString().split(",");

        for (String cldTempId : cloudTemplateIdArray) {

            CrCloudJobStatus crHcmDataLoader = new CrCloudJobStatus();

            crHcmDataLoader.setObjectId(hcmLoadandImportDataReqPo.getParentObjectId());

            crHcmDataLoader.setCldTemplateId(Long.parseLong(cldTempId));

            crHcmDataLoader.setContentId(contentId);

            crHcmDataLoader.setImportType("HCM");

            crHcmDataLoader.setLoadRequestId(Long.parseLong(processId));

            crHcmDataLoader.setBatchName(hcmLoadandImportDataReqPo.getBatchName());

            crHcmDataLoader.setDocumentAccount(hcmLoadandImportDataReqPo.getDocumentAccount());

            crHcmDataLoader.setDocumentAuthor(hcmLoadandImportDataReqPo.getDocumentAuthor());

            crHcmDataLoader.setDocumentSecurityGroup(hcmLoadandImportDataReqPo.getDocumentSecurityGroup());

            crHcmDataLoader.setDocumentTitle(hcmLoadandImportDataReqPo.getDocumentTitle());

            crHcmDataLoader.setCreationDate(new java.sql.Date(new java.util.Date().getTime()));

            crHcmDataLoader.setCreatedBy("ConvertRite");

            crHcmDataLoader.setLastUpdatedDate(new java.sql.Date(new java.util.Date().getTime()));

            crHcmDataLoader.setLastUpdatedBy("ConvertRite");

            crCloudJobStatusRepo.save(crHcmDataLoader);

        }

    }

    private String hcmCloudDataLoader(String contentId, CrCloudTemplateHeadersView crCldTempHdrsView, Long podId, String bearerToken) throws ValidationException, Exception {
        ImportAndLoadDataResponse importAndLoadDataResponse = new ImportAndLoadDataResponse();
        try {
            ImportAndLoadData importAndLoadData = new ImportAndLoadData();
            importAndLoadData.setContentId(contentId);
            importAndLoadData.setParameters("DeleteSourceFile=N");
            HCMDataLoaderStub hCMDataLoaderStub = (HCMDataLoaderStub) utils.getCloudLoginDetails(podId, bearerToken, crCldTempHdrsView, "hcmUrl");

            importAndLoadDataResponse = hCMDataLoaderStub.importAndLoadData(importAndLoadData);
        } catch (Exception e) {
            throw new Exception(e.getMessage());
        }
        return importAndLoadDataResponse.getResult();
    }

    private String uploadZipToUcm(HcmLoadandImportDataReqPo hcmLoadandImportDataReqPo, CrCloudTemplateHeadersView crCldTempHdrsView, String bearerToken, HttpServletResponse response) throws ValidationException, Exception {
        String contentId = "";
        String statusMessage = "";
        String statusCode = "";
        String zipfilePath = "";
        try {
            Field_type0 fieldType0 = new Field_type0();
            fieldType0.setName("dDocTitle");
            fieldType0.setString(hcmLoadandImportDataReqPo.getDocumentTitle());
            Field_type0 fieldType1 = new Field_type0();
            fieldType1.setName("dDocType");
            fieldType1.setString("Document");
            Field_type0 fieldType2 = new Field_type0();
            fieldType2.setName("dDocAuthor");
            fieldType2.setString(hcmLoadandImportDataReqPo.getDocumentAuthor());
            Field_type0 fieldType3 = new Field_type0();
            fieldType3.setName("dSecurityGroup");
            fieldType3.setString(hcmLoadandImportDataReqPo.getDocumentSecurityGroup());
            Field_type0 fieldType4 = new Field_type0();
            fieldType4.setName("dDocAccount");
            fieldType4.setString(hcmLoadandImportDataReqPo.getDocumentAccount());

            // Zip ".dat" file in temp location
            zipfilePath = zipDatFile(hcmLoadandImportDataReqPo, response);

            DataSource source = new ByteArrayDataSource(Files.readAllBytes(Paths.get(zipfilePath)), "text/plain;charset=UTF-8");
            DataHandler dataHandler = new DataHandler(source);

            Fil file = new Fil();
            file.setHref(hcmLoadandImportDataReqPo.getDocumentTitle());
            file.setName("primaryFile");
            file.setContents(dataHandler);
            Field_type0[] fieldArr = new Field_type0[]{fieldType0, fieldType1, fieldType2, fieldType3, fieldType4};
            Fil[] fileArr = new Fil[]{file};

            Document_type0 document_type0 = new Document_type0();
            document_type0.setField(fieldArr);
            document_type0.setFile(fileArr);

            Servce service = new Servce();
            service.setIdcService("CHECKIN_UNIVERSAL");
            service.setDocument(document_type0);

            Generic generic = new Generic();
            generic.setWebKey("cs");
            generic.setService(service);

            GenericRequest genericRequest = new GenericRequest();
            genericRequest.setGenericRequest(generic);

            GenericSoapServiceStub genericSoapServiceStub = (GenericSoapServiceStub) utils.getCloudLoginDetails(hcmLoadandImportDataReqPo.getPodId(), bearerToken, crCldTempHdrsView, "genericSoapUrl");
            GenericResponse genericResp = genericSoapServiceStub.genericSoapOperation(genericRequest);

            Field_type0[] fieldArry = genericResp.getGenericResponse().getService().getDocument().getField();
            for (Field_type0 fieldType : fieldArry) {
                if (fieldType.getName().equals("dDocName")) {
                    contentId = fieldType.getString();
                    log.info("contentId-->" + contentId);
                }
                if (fieldType.getName().equals("StatusMessage")) {
                    statusMessage = fieldType.getString();
                }
                if (fieldType.getName().equals("StatusCode")) {
                    statusCode = fieldType.getString();
                }
            }
            Predicate<String> p = x -> (x.equals("0"));
            if (!p.test(statusCode)) {
                throw new ValidationException(statusMessage);
            }
        } catch (ValidationException e) {
            throw new ValidationException(e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception(e.getMessage());
        }
        return contentId;
    }

    private String zipDatFile(HcmLoadandImportDataReqPo hcmLoadandImportDataReqPo, HttpServletResponse response) throws Exception {
        Long parentObjectId = null;
        Path target = null;
        String clobString = "";
        String zipfilePath = "";
        FileWriter outputfile = null;
        String parentObjectCode = "";
        try {
            // Create Temp Dir

            String[] cloudTemplateIdArray = hcmLoadandImportDataReqPo.getCloudTemplateId().split(",");
            for (String id : cloudTemplateIdArray) {
                log.info(" - " + Long.valueOf(id.trim()));
            }

            target = Files.createTempDirectory("");
            Optional<CrCloudTemplateHeadersView> crCloudTemplateHeadersViewOptional = crCloudTemplateHeadersViewRepository.findById(Long.valueOf(cloudTemplateIdArray[0].trim()));

            if (hcmLoadandImportDataReqPo.getParentObjectId() != null) {
                CrProjectsObjects crProjectsObjects = crProjectsObjectsRepo.getByObjectIdAndProjectId(hcmLoadandImportDataReqPo.getParentObjectId(), hcmLoadandImportDataReqPo.getProjectId());
                parentObjectCode = Utils.removeSpace(crProjectsObjects.getObjectName());
            }

            clobString = cloudTemplateHeaderDaoImpl.generateHdlFromLob1(String.valueOf(hcmLoadandImportDataReqPo.getCloudTemplateId()), hcmLoadandImportDataReqPo.getBatchName(), hcmLoadandImportDataReqPo.getIsIntialLoad());

            // create dat file in temp location
            String filePath = target + File.separator + parentObjectCode + ".dat";
            File fil = new File(filePath);
            outputfile = new FileWriter(fil);
            outputfile.write(clobString);
            outputfile.close();

            zipfilePath = target + File.separator + parentObjectCode + ".zip";
            // zip dat file
            Utils.zipFile(target.toString(), parentObjectCode, parentObjectCode + ".dat");
        } catch (Exception e) {
            throw new Exception(e.getMessage());
        }
        return zipfilePath;
    }

    @Override
    public List<XxrHcmDataLoaderResPo> getHcmDataLoaderDetails(HcmDetailsPageReqPo hcmDetailsPageReqPo, HttpHeaders httpHeaders) throws Exception {
        log.info("Start of getHcmDataLoaderDetails Method in Service###");
        List<XxrHcmDataLoader> hcmDataLoaderLi = new ArrayList<>();
        List<XxrHcmDataLoaderResPo> hcmDataRes = new ArrayList<>();
        try {

            Pageable pageable = PageRequest.of(hcmDetailsPageReqPo.getPageNo(), hcmDetailsPageReqPo.getPageSize(), Sort.by(hcmDetailsPageReqPo.getSortDirection(), hcmDetailsPageReqPo.getSortBy()));
            Page<XxrHcmDataLoader> pageContent = xxrHcmDataLoaderRepository.findAll(pageable);
            httpHeaders.set("pagecount", String.valueOf(pageContent.getTotalPages()));
            httpHeaders.set("totalcount", String.valueOf(pageContent.getTotalElements()));
            if (pageContent.hasContent()) hcmDataLoaderLi = pageContent.getContent();
            // hcmDataLoaderLi = xxrHcmDataLoaderRepository.findAll();
            hcmDataLoaderLi.stream().forEach(x -> {
                // List<XxrCloudTemplateHeader> cloudTemplateHeaderList = new ArrayList<>();
                XxrHcmDataLoaderResPo xxrHcmDataLoaderResPo = new XxrHcmDataLoaderResPo();
                xxrHcmDataLoaderResPo.setCloudTemplateId(x.getCloudTemplateId());
                XxrCldTempHdrsView xxrCloudTemplateHeader = xxrCldTempHdrsViewRepository.findByTemplateId(x.getCloudTemplateId());
                // XxrCloudTemplateHeader xxrCloudTemplateHeader =
                // cloudTemplateHeaderList.get(0);
                xxrHcmDataLoaderResPo.setPodName(xxrCloudTemplateHeader.getPodName());
                xxrHcmDataLoaderResPo.setPodId(x.getPodId());
                xxrHcmDataLoaderResPo.setProjectId(x.getProjectId());
                xxrHcmDataLoaderResPo.setProjectName(xxrCloudTemplateHeader.getProjectName());
                xxrHcmDataLoaderResPo.setParentObjectName(xxrCloudTemplateHeader.getParentObjectCode());
                xxrHcmDataLoaderResPo.setParentObjectId(x.getParentObjectId());
                xxrHcmDataLoaderResPo.setXxrBatchName(x.getXxrBatchName());
                xxrHcmDataLoaderResPo.setCloudTemplateName(xxrCloudTemplateHeader.getTemplateName());
                xxrHcmDataLoaderResPo.setContentId(x.getContentId());
                xxrHcmDataLoaderResPo.setDocumentAccount(x.getDocumentAccount());
                xxrHcmDataLoaderResPo.setDocumentAuthor(x.getDocumentAuthor());
                xxrHcmDataLoaderResPo.setDocumentSecurityGroup(x.getDocumentSecurityGroup());
                xxrHcmDataLoaderResPo.setDocumentTitle(x.getDocumentTitle());
                xxrHcmDataLoaderResPo.setId(x.getId());
                xxrHcmDataLoaderResPo.setProcessId(x.getProcessId());

                hcmDataRes.add(xxrHcmDataLoaderResPo);
            });
        } catch (Exception e) {
            throw new Exception(e.getMessage());
        }
        return hcmDataRes;
    }

    @Override
    public CrCloudDataProcessResPo processHdlReconcile(Long cldTemplateId, String batchName, HttpServletRequest request) throws Exception {
        log.info("processHdlReconcile for cldTemplateId {}", cldTemplateId);
        CrCloudDataProcessResPo crCloudDataProcessResPo = new CrCloudDataProcessResPo();
        List<CrCloudJobStatus> jobStatusList = crCloudJobStatusRepo.findByCldTemplateIdAndBatchNameOrderByCreationDateDesc(cldTemplateId, batchName);
        if (jobStatusList != null && jobStatusList.size() != 0) {
            InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream("HdlErrorDetails.sql");
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            String sql = reader.lines().collect(Collectors.joining(System.lineSeparator()));
            sql = sql.replaceAll("\\{0\\}", jobStatusList.get(0).getContentId());
            crCloudDataProcessResPo = utils.cloudDataProcessThroughSqlQuery(sql, "HDL_RECONCILE", request);
        } else {
            throw new ValidationException("No Records present in CloudJobStatus for cldTemplateId: " + cldTemplateId);
        }
        return crCloudDataProcessResPo;
    }

    @Override
    public CrCloudDataProcessResPo processHdlSummary(String batchName, Long cldTemplateId, HttpServletRequest request) throws Exception {
        log.info("Start of processHdlSummary method in service");
        CrCloudDataProcessResPo crCloudDataProcessResPo = new CrCloudDataProcessResPo();
        List<CrCloudJobStatus> jobStatusList = crCloudJobStatusRepo.findByCldTemplateIdAndBatchNameOrderByCreationDateDesc(cldTemplateId, batchName);
        if (jobStatusList != null && jobStatusList.size() != 0) {
            InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream("HDL_Summary.sql");
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            String sql = reader.lines().collect(Collectors.joining(System.lineSeparator()));
            sql = sql.replaceAll("\\{0\\}", jobStatusList.get(0).getContentId());
            crCloudDataProcessResPo = utils.cloudDataProcessThroughSqlQuery(sql, "HDL_SUMMARY", request);
        } else {
            throw new ValidationException("No Records present in CloudJobStatus for cldTemplateId: " + cldTemplateId);
        }
        return crCloudDataProcessResPo;
    }

    @Override
    public XxrCloudDataProcess processHdlStatus(String contentId, Long podId, Long projectId, HttpServletRequest request) throws Exception {
        log.info("start of processHdlStatus contentId {}", contentId);
        XxrCloudDataProcess xxrCloudDataProcess = new XxrCloudDataProcess();
        InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream("HDL_Success_Error.sql");
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        String sql = reader.lines().collect(Collectors.joining(System.lineSeparator()));
        sql = sql.replaceAll("\\{0\\}", contentId);
        xxrCloudDataProcess = utils.cldDataProcess(sql, "HdlStatus", request, projectId);
        return xxrCloudDataProcess;
    }

    @Override
    public void hdlReport(Long statusId, Long id, String reportType, HttpServletResponse response) throws ValidationException, Exception {
        String reportName = reportType.equalsIgnoreCase("Reconcile") ? "ReconcileReport" : "SummaryReport";
        response.setContentType("text/csv");
        response.setHeader(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=" + reportName + ".csv");
        JSONObject statusJson = utils.getCldJobStatus(id, statusId);
        String status = statusJson.getString("status");
        if (List.of("processing", "starting").contains(status))
            throw new ValidationException("Request is still in process,Please wait for sometime");
        else if ("error".equals(status))
            throw new ValidationException("Error while fetching " + reportName + " information from Fusion Instance");
        else {
            downloadCsvFile(statusId, response);
        }
    }

    @Override
    public CrHcmCloudImportStatusResPo hcmCldImportSummary(Long statusId, Long id) throws ValidationException, Exception {
        CrHcmCloudImportStatusResPo crHcmCloudImportStatusResPo = new CrHcmCloudImportStatusResPo();
        JSONObject statusJson = utils.getCldJobStatus(id, statusId);
        String status = statusJson.getString("status");
        if (List.of("processing", "starting").contains(status))
            throw new ValidationException("Request is still in process,Please wait for sometime");
        else if ("error".equals(status))
            throw new ValidationException("Something went wrong,Please contact system administrator");
        else {
            CrFileDetails res = new CrFileDetails();
            Optional<CrFileDetails> crFileDetailsOptional = crFileDetailsRepo.findByCldFileId(statusId);
            if (crFileDetailsOptional.isPresent()) {
                res = crFileDetailsOptional.get();
                String lob = res.getFileContent();
                String[] lineArr = lob.split("\n");
                List<String[]> csvList = new ArrayList<>();
                for (String line : lineArr) {
                    csvList.add(line.split(","));
                }
                String[] statusArr = csvList.get(1);
                if (statusArr != null) {
                    crHcmCloudImportStatusResPo.setImportLinesTotalCount(statusArr[6]);
                    crHcmCloudImportStatusResPo.setImportErrorCount(statusArr[7]);
                    crHcmCloudImportStatusResPo.setLoadedCount(statusArr[8]);
                    crHcmCloudImportStatusResPo.setErrorCount(statusArr[9]);
                } else {
                    throw new ValidationException("Hcm cloud import status is empty");
                }
            }
        }
        return crHcmCloudImportStatusResPo;
    }

    private void downloadCsvFile(Long statusId, HttpServletResponse response) throws Exception {
        PrintWriter writer = response.getWriter();
        CrFileDetails res = new CrFileDetails();
        Optional<CrFileDetails> crFileDetailsOptional = crFileDetailsRepo.findByCldFileId(statusId);
        if (crFileDetailsOptional.isPresent()) {
            res = crFileDetailsOptional.get();
            CSVWriter csvWriter = new CSVWriter(writer, CSVWriter.DEFAULT_SEPARATOR, CSVWriter.NO_QUOTE_CHARACTER, CSVWriter.NO_ESCAPE_CHARACTER, CSVWriter.DEFAULT_LINE_END);
            String lob = res.getFileContent();
            String[] lineArr = lob.split("\n");
            List<String[]> csvList = new ArrayList<>();
            for (String line : lineArr) {
                csvList.add(line.split(","));
            }
            csvWriter.writeAll(csvList);
            csvWriter.flush();
            csvWriter.close();
        }
    }
}