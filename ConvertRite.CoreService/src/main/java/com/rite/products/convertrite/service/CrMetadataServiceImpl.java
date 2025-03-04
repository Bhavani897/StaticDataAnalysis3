package com.rite.products.convertrite.service;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import com.rite.products.convertrite.exception.ValidationException;
import com.rite.products.convertrite.model.*;
import com.rite.products.convertrite.po.*;
import com.rite.products.convertrite.respository.*;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletRequest;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.*;
import java.util.stream.Collectors;

@Service
@Slf4j
public class CrMetadataServiceImpl implements CrMetadataService {
    @Value("${clouddataprocess-url}")
    private String url;
    @Autowired
    CrCloudTemplateService crCloudTemplateService;
    @Autowired
    RestTemplate restTemplate;
    @Value("${cloud-status-check}")
    private String statusUrl;
    @Autowired
    CrCloudTableRepository crCloudTableRepository;
    @Autowired
    CrFileDetailsRepo crFileDetailsRepo;
    @Autowired
    CrCloudColumnsRepository crCloudColumnsRepository;

    @Autowired
    CrCloudTemplateColumnsRepository crCloudTemplateColumnsRepository;
    @Autowired
    CrCloudTemplateHeadersRepository crCloudTemplateHeadersRepository;

    @Autowired
    CrFbdiHdrsRepo crFbdiHdrsRepo;
    @Autowired
    CrFbdiColsRepo crFbdiColsRepo;

    @Override
    public LoadMetaDataFromCloudRes loadMetaDataFromCloud(CrCloudMetadataPo crCloudMetadataPo, HttpServletRequest request) throws ValidationException, Exception {
        log.info("Start of loadMetaDataFromCloud Method in Service class######");
        Connection con = null;
        Long metaDataTableId = null;
        String responseBody = null;
        LoadMetaDataFromCloudRes loadMetaDataFromCloudRes = new LoadMetaDataFromCloudRes();
        String applicationTableName = "";

        String sqlQuery = "select fc.COLUMN_NAME,fc.PHYSICAL_COLUMN_NAME,fc.USER_COLUMN_NAME,fc.STATUS,fc.SHORT_NAME,fc.COLUMN_SEQUENCE,fc.COLUMN_TYPE,fc.WIDTH,fc.NULL_ALLOWED_FLAG,fc.TRANSLATE_FLAG,fc.PRECISION,fc.SCALE,fc.DOMAIN_CODE,fc.DENORM_PATH,fc.ROUTING_MODE,fc.VERSION_COLUMN,fc.ELIGIBLE_TO_BE_SECURED,fc.SECURITY_CLASSIFICATION,fc.SEC_CLASSIFICATION_OVERRIDE,fc.DESCRIPTION from fnd_columns fc,fnd_tables ft where" +
                " ft.table_id=fc.table_id and ft.table_name='"
                + crCloudMetadataPo.getIntTableName() + "'";


        Date date = new Date();
        CrCloudReqPo crCloudReqPo = new CrCloudReqPo();
        crCloudReqPo.setPodId(Integer.parseInt(request.getHeader("X-TENANT-ID")));
        crCloudReqPo.setScheduledJobCall("N");
        crCloudReqPo.setSqlQuery(sqlQuery);
        crCloudReqPo.setDestinationType("Metadata Fetching");
        crCloudReqPo.setCreationDate(date);
        crCloudReqPo.setCreatedBy("Convertrite-Core");
        crCloudReqPo.setLookUpFlag("N");
        crCloudReqPo.setIsInternalServiceCall(true);
        crCloudReqPo.setLastUpdateDate(date);
        crCloudReqPo.setApiCallFrom("Metadata Fetching :InternalCall");


        HttpHeaders headers = new HttpHeaders();
        headers.set("Accept", MediaType.APPLICATION_JSON_VALUE);
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set("Authorization", request.getHeader("Authorization"));
        HttpEntity<CrCloudReqPo> requestEntity = new HttpEntity<>(crCloudReqPo, headers);
        // System.out.println(String.format(url, parametrizedArgs));

        ResponseEntity<String> cloudDataApiResponse = restTemplate.exchange(url, HttpMethod.POST, requestEntity,
                String.class);
        System.out.println(cloudDataApiResponse.getBody());
        if (cloudDataApiResponse.getStatusCode() == HttpStatus.OK) {
            responseBody = cloudDataApiResponse.getBody();
        }


            JSONObject jsonObject = new JSONObject(responseBody);

            JSONObject crCloudStatusInfo = jsonObject.getJSONObject("crCloudStatusInformation");
            long statusId = crCloudStatusInfo.getLong("statusId");

            JSONObject cloudDataProcess = jsonObject.getJSONObject("cloudDataProcess");
            long id = cloudDataProcess.getLong("id");
            while (true) {
                JSONObject statusJson = getStatus(id, statusId);
                String status = statusJson.getString("status");
                if (Objects.equals(status, "completed")) {
                    break;
                }
                if (Objects.equals(status, "error")) {
                    String errorMessage = statusJson.getString("status_error_msg");
                    throw new Exception(errorMessage);
                }
            }

            Optional<CrCloudTables> cloudTable = crCloudTableRepository.findByTableName(crCloudMetadataPo.getMetaDataTableName());
            if (cloudTable.isPresent())
                throw new ValidationException("This MetaDataTableName Already exists,Please give different name");
            // Inserting MetaData into cr_cloud_tables
            CrCloudTables cloudTables = insertTableMetaData(crCloudMetadataPo.getObjectId(), crCloudMetadataPo.getMetaDataTableName(), "T");
            log.info("metaDataTableId:::::" + cloudTables.getTableId());
            insertColumnMetaData(statusId, cloudTables.getTableId());
            loadMetaDataFromCloudRes.setMessage("Successfully loaded MetaData from cloud");
            loadMetaDataFromCloudRes.setTableName(crCloudMetadataPo.getMetaDataTableName());
            loadMetaDataFromCloudRes.setTableId(cloudTables.getTableId());
        return loadMetaDataFromCloudRes;
    }

    @Override
    public List<CrCloudTemplateColumnsResPo> generateSequence(Long templateId, Long objectId, String version,
                                                              HttpServletRequest request) throws Exception {
        log.info("Start Of generateSequence Method in Service Layer ####");
        List<CrCloudTemplateColumns> cloudTemplateColumnsList = new ArrayList<>();
        List<CrCloudTemplateColumnsReqPo> cloudTemplateColumnsPo = new ArrayList<>();
        List<Object> columnSequenceList = new ArrayList<>();
        String msg = "";
        List<CrCloudTemplateColumnsResPo> crCloudTemplateColumns = new ArrayList<>();
        SaveCloudTemplateColumnsResPo saveCloudTemplateColumnsResPo = new SaveCloudTemplateColumnsResPo();
        try {
            if (templateId != null)
                cloudTemplateColumnsList = crCloudTemplateColumnsRepository.findByTemplateId(templateId);

            columnSequenceList = crFbdiColsRepo.getColumnNameandSequence(objectId, version);

            log.info(columnSequenceList.size() + "::::::::size");
            if (!columnSequenceList.isEmpty()) {
                cloudTemplateColumnsPo = getColumnSequence(columnSequenceList, cloudTemplateColumnsList);
                crCloudTemplateColumns = crCloudTemplateService.saveAllCloudTemplateColumns(cloudTemplateColumnsPo);


            } else {
                throw new Exception("We dont have sequence in fbdi workbench for above combination");
            }
        } catch (Exception e) {
            throw new Exception(e.getMessage());
        }
        return crCloudTemplateColumns;
    }

    @Override
    public LoadMetaDataFromCloudRes loadHdlCloudMetaData(Long objectId, String metaDataTableName, MultipartFile file, String objectCode) throws Exception {
        log.info("Start of loadHdlCloudMetaData Method in Service###");
        Long metaDataTableId = null;
        String fileName = "";
        CrCloudTables cloudTables = null;
        LoadMetaDataFromCloudRes loadMetaDataFromCloudRes = new LoadMetaDataFromCloudRes();
        // upload file to ftp path
        fileName = uploadFile(file);
        Optional<CrCloudTables> cloudTablesOptional = crCloudTableRepository.findByTableName(metaDataTableName);
        if (cloudTablesOptional.isPresent()) {
            metaDataTableId = cloudTablesOptional.get().getTableId();
        }
        if (metaDataTableId != null)
            throw new ValidationException("This MetaDataTableName Already exists,Please give different name");
        // Inserting MetaData into xxr_cloud_tables
        cloudTables = insertTableMetaData(objectId, metaDataTableName, "T");
        log.debug("metaDataTableName::" + metaDataTableName);
        // metaDataTableId = crCloudTableRepository.getTableId(metaDataTableName);
        log.info("metaDataTableId:::::" + cloudTables.getTableId());
        // Inserting MetaData columns into xxr_cloud_columns
        insertColumnMetaDataFromHdl(fileName, cloudTables.getTableId(), objectCode, objectId);
        loadMetaDataFromCloudRes.setTableId(cloudTables.getTableId());
        loadMetaDataFromCloudRes.setTableName(metaDataTableName);
        loadMetaDataFromCloudRes.setMessage("Successfully loaded hdl cloud metadata");
        return loadMetaDataFromCloudRes;
    }

    private void insertColumnMetaDataFromHdl(String fileName, Long metaDataTableId, String objectCode, Long objectId)
            throws Exception {

        try {
            CrFileDetails lobResp = crFileDetailsRepo.findByFileName(fileName);
            String lob = lobResp.getFileContent();
            String[] lineArr = lob.split("\n");
            String columnString = "";
            String objectCde = objectCode.replaceAll("\\s+", "");
            for (String line : lineArr) {
                if (line.contains("METADATA|" + objectCde)) {
                    columnString = line;
                    break;
                }
            }
            String[] columnNamesArr = columnString.split("\\|");
            log.info("objectId:::::::" + objectId);
            int j = 1;
            Date date = new Date();

            List<CrCloudColumns> cloudColumnsList = new ArrayList<>();
            for (int i = 0; i < columnNamesArr.length; i++) {

                String columnType = "";
                int columnWidth = 0;
                String columnName = columnNamesArr[i];

                if (columnName.contains("(")) {
                    int index = columnName.indexOf("(");
                    columnName = columnName.substring(0, index);
                } else if (columnName.contains("=<")) {
                    int index = columnName.indexOf("=<");
                    columnName = columnName.substring(0, index);
                } else if (columnName.contains("FLEX:")) {
                    columnName = columnName.substring(5);
                }
                if (columnName.substring(0, 1).equalsIgnoreCase("_"))
                    columnName = columnName.substring(1);
                log.info(columnName + "#####columnName");
                if (columnName.toLowerCase().contains("date")) {
//					log.info("#####columnName enterinngiifff");
                    columnType = "D";
                } else {
                    columnType = "V";
                    columnWidth = 2000;
                }

                CrCloudColumns crCloudColumns = new CrCloudColumns();
                columnName = columnName.toUpperCase();

                crCloudColumns.setColumnId((long) j);
                crCloudColumns.setLastUpdateDate(date);
                crCloudColumns.setCreationDate(date);
                crCloudColumns.setCreatedBy("Convert");
                crCloudColumns.setLastUpdatedBy("Convert");
                crCloudColumns.setColumnName(columnName);
                crCloudColumns.setPhysicalColumnName(columnName);
                crCloudColumns.setUserColumnName(columnNamesArr[i]);
                crCloudColumns.setDescription(columnName);
                crCloudColumns.setColumnType(columnType);
                crCloudColumns.setWidth(String.valueOf(columnWidth));
                crCloudColumns.setNullAllowedFlag("Y");
                crCloudColumns.setTranslateFlag("N");
                crCloudColumns.setColumnSequence(String.valueOf(j));
                crCloudColumns.setTableId(metaDataTableId);
                cloudColumnsList.add(crCloudColumns);
                j++;
            }
            crCloudColumnsRepository.saveAll(cloudColumnsList);

        } catch (Exception e) {
            throw new Exception(e.getMessage());
        }

    }

    public String uploadFile(MultipartFile file) throws Exception {
        log.info("Start of uploadFile Method in Service class######");
        //XxrBlobConvertrite xxrBlobConvertrite = new XxrBlobConvertrite();
        String fileName = null;
        try {

            CrFileDetails crFileDetails = new CrFileDetails();
            UUID uuid = UUID.randomUUID();
            fileName = org.apache.commons.io.FilenameUtils.getName(file.getOriginalFilename());
            String[] fileNameArr = fileName.split("\\.");
            String result = new BufferedReader(new InputStreamReader(file.getInputStream())).lines()
                    .collect(Collectors.joining("\n"));
            //xxrBlobConvertrite = xxrBlobConvertriteRepository.findByBlobName(fileName);
            crFileDetails = crFileDetailsRepo.findByFileName(fileName);
            System.out.println(fileName + "crFileDetails-->" + crFileDetails);

            if (crFileDetails != null) {
                System.out.println("if-->" + fileName);
                crFileDetails.setFileContent(result);
                crFileDetailsRepo.save(crFileDetails);
            } else {
                System.out.println("else-->" + fileName);
                CrFileDetails crFile = new CrFileDetails();
                crFile.setFileContent(result);
                crFile.setFileName(fileName);
                crFile.setFileType(fileNameArr[1]);
                crFile.setCreatedBy("ConvertRiteAdmin");
                crFile.setCreateDate(new java.sql.Date(new java.util.Date().getTime()));
                crFileDetailsRepo.save(crFile);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return fileName;
    }

    private List<CrCloudTemplateColumnsReqPo> getColumnSequence(List<Object> columnSequenceList,
                                                                List<CrCloudTemplateColumns> cloudTemplateColumnsList) throws Exception {
        List<CrCloudTemplateColumnsReqPo> cloudTemplateColumnsPo = new ArrayList<>();
        try {

            for (CrCloudTemplateColumns crCloudTemplateColumns : cloudTemplateColumnsList) {
                Integer seq = null;
                for (Object columnSequence : columnSequenceList) {
                    Object[] obj = (Object[]) columnSequence;
                    String columnName = (String) obj[0];
                    if (crCloudTemplateColumns.getColumnName().equalsIgnoreCase(columnName)) {
                        seq = (int) obj[1];
                        break;
                    }
                }
                CrCloudTemplateColumnsReqPo crCloudTemplateColumnsReqPo = new CrCloudTemplateColumnsReqPo();

                crCloudTemplateColumnsReqPo.setTemplateId(crCloudTemplateColumns.getTemplateId());
                crCloudTemplateColumnsReqPo.setColumnId(crCloudTemplateColumns.getColumnId());
                crCloudTemplateColumnsReqPo.setColumnName(crCloudTemplateColumns.getColumnName());
                if (seq != null)
                    crCloudTemplateColumnsReqPo.setSeq(seq);
                //saveCloudTemplateColumnsPo.setEnableFlag(xxrCloudTemplateColumns.getEnabledFlag());
                //saveCloudTemplateColumnsPo.setEndDate(xxrCloudTemplateColumns.getEndDate());
                //saveCloudTemplateColumnsPo.setStartDate(xxrCloudTemplateColumns.getStartDate());
                crCloudTemplateColumnsReqPo.setMappingSetId(crCloudTemplateColumns.getMappingSetId());
                crCloudTemplateColumnsReqPo.setMappingType(crCloudTemplateColumns.getMappingType());
                crCloudTemplateColumnsReqPo.setMappingValue1(crCloudTemplateColumns.getMappingValue1());
                crCloudTemplateColumnsReqPo.setMappingValue2(crCloudTemplateColumns.getMappingValue2());
                crCloudTemplateColumnsReqPo.setMappingValue3(crCloudTemplateColumns.getMappingValue3());
                crCloudTemplateColumnsReqPo.setMappingValue4(crCloudTemplateColumns.getMappingValue4());
                crCloudTemplateColumnsReqPo.setMappingValue5(crCloudTemplateColumns.getMappingValue5());
                crCloudTemplateColumnsReqPo.setSelected(crCloudTemplateColumns.getSelected());
                crCloudTemplateColumnsReqPo.setSourceColumnId(crCloudTemplateColumns.getSourceColumnId());
                crCloudTemplateColumnsReqPo.setColumnType(crCloudTemplateColumns.getColumnType());
                crCloudTemplateColumnsReqPo.setWidth(crCloudTemplateColumns.getWidth());
                crCloudTemplateColumnsReqPo.setNullAllowedFlag(crCloudTemplateColumns.getNullAllowedFlag());
                crCloudTemplateColumnsReqPo.setUniqueTransRef(crCloudTemplateColumns.getUniqueTransRef());
                crCloudTemplateColumnsReqPo.setInsertOrDelete("k");
                cloudTemplateColumnsPo.add(crCloudTemplateColumnsReqPo);

            }

        } catch (Exception e) {
            throw new Exception(e.getMessage());
        }

        return cloudTemplateColumnsPo;
    }

    JSONObject getStatus(long id, long statusId) {
        HttpHeaders headers = new HttpHeaders();
        String responseBody = null;
        String url = statusUrl + "?id=" + id + "&statusid=" + statusId;
        HttpEntity<?> httpEntity = new HttpEntity<>(headers);
        restTemplate = new RestTemplate();
        ResponseEntity<String> response = restTemplate.exchange(url, HttpMethod.GET, httpEntity, String.class);
        if (response.getStatusCode() == HttpStatus.OK) {
            responseBody = response.getBody();
        }
        JSONObject jsonObject = new JSONObject(responseBody);
        return jsonObject;

    }

    private CrCloudTables insertTableMetaData(Long objectId, String metaDataTableName, String tableType)  {
        PreparedStatement insertStmnt = null;
        int count = 0;
        CrCloudTables cloudTablesRes = new CrCloudTables();
        // String metaDataTable = "";
            CrCloudTables crCloudTables = new CrCloudTables();
            crCloudTables.setTableName(metaDataTableName);
            crCloudTables.setUserTableName(metaDataTableName);
            crCloudTables.setObjectId(objectId);
            crCloudTables.setPhysicalTableName(metaDataTableName);
            crCloudTables.setTableType(tableType);
            Date date = new Date();
            crCloudTables.setLastUpdateDate(date);
            crCloudTables.setLastUpdatedBy("Convert-Core");
            crCloudTables.setCreationDate(date);
            crCloudTables.setCreatedBy("Convert-Core");
            crCloudTables.setApplicationShortName("Convertrite");
        return crCloudTableRepository.save(crCloudTables);
    }

    private void insertColumnMetaData(Long requestId, Long metaDataTableId) throws IOException, CsvValidationException {
        Date date = new Date();
        Optional<CrFileDetails> crFileDetailsOptional = crFileDetailsRepo.findByCldFileId(requestId);
        if (crFileDetailsOptional.isPresent()) {
            CrFileDetails res = crFileDetailsOptional.get();
            String lob = res.getFileContent();
            String[] lines = lob.split("\n");

            String[] headers = lines[0].split(",");

            List<CrCloudColumns> crCloudColumnList = new ArrayList<>();

            try (CSVReader csvReader = new CSVReader(new StringReader(lob))) {
                String[] values;
                csvReader.readNext();
                int i = 1;
                while ((values = csvReader.readNext()) != null) {
                    CrCloudColumns crCloudColumns = new CrCloudColumns();
                    crCloudColumns.setColumnId((long) i);
                    crCloudColumns.setLastUpdateDate(date);
                    crCloudColumns.setCreationDate(date);
                    crCloudColumns.setCreatedBy("Convert");
                    crCloudColumns.setLastUpdatedBy("Convert");


                    for (int j = 0; j < headers.length && j < values.length; j++) {
                        String header = headers[j];
                        String value = values[j];

                        switch (header) {
                            case "COLUMN_NAME":
                                crCloudColumns.setColumnName(value);
                                crCloudColumns.setPhysicalColumnName(value);
                                break;
                            case "USER_COLUMN_NAME":
                                crCloudColumns.setUserColumnName(value);
                                break;
                            case "DESCRIPTION":
                                crCloudColumns.setDescription(value);
                                break;
                            case "COLUMN_TYPE":
                                crCloudColumns.setColumnType(value);
                                break;
                            case "WIDTH":
                                crCloudColumns.setWidth(value);
                                break;
                            case "NULL_ALLOWED_FLAG":
                                crCloudColumns.setNullAllowedFlag(value);
                                break;
                            case "TRANSLATE_FLAG":
                                crCloudColumns.setTranslateFlag(value);
                                break;
                            case "PRECISION":
                                crCloudColumns.setPrecision(value);
                                break;
                            case "SCALE":
                                crCloudColumns.setScale(value);
                                break;
                            case "DOMAIN_CODE":
                                crCloudColumns.setDomainCode(value);
                                break;
                            case "DENORM_PATH":
                                crCloudColumns.setDenormPath(value);
                                break;
                            case "ROUTING_MODE":
                                crCloudColumns.setRoutingMode(value);
                                break;
                            case "COLUMN_SEQUENCE":
                                crCloudColumns.setColumnSequence(value);
                                break;
                            case "CLOUD_VERSION":
                                crCloudColumns.setCloudVersion(value);
                                break;
                            case "ELIGIBLE_TO_BE_SECURED":
                                crCloudColumns.setEligibleToBeSecured(value);
                                break;
                            case "SECURITY_CLASSIFICATION":
                                crCloudColumns.setSecurityClassification(value);
                                break;

                            default:
                                break;
                        }
                    }

                    crCloudColumns.setTableId(metaDataTableId);
                    crCloudColumnList.add(crCloudColumns);
                    i++;
                }

                crCloudColumnsRepository.saveAll(crCloudColumnList);


            }
        }
    }
}
