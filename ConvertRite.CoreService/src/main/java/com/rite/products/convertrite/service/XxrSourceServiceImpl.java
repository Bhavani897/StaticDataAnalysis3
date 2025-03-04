package com.rite.products.convertrite.service;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.rite.products.convertrite.model.*;
import com.rite.products.convertrite.multitenancy.config.tenant.hibernate.DynamicDataSourceBasedMultiTenantConnectionProvider;
import com.rite.products.convertrite.po.*;
import com.rite.products.convertrite.respository.*;
import org.apache.commons.math3.exception.NoDataException;
import org.apache.poi.util.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.Session;
import com.rite.products.convertrite.Validations.Validations;
import com.rite.products.convertrite.exception.BadRequestException;
import com.rite.products.convertrite.exception.ValidationException;
import com.rite.products.convertrite.utils.DataSourceUtil;
import com.rite.products.convertrite.utils.Utils;

@Service
public class XxrSourceServiceImpl implements XxrSourceService {
    private static final Logger log = LoggerFactory.getLogger(XxrSourceServiceImpl.class);

    @Autowired
    SourceTemplateHeadersRepository sourceTemplateHeadersRepository;
    @Autowired
    CrBatchProcessingDetailsRepo crBatchProcessingDetailsRepo;
    @Autowired
    CrSourceTemplateHeadersRepo crSourceTemplateHeadersRepo;
    @Autowired
    SourceTemplateColumnsRepository sourceTemplateColumnsRepository;
    @Autowired
    CrSourceTemplateColumnsRepo crSourceTemplateColumnsRepo;
    @Autowired
    SourceTemplateHeadersDaoImpl sourceTemplateHeadersDaoImpl;
    @Autowired
    XxrSourceTablesRepository xxrSourceTablesRepository;
    @Autowired
    CrSourceTableRepo crSourceTableRepo;
    @Autowired
    XxrSourceColumnsRepository xxrSourceColumnsRepository;
    @Autowired
    CrSourceColumnsRepo crSourceColumnsRepo;
    @Autowired
    SaveSourceTemplateHeaderDaoImpl saveSourceTemplateHeaderDaoImpl;
    @Autowired
    SaveSourceTemplateColumnsDaoImpl saveSourceTemplateColumnsDaoImpl;
    @Autowired
    CreateSourceStagTableDaoImpl createSourceStagTableDaoImpl;
    @Autowired
    CreateSourceDynamicViewDaoImpl createSourceDynamicViewDaoImpl;
    @Autowired
    XxrLookUpValuesRepository xxrLookUpValuesRepository;
    @Autowired
    DataSourceUtil dataSourceUtil;
    @Autowired
    GenerateOrigTransRefDaoImpl generateOrigTransRefDaoImpl;
    @Autowired
    Utils utils;
    @Autowired
    XxrSourceLoadFailRecordsRepository xxrSourceLoadFailRecordsRepository;
    @Autowired
    CrSourceLoadFailRecordsRepo crSourceLoadFailRecordsRepo;
    @Autowired
    CrSourceTemplateHeadersViewRepo crSourceTemplateHeadersViewRepo;
    @Value("${file.upload-dir}")
    String fileUploadDir;
    @Value("${file-transfer}")
    String fileTransfer;
    @Autowired
    DynamicDataSourceBasedMultiTenantConnectionProvider dynamicDataSourceBasedMultiTenantConnectionProvider;

    @Autowired
    CrBatchWiseProcessingDaoImpl batchWiseProcessingDaoImpl;

    @Override
    public List<CrSourceTemplateHeadersView> getSourceTemplates() throws Exception {
        log.info("Start of getSourceTemplates in service -->");
        return crSourceTemplateHeadersViewRepo.findAll();
    }
    /*
     * public List<SourceTemplateHeadersResPo> getSourceTemplates() throws Exception
     * { log.info("Start Of getSourceTemplates Method in Service Layer ####");
     * List<SourceTemplateHeaders> sourceTemplateHeaders = new ArrayList<>();
     * List<SourceTemplateHeadersResPo> sourceTemplateHeaderRes = new ArrayList<>();
     * try {
     *
     * sourceTemplateHeaders = sourceTemplateHeadersRepository.findAll();
     *
     * sourceTemplateHeaders.stream().forEach(x -> { SourceTemplateHeadersResPo
     * sourceTemplateHeadersResPo = new SourceTemplateHeadersResPo(); String
     * metaDataTableName = "";
     *
     * String podName = xxrLookUpValuesRepository.getValueById(x.getPodId()); String
     * projectName = xxrLookUpValuesRepository.getValueById(x.getProjectId());
     * String objectCode = xxrLookUpValuesRepository.getValueById(x.getObjectId());
     * String parentObjectCode =
     * xxrLookUpValuesRepository.getValueById(x.getParentObjectId()); if
     * (x.getMetaDataTableId() != null) { metaDataTableName =
     * xxrSourceTablesRepository.getMetaDataTableName(x.getMetaDataTableId());
     * sourceTemplateHeadersResPo.setMetaDataTableId(x.getMetaDataTableId()); }
     * sourceTemplateHeadersResPo.setTemplateId(x.getTemplateId());
     * sourceTemplateHeadersResPo.setTemplateName(x.getTemplateName());
     * sourceTemplateHeadersResPo.setPodId(x.getPodId());
     * sourceTemplateHeadersResPo.setPodName(podName);
     * sourceTemplateHeadersResPo.setProjectId(x.getProjectId());
     * sourceTemplateHeadersResPo.setProjectName(projectName);
     * sourceTemplateHeadersResPo.setParentObjectId(x.getParentObjectId());
     * sourceTemplateHeadersResPo.setParentObjectCode(parentObjectCode);
     * sourceTemplateHeadersResPo.setObjectId(x.getObjectId());
     * sourceTemplateHeadersResPo.setObjectCode(objectCode); if
     * (!Validations.isNullOrEmpty(metaDataTableName))
     * sourceTemplateHeadersResPo.setMetaDataTableName(metaDataTableName); if
     * (!Validations.isNullOrEmpty(x.getStagingTableName()))
     * sourceTemplateHeadersResPo.setStagingTableName(x.getStagingTableName()); if
     * (!Validations.isNullOrEmpty(x.getViewName()))
     * sourceTemplateHeadersResPo.setViewName(x.getViewName()); if
     * (!Validations.isNullOrEmpty(x.getBu()))
     * sourceTemplateHeadersResPo.setBu(x.getBu()); if
     * (!Validations.isNullOrEmpty(x.getBuSpecific()))
     * sourceTemplateHeadersResPo.setBuSpecific(x.getBuSpecific()); if
     * (!Validations.isNullOrEmpty(x.getAttribute1()))
     * sourceTemplateHeadersResPo.setAttribute1(x.getAttribute1()); if
     * (!Validations.isNullOrEmpty(x.getAttribute2()))
     * sourceTemplateHeadersResPo.setAttribute2(x.getAttribute2()); if
     * (!Validations.isNullOrEmpty(x.getAttribute3()))
     * sourceTemplateHeadersResPo.setAttribute3(x.getAttribute3()); if
     * (!Validations.isNullOrEmpty(x.getAttribute4()))
     * sourceTemplateHeadersResPo.setAttribute4(x.getAttribute4()); if
     * (!Validations.isNullOrEmpty(x.getAttribute5()))
     * sourceTemplateHeadersResPo.setAttribute5(x.getAttribute5());
     *
     * sourceTemplateHeaderRes.add(sourceTemplateHeadersResPo); });
     *
     * } catch (Exception e) { throw new Exception(e.getMessage()); }
     *
     * return sourceTemplateHeaderRes; }
     */

    public List<SourceTemplateHeadersResPo> getSourceTemplateHeaderById(long templateId) throws Exception {

        log.info("Start Of getSourceTemplateHeaderById Method  in Service Layer ####");
        List<SourceTemplateHeaders> sourceTemplateHeaders = new ArrayList<>();
        List<SourceTemplateHeadersResPo> sourceTemplateHeaderRes = new ArrayList<>();
        try {

            sourceTemplateHeaders = sourceTemplateHeadersRepository.findByTemplateId(templateId);

            sourceTemplateHeaders.stream().forEach(x -> {
                SourceTemplateHeadersResPo sourceTemplateHeadersResPo = new SourceTemplateHeadersResPo();
                String metaDataTableName = "";

                // String podName = xxrLookUpValuesRepository.getValueById(x.getPodId());
                String projectName = xxrLookUpValuesRepository.getValueById(x.getProjectId());
                String objectCode = xxrLookUpValuesRepository.getValueById(x.getObjectId());
                String parentObjectCode = xxrLookUpValuesRepository.getValueById(x.getParentObjectId());
                if (x.getMetaDataTableId() != null) {
                    metaDataTableName = xxrSourceTablesRepository.getMetaDataTableName(x.getMetaDataTableId());
                    sourceTemplateHeadersResPo.setMetaDataTableId(x.getMetaDataTableId());
                }
                sourceTemplateHeadersResPo.setTemplateId(x.getTemplateId());
                sourceTemplateHeadersResPo.setTemplateName(x.getTemplateName());
                // sourceTemplateHeadersResPo.setPodId(x.getPodId());
                // sourceTemplateHeadersResPo.setPodName(podName);
                sourceTemplateHeadersResPo.setProjectId(x.getProjectId());
                sourceTemplateHeadersResPo.setProjectName(projectName);
                sourceTemplateHeadersResPo.setParentObjectId(x.getParentObjectId());
                sourceTemplateHeadersResPo.setParentObjectCode(parentObjectCode);
                sourceTemplateHeadersResPo.setObjectId(x.getObjectId());
                sourceTemplateHeadersResPo.setObjectCode(objectCode);
                sourceTemplateHeadersResPo.setNormalizeDataFlag(x.getNormalizeDataFlag());
                sourceTemplateHeadersResPo.setTemplateCode(x.getTemplateCode());
                if (!Validations.isNullOrEmpty(metaDataTableName))
                    sourceTemplateHeadersResPo.setMetaDataTableName(metaDataTableName);
                if (!Validations.isNullOrEmpty(x.getStagingTableName()))
                    sourceTemplateHeadersResPo.setStagingTableName(x.getStagingTableName());
                if (!Validations.isNullOrEmpty(x.getViewName()))
                    sourceTemplateHeadersResPo.setViewName(x.getViewName());
                /*
                 * if (!Validations.isNullOrEmpty(x.getBu()))
                 * sourceTemplateHeadersResPo.setBu(x.getBu()); if
                 * (!Validations.isNullOrEmpty(x.getBuSpecific()))
                 * sourceTemplateHeadersResPo.setBuSpecific(x.getBuSpecific());
                 */
                if (!Validations.isNullOrEmpty(x.getAttribute1()))
                    sourceTemplateHeadersResPo.setAttribute1(x.getAttribute1());
                if (!Validations.isNullOrEmpty(x.getAttribute2()))
                    sourceTemplateHeadersResPo.setAttribute2(x.getAttribute2());
                if (!Validations.isNullOrEmpty(x.getAttribute3()))
                    sourceTemplateHeadersResPo.setAttribute3(x.getAttribute3());
                if (!Validations.isNullOrEmpty(x.getAttribute4()))
                    sourceTemplateHeadersResPo.setAttribute4(x.getAttribute4());
                if (!Validations.isNullOrEmpty(x.getAttribute5()))
                    sourceTemplateHeadersResPo.setAttribute5(x.getAttribute5());

                sourceTemplateHeaderRes.add(sourceTemplateHeadersResPo);
            });

        } catch (Exception e) {
            throw new Exception(e.getMessage());
        }

        return sourceTemplateHeaderRes;

    }

    public List<SourceTemplateHeaders> getSourceTemplatesByPo(SourceTemplatePo sourceTemplatePo) throws Exception {
        log.info("Start Of getSourceTemplatesByPo Method  in Service Layer ####");

        List<SourceTemplateHeaders> sourceTemplateHeaders = new ArrayList<>();
        try {
            sourceTemplateHeaders = sourceTemplateHeadersDaoImpl.getSourceTemplatesByPo(sourceTemplatePo);

        } catch (Exception e) {
            throw new Exception(e.getMessage());
        }

        return sourceTemplateHeaders;
    }

    public List<Object> getSourceTemplateColumns(long templateId) throws Exception {
        log.info("Start Of getSourceTemplateColumns Method  in Service Layer ####");
        List<SourceTemplateColumns> sourceTemplateColumns = new ArrayList<>();
        List<Object> sourceTemplateColumnsRes = new ArrayList<>();
        try {
            //	sourceTemplateColumns = sourceTemplateColumnsRepository.findByTemplateId(templateId);
            List<CrSourceTemplateColumns> crSourceTemplateColumns = crSourceTemplateColumnsRepo.findAllByTemplateId(templateId);
            sourceTemplateColumnsRes.add(crSourceTemplateColumns);

        } catch (Exception e) {
            throw new Exception(e.getMessage());
        }
        return sourceTemplateColumnsRes;
    }

    public List<CrColumnPo> getSourceColumnsByName(String sourceTableName) throws Exception {
        log.info("Start Of getSourceColumnsByName Method  in Service Layer ####");
        List<CrColumnPo> sourceTemplateColumns = new ArrayList<>();

        try {
            //	Long tableId = xxrSourceTablesRepository.getTableId(sourceTableName);
            Long tableId = crSourceTableRepo.getTableId(sourceTableName);

            log.info("tableId::::::::" + tableId);

            if (tableId != null)
                //	sourceTemplateColumns = xxrSourceColumnsRepository.getSourceColumnsById(tableId);
                sourceTemplateColumns = crSourceColumnsRepo.getSourceColumnsById(tableId);

        } catch (Exception e) {
            throw new Exception(e.getMessage());
        }
        return sourceTemplateColumns;
    }

    public List<ColumnPo> getSourceColumnsById(long sourceTableId) throws Exception {
        log.info("Start Of getSourceColumnsById Method  in Service Layer ####");
        List<ColumnPo> sourceTemplateColumns = new ArrayList<>();

        try {
            sourceTemplateColumns = xxrSourceColumnsRepository.getSourceColumnsById(sourceTableId);
        } catch (Exception e) {
            throw new Exception(e.getMessage());
        }

        return sourceTemplateColumns;
    }

    public SaveTemplateHeaderResponsePo saveSourceTemplateHeaders(
            CrSourceTemplateHeaders sourceTemplateHeadersPo, HttpServletRequest request)
            throws ValidationException, Exception {
        log.info("Start of saveSourceTemplateHeaders in Service Layer ###");
        String msg = "";
        long templateId = 0;
        SaveTemplateHeaderResponsePo saveTemplateHeaderResponsePo = new SaveTemplateHeaderResponsePo();
        try {
            //msg = saveSourceTemplateHeaderDaoImpl.saveSourceTemplateHeaders(sourceTemplateHeadersPo, request);
            try {
                System.out.println("sourceTemplateHeadersPo-->" + sourceTemplateHeadersPo);
                sourceTemplateHeadersPo.setCreatedBy("CoreUser");
                sourceTemplateHeadersPo.setCreationDate(new Date());
                CrSourceTemplateHeaders crSourceTemplateHeader = crSourceTemplateHeadersRepo.save(sourceTemplateHeadersPo);
                if (crSourceTemplateHeader != null) {
                    msg = "Source Template successfully saved";

                    saveTemplateHeaderResponsePo.setTemplateId(crSourceTemplateHeader.getTemplateId());
                    saveTemplateHeaderResponsePo.setTemplateName(crSourceTemplateHeader.getTemplateName());
                }
            } catch (NumberFormatException e) {
                log.error("This is not a number");
                log.error(e.getMessage());
            } catch (Exception e) {
                msg = "Exception while saving data";
                log.error("Exception in saveSorceTemplateHeaders->" + e);
            }
            saveTemplateHeaderResponsePo.setMessage(msg);
//			SaveSourceTemplateHeadersPo saveSourceTemplateHeadersPo = sourceTemplateHeadersPo.get(0);
//			String templateName = saveSourceTemplateHeadersPo.getTemplateName();
//			if (!Validations.isNullOrEmpty(templateName)) {
//				templateId = sourceTemplateHeadersRepository.getTemplateId(templateName);
//				saveTemplateHeaderResponsePo.setTemplateId(templateId);
//				saveTemplateHeaderResponsePo.setTemplateName(templateName);
//			}
            log.info(msg + "msg###########");
        } catch (NumberFormatException e) {
            log.error("This is not a number");
            log.error(e.getMessage());
        } catch (Exception e) {
            log.error("Exception in saveSorceTemplateHeaders->" + e);
            throw new Exception(e.getMessage());
        }
        return saveTemplateHeaderResponsePo;
    }

    public SaveSourceTemplateColumnsResPo saveSourceTemplateColumns(
            List<CrSourceTemplateColumns> sourceTemplateColumnsPo, HttpServletRequest request)
            throws BadRequestException, Exception {
        log.info("Start of saveSourceTemplateColumns in Service Layer ###");
        String msg = "";
        List<SourceTemplateColumns> sourceTemplateColumns = new ArrayList<>();
        SaveSourceTemplateColumnsResPo saveSourceTemplateColumnsResPo = new SaveSourceTemplateColumnsResPo();
        List<CrSourceTemplateColumns> res = null;
        for (CrSourceTemplateColumns sourceTemplateColumnPo : sourceTemplateColumnsPo) {
            sourceTemplateColumnPo.setCreatedBy("CoreUser");
            sourceTemplateColumnPo.setCreationDate(new Date());
        }
        try {
            //	msg = saveSourceTemplateColumnsDaoImpl.saveSourceTemplateColumns(sourceTemplateColumnsPo, request);
            res = crSourceTemplateColumnsRepo.saveAll(sourceTemplateColumnsPo);

            if (res.size() > 0) {
                msg = "Source Template Columns Data Saved Successfully";
                saveSourceTemplateColumnsResPo.setMessage(msg);
                //Long templateId = sourceTemplateColumnsPo.get(0).getTemplateId();
                //sourceTemplateColumns = sourceTemplateColumnsRepository.findByTemplateId(templateId);
                saveSourceTemplateColumnsResPo.setSourceTemplateColumns(res);
            } else {
                msg = "Save Source Template Columns Data Failed";
            }
            log.info(msg + "msg###########");
        } catch (Exception e) {
            throw new Exception(e.getMessage());
        }

        return saveSourceTemplateColumnsResPo;

    }

    @Override
    public Object loadSourceTemplateMetaData(String type, String fileName, Long objectId, HttpServletRequest request) {
        String userId = request.getHeader("userId");
        Object res = createSourceStagTableDaoImpl.loadSourceMetaData(type, fileName, userId, objectId);
        //String res=crSourceTableRepo.CR_LOAD_METADATA_PROC(type,fileName,userId,objectId);
        return res;
    }

    @Override
    public BasicResPo loadSourceData(String dataFileName, String batchName, Long templateId, String templateName, String userId) {
        return createSourceStagTableDaoImpl.loadSourceData(dataFileName, batchName, templateId, templateName, userId);
    }

    public SourceStagingTablePo createSourceStaggingTab(Long tableId, Long templateId, String templateCode, String environment, HttpServletRequest request) throws Exception {

        log.info("Start Of createSourceStaggingTab Method in Service Layer ####");
        SourceStagingTablePo stagingPo = new SourceStagingTablePo();
        try {
            String userId = request.getHeader("userId");
            stagingPo = createSourceStagTableDaoImpl.createStaggingTable(tableId, templateId, templateCode, environment, userId);
            //String res= crSourceTableRepo.CR_CREATE_STG_TABLE_PROC(tableId, templateId,templateCode,environment, userId);
            //	crSourceTableRepo.test(tableId, templateId,templateCode,environment, userId);
            System.out.println("res---->" + stagingPo);
        } catch (Exception e) {
            throw new Exception(e.getMessage());
        }

        return stagingPo;

    }

    public CreateDynamicViewPo createDynamicView(Long templateId, String stgTableName, HttpServletRequest request)
            throws Exception {
        log.info("Start Of createDynamicView Method in Service Layer ####");
        CreateDynamicViewPo createDynamicViewPo = new CreateDynamicViewPo();
        try {
            createDynamicViewPo = createSourceDynamicViewDaoImpl.createDynamicView(templateId, stgTableName, request);
        } catch (Exception e) {
            throw new Exception(e.getMessage());
        }

        return createDynamicViewPo;

    }

    @Override
    public List<SourceTablesPo> getSourceTableNames(Long objectId) throws Exception {
        log.info("Start Of getSourceTableNames Method in Service Layer ####");
        List<SourceTablesPo> sourceTableNames = new ArrayList<>();
        try {
            //sourceTableNames = xxrSourceTablesRepository.getTableIdNames(objectId);
            sourceTableNames = crSourceTableRepo.findAllByObjectId(objectId);
        } catch (Exception e) {
            throw new Exception(e.getMessage());
        }
        return sourceTableNames;

    }

    @Override
    public SaveTemplateHeaderResponsePo copySourceTemplate(String newTemplateName, Long templateId,
                                                           HttpServletRequest request) throws Exception {
        log.info("Start Of copySourceTemplate Method in Service Layer ####");
        List<SaveSourceTemplatesColumnsPo> sourceTemplateColumnsPo = new ArrayList<>();
        List<SaveSourceTemplateHeadersPo> sourceTemplateHeadersPo = new ArrayList<>();
        SaveSourceTemplateHeadersPo saveSourceTemplateHeadersPo = new SaveSourceTemplateHeadersPo();
        SaveTemplateHeaderResponsePo saveTemplateHeaderResponsePo = new SaveTemplateHeaderResponsePo();
        String msg = "";
        Long newTemplateId = null;
        String metaDataTableName = "";
        SourceStagingTablePo stagingPo = new SourceStagingTablePo();
        CreateDynamicViewPo createDynamicViewPo = new CreateDynamicViewPo();
        List<SourceTemplateColumns> sourceTemplateColumns = new ArrayList<>();
        try {

            SourceTemplateHeaders tempHeader = sourceTemplateHeadersRepository.findAll().stream()
                    .filter(x -> x.getTemplateId() == templateId).findFirst().get();

            saveSourceTemplateHeadersPo.setTemplateName(newTemplateName);
            saveSourceTemplateHeadersPo.setMetaDataTableId(tempHeader.getMetaDataTableId());
            saveSourceTemplateHeadersPo.setObjectId(tempHeader.getObjectId());
            saveSourceTemplateHeadersPo.setParentObjectId(tempHeader.getParentObjectId());
            saveSourceTemplateHeadersPo.setProjectId(tempHeader.getProjectId());

            sourceTemplateHeadersPo.add(saveSourceTemplateHeadersPo);

            msg = saveSourceTemplateHeaderDaoImpl.saveSourceTemplateHeaders(sourceTemplateHeadersPo, request);
            log.info("Source :::: " + msg);

            if (!Validations.isNullOrEmpty(newTemplateName))
                newTemplateId = sourceTemplateHeadersRepository.getTemplateId(newTemplateName);

            if (newTemplateId != null) {
                final long nTemplateId = newTemplateId;
                SourceTemplateHeaders srctempHeader = sourceTemplateHeadersRepository.findAll().stream()
                        .filter(x -> x.getTemplateId() == nTemplateId).findFirst().get();

                log.info("tableId:::::::" + srctempHeader.getMetaDataTableId());

                sourceTemplateColumns = sourceTemplateColumnsRepository.findByTemplateId(templateId);

                for (SourceTemplateColumns sourceTemplateColumn : sourceTemplateColumns) {

                    SaveSourceTemplatesColumnsPo saveSourceTemplatesColumnsPo = new SaveSourceTemplatesColumnsPo();

                    saveSourceTemplatesColumnsPo.setColumnName(sourceTemplateColumn.getColumnName());
                    saveSourceTemplatesColumnsPo.setDisplaySeq(sourceTemplateColumn.getSeq());
                    /*
                     * saveSourceTemplatesColumnsPo.setEnableFlag(sourceTemplateColumn.
                     * getEnabledFlag());
                     * saveSourceTemplatesColumnsPo.setStartDate(sourceTemplateColumn.getStartDate()
                     * );
                     * saveSourceTemplatesColumnsPo.setEndDate(sourceTemplateColumn.getEndDate());
                     * saveSourceTemplatesColumnsPo.setMappingSetId(sourceTemplateColumn.
                     * getMappingSetId());
                     * saveSourceTemplatesColumnsPo.setMappingValue(sourceTemplateColumn.
                     * getMappingValue());
                     * saveSourceTemplatesColumnsPo.setMappingType(sourceTemplateColumn.
                     * getMappingType());
                     */
                    saveSourceTemplatesColumnsPo.setOrigTransRef(sourceTemplateColumn.getOrigTransRef());
                    saveSourceTemplatesColumnsPo.setSelected(sourceTemplateColumn.getSelected());
                    if (!Validations.isNullOrEmpty((sourceTemplateColumn.getColumnType())))
                        saveSourceTemplatesColumnsPo.setColumnType(sourceTemplateColumn.getColumnType());
                    /*
                     * if (!Validations.isNullOrEmpty((sourceTemplateColumn.getNullAllowedFlag())))
                     * saveSourceTemplatesColumnsPo.setNullAllowedFlag(sourceTemplateColumn.
                     * getNullAllowedFlag());
                     */
                    if (sourceTemplateColumn.getWidth() != null)
                        saveSourceTemplatesColumnsPo.setWidth(sourceTemplateColumn.getWidth());
                    saveSourceTemplatesColumnsPo.setTemplateId(newTemplateId);

                    sourceTemplateColumnsPo.add(saveSourceTemplatesColumnsPo);
                }

                msg = saveSourceTemplateColumnsDaoImpl.saveSourceTemplateColumns(sourceTemplateColumnsPo, request);

                log.info("source::: " + msg);

                if (srctempHeader.getMetaDataTableId() != null)
                    metaDataTableName = xxrSourceTablesRepository
                            .getMetaDataTableName(srctempHeader.getMetaDataTableId());

                /*
                 * stagingPo =
                 * createSourceStagTableDaoImpl.createStaggingTable(metaDataTableName,
                 * newTemplateId, podId, request);
                 */

                log.info("Source staging Table::::: " + stagingPo.getResult() + "stagingTableName :: "
                        + stagingPo.getTableName());

                if (stagingPo != null && !(stagingPo.getResult().isBlank())
                        && stagingPo.getResult().equalsIgnoreCase("Y")) {
                    String stagingTableName = stagingPo.getTableName().split(":")[1];
                    // srctempHeader.setStagingTableName(stagingTableName);

                    createDynamicViewPo = createSourceDynamicViewDaoImpl.createDynamicView(newTemplateId,
                            stagingTableName, request);
                    log.info("Source Dynamic view ::::: " + createDynamicViewPo.getResult() + "DynamicView :: "
                            + createDynamicViewPo.getViewName());

                    /*
                     * if (createDynamicViewPo != null &&
                     * !(createDynamicViewPo.getResult().isBlank()) &&
                     * createDynamicViewPo.getResult().equalsIgnoreCase("Y"))
                     * srctempHeader.setViewName(createDynamicViewPo.getViewName());
                     *
                     * sourceTemplateHeadersRepository.save(srctempHeader);
                     */
                }

                saveTemplateHeaderResponsePo.setTemplateId(newTemplateId);
                saveTemplateHeaderResponsePo.setTemplateName(newTemplateName);
                saveTemplateHeaderResponsePo.setMessage("SourceTemplate Copied successfully");
            }

        } catch (Exception e) {
            throw new Exception(e.getMessage());
        }
        return saveTemplateHeaderResponsePo;
    }

    @Override
    public BasicResPo loadSourceData(LoadSourceDataReqPo loadSourceDataReqPo, String tenantId,String userId)
            throws Exception {
        log.info("Start of loadSourceData ##########");
        BasicResPo res = new BasicResPo();
        Connection con = null;
        String executeQuery = null;
        String extrnlTableName = "";
        Session jschSession = null;
        ChannelSftp channelSftp = null;
        InputStream inputStream = null;

        LoadSourceDataResPo sourceDataResPo = new LoadSourceDataResPo();
        sourceDataResPo.setStgTableName(loadSourceDataReqPo.getSrcStgTableName());
        sourceDataResPo.setLoadedRecords(0);
        sourceDataResPo.setFailedRecords(0);
        try {
            if (loadSourceDataReqPo.getBatchName().toLowerCase().indexOf("cr20") != -1)
                throw new ValidationException("Please don't use CR20 value in batchName");
            log.info("TENANT-->" +tenantId);
            con = dynamicDataSourceBasedMultiTenantConnectionProvider.getConnection(tenantId);

            PreparedStatement countStmnt = con
                    .prepareStatement("select count(*) from " + loadSourceDataReqPo.getSrcStgTableName()
                            + " where cr_batch_name='" + loadSourceDataReqPo.getBatchName() + "'");

            ResultSet rs = countStmnt.executeQuery();
            int count = 0;
            if (rs.next()) {
                count = rs.getInt("count(*)");
                log.info("########" + rs.getInt("count(*)"));
            }
            countStmnt.close();
            boolean existingBatchDetails = crBatchProcessingDetailsRepo.existsBySrcTempIdAndBatchName(
                    loadSourceDataReqPo.getSrcTemplateId(),
                    loadSourceDataReqPo.getBatchName()
            );

            if (count > 0 || existingBatchDetails == true) {
                log.error("Given batch name is already used. It should be unique for a selected template");
                return new BasicResPo() {{
                    setStatusCode(HttpStatus.INTERNAL_SERVER_ERROR);
                    setStatus("error");
                    setMessage("Given batch name is already used. It should be unique for a selected template");
                    setPayload(sourceDataResPo);
                }};
            }

            MetaDataColumnsPo metaDataColumnsPo = getMetaDataAndColumnNames(loadSourceDataReqPo.getMetaDataTableId());
            log.info("metaDataColumnsPo-->" + metaDataColumnsPo);
            /*
             * UUID uuid = UUID.randomUUID(); String correlationId = uuid.toString();
             */
            String fileName = loadSourceDataReqPo.getFileName();
            // Generate a timestamp without special characters
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd_HHmmss");
            String timestamp = dateFormat.format(new Date()).replaceAll("[^a-zA-Z0-9]", "");
            extrnlTableName = loadSourceDataReqPo.getSrcStgTableName() +timestamp+ "_EXT";
            String externalSql = "CREATE TABLE " + extrnlTableName + " (" + metaDataColumnsPo.getMetaDataStr() + " )"
                    + "ORGANIZATION EXTERNAL\r\n" + "(TYPE ORACLE_LOADER\r\n" + "DEFAULT DIRECTORY G2N_TAB_MAIN\r\n"
                    + "ACCESS PARAMETERS\r\n" + "(\r\n" + "records delimited by newline\r\n"
                    + "LOGFILE G2N_TAB_MAIN:'" + extrnlTableName + ".log'\r\n" + "BADFILE G2N_TAB_MAIN:'"
                    + extrnlTableName + ".bad'\r\n" + "skip 1\r\n"
                    + "fields terminated by ',' optionally enclosed BY '\"' LDRTRIM\r\n"
                    + "missing field values are null\r\n" + ")\r\n" + "LOCATION ('" + loadSourceDataReqPo.getFileName()
                    + "')\r\n" + ")  REJECT LIMIT UNLIMITED";
            log.info(externalSql);
            PreparedStatement extrnlStmnt = con.prepareStatement(externalSql);
            int extrnlCount = extrnlStmnt.executeUpdate();
            log.info(extrnlCount + "########count");
            extrnlStmnt.close();

            executeQuery = "insert into " + loadSourceDataReqPo.getSrcStgTableName() + "  SELECT '"
                    + loadSourceDataReqPo.getSrcTemplateId() + "',NULL,NULL,ROWNUM,b.*,ROWNUM,'"
                    + loadSourceDataReqPo.getBatchName() + "' FROM (SELECT " + metaDataColumnsPo.getColumnNames()
                    + " FROM " + extrnlTableName + ") b";
            log.info(executeQuery + ":::::::Insert executeQuery");

            PreparedStatement extStmnt = con.prepareStatement(executeQuery);
            int insertcount = extStmnt.executeUpdate();
            log.info("insertcount########" + insertcount);
            extStmnt.close();
            generateOrigTransRefDaoImpl.generateOrigTranRef(loadSourceDataReqPo.getSrcTemplateId(),
                    loadSourceDataReqPo.getSrcStgTableName(), userId, loadSourceDataReqPo.getBatchName());
            CrLoadDataResPo crLoadDataResPo = new CrLoadDataResPo();

            if (loadSourceDataReqPo.getBatchSize() != null && loadSourceDataReqPo.getBatchSize() > 0) {
                crLoadDataResPo = batchWiseProcessingDaoImpl.srcDataBatchProcessing(loadSourceDataReqPo.getBatchName(), loadSourceDataReqPo.getBatchSize(), loadSourceDataReqPo.getParentStgTableName(), loadSourceDataReqPo.getParentColumn(), loadSourceDataReqPo.getSrcStgTableName());
            }

            if ("SFTP".equalsIgnoreCase(fileTransfer)) {
                jschSession = utils.setupJschSession();
                channelSftp = (ChannelSftp) jschSession.openChannel("sftp");
                channelSftp.connect();
                channelSftp.cd(fileUploadDir);
            }
            try {
                if ("SFTP".equalsIgnoreCase(fileTransfer)) {
                    inputStream = channelSftp.get(extrnlTableName + ".bad");
                } else if ("NFS".equalsIgnoreCase(fileTransfer)) {
                    inputStream = new FileInputStream(fileUploadDir + extrnlTableName + ".bad");
                }

            } catch (Exception e) {
                log.error("File Not Found --->"+extrnlTableName + ".bad");
                log.error("File Not Found --->"+e);
            }
            long failedCount = 0;
            String result = null;
            String logFileText = null;
            if (inputStream != null) {
                if ("SFTP".equalsIgnoreCase(fileTransfer)) {
                    failedCount = new BufferedReader(new InputStreamReader(channelSftp.get(extrnlTableName + ".bad")))
                            .lines().count();
                    result = new BufferedReader(new InputStreamReader(channelSftp.get(extrnlTableName + ".bad")))
                            .lines().collect(Collectors.joining("\n"));
                    logFileText = new BufferedReader(new InputStreamReader(channelSftp.get(extrnlTableName + ".log")))
                            .lines().collect(Collectors.joining("\n"));
                } else if ("NFS".equalsIgnoreCase(fileTransfer)) {
                    failedCount = new BufferedReader(new InputStreamReader(new FileInputStream(fileUploadDir + extrnlTableName + ".bad")))
                            .lines().count();
                    result = new BufferedReader(new InputStreamReader(new FileInputStream(fileUploadDir + extrnlTableName + ".bad")))
                            .lines().collect(Collectors.joining("\n"));
                    logFileText = new BufferedReader(new InputStreamReader(new FileInputStream(fileUploadDir + extrnlTableName + ".bad")))
                            .lines().collect(Collectors.joining("\n"));
                }


                CrSourceLoadFailRecords crSourceLoadFailRecords = new CrSourceLoadFailRecords();
                crSourceLoadFailRecords.setTemplateId(loadSourceDataReqPo.getSrcTemplateId());
                crSourceLoadFailRecords.setFileName(fileName);
                crSourceLoadFailRecords.setFailed(failedCount);
                crSourceLoadFailRecords.setCrBatchName(loadSourceDataReqPo.getBatchName());
                crSourceLoadFailRecords.setSuccess(insertcount);
                crSourceLoadFailRecords.setFailedClob(result);
                crSourceLoadFailRecords.setCreatedBy("ConvertRite");
                crSourceLoadFailRecords.setCreationDate(new java.sql.Date(new java.util.Date().getTime()));
                crSourceLoadFailRecords.setLastUpdateBy("ConvertRite");
                crSourceLoadFailRecords.setLastUpdatedDate(new java.sql.Date(new java.util.Date().getTime()));
                crSourceLoadFailRecords.setLogFileBlob(logFileText);

                crSourceLoadFailRecordsRepo.save(crSourceLoadFailRecords);
                if("SFTP".equalsIgnoreCase(fileTransfer)) {
                    channelSftp.rm(extrnlTableName + ".bad");
                }else if("NFS".equalsIgnoreCase(fileTransfer)){
                    //    Path filePath = Paths.get(fileUploadDir+fileName);
                    File fileToDelete = new File(fileUploadDir + extrnlTableName + ".bad");
                    try {
                        boolean isFileDeleted = fileToDelete.delete();
                        log.error("Bad deleted or not:"+isFileDeleted);
                    } catch (Exception e) {
                        log.error(e.getMessage());
                        e.printStackTrace();
                    }
                }
            }

            PreparedStatement dropStmnt = con.prepareStatement("DROP TABLE " + extrnlTableName);
            int tableDeleted = dropStmnt.executeUpdate();
            log.info(tableDeleted + "::::::external table Deleted");
            dropStmnt.close();

            String strMessage = "";
            if (failedCount == 0) {
                strMessage = "Successfully Loaded Data into Source Staging table";
            } else {
                strMessage = failedCount + " records failed loading into source staging table";
            }
            String finalStrMessage = strMessage;
            sourceDataResPo.setLoadedRecords(insertcount);
            sourceDataResPo.setFailedRecords(failedCount);
            sourceDataResPo.setBatchNames(crLoadDataResPo.getBatchNames());
            sourceDataResPo.setPresult(crLoadDataResPo.getPresult());
            sourceDataResPo.setDuplicateRecCount(crLoadDataResPo.getDuplicateRecCount());
            res = new BasicResPo() {{
                setStatusCode(HttpStatus.OK);
                setStatus("success");
                setMessage(finalStrMessage);
                setPayload(sourceDataResPo);
            }};

        } catch (Exception e) {
            log.error(e.getMessage());
            e.printStackTrace();
            PreparedStatement dropStmnt = null;
            if (!Validations.isNullOrEmpty(extrnlTableName)) {
                dropStmnt = con.prepareStatement("DROP TABLE " + extrnlTableName);
                int tableDeleted = dropStmnt.executeUpdate();
                log.info(tableDeleted + "::::::tableDeleted");
                dropStmnt.close();
            }
            res = new BasicResPo() {{
                setStatusCode(HttpStatus.INTERNAL_SERVER_ERROR);
                setStatus("error");
                setMessage(e.getMessage());
                setPayload(sourceDataResPo);
            }};
        } finally {
            if (channelSftp != null) {
                channelSftp.exit();
                channelSftp.disconnect();
            }
            if (jschSession != null)
                jschSession.disconnect();
            if (con != null)
                con.close();
        }
        return res;
    }

    private MetaDataColumnsPo getMetaDataAndColumnNames(Long metaDataTableId) {
        MetaDataColumnsPo metaDataColumnsPo = new MetaDataColumnsPo();

        List<CrSourceColumns> sourceColumnsLi = crSourceColumnsRepo
                .findAllByTableId(metaDataTableId);
        String columnName = "";
        String columnType = "";
        Long width = null;

        // Sort the list based on the columnSequence
        List<CrSourceColumns> sortedSourceColumnsLi = sourceColumnsLi.stream()
                .sorted(Comparator.comparing(CrSourceColumns::getColumnSequence))
                .collect(Collectors.toList());
        int size = sortedSourceColumnsLi.size();
        log.info("sourceColumnsLi-->" + sortedSourceColumnsLi);
        log.info("sourceColumnsLi-->" + size);

        StringBuffer metaData = new StringBuffer();
        StringBuffer columnNames = new StringBuffer();
        for (int i = 0; i < size; i++) {
            columnName = sortedSourceColumnsLi.get(i).getColumnName();
            log.debug(i + "-columnName-->" + columnName);
            columnType = sortedSourceColumnsLi.get(i).getColumnType();
            width = sortedSourceColumnsLi.get(i).getWidth();
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

    @Override
    public void downloadFailedRecLogFile(Long failRecId, HttpServletResponse resp) throws Exception {
        log.info("Start of downloadFailedRecLogFile########");
        resp.setContentType("text/plain");
        List<CrSourceTemplateHeaders> sourceTemplateHdrList = new ArrayList<>();
        CrSourceTemplateHeaders sourceTemplateHeader = new CrSourceTemplateHeaders();
        Optional<CrSourceLoadFailRecords> sourceLoadFailedRec = crSourceLoadFailRecordsRepo
                .findById(failRecId);

        if (sourceLoadFailedRec.isPresent()) {
            CrSourceLoadFailRecords crSourceLoadFailRecords = sourceLoadFailedRec.get();
            sourceTemplateHdrList = crSourceTemplateHeadersRepo
                    .findAllById(Collections.singleton(crSourceLoadFailRecords.getTemplateId()));

            if (sourceTemplateHdrList != null && !sourceTemplateHdrList.isEmpty())
                sourceTemplateHeader = sourceTemplateHdrList.get(0);

            resp.setHeader(HttpHeaders.CONTENT_DISPOSITION,
                    "attachment; filename=" + sourceTemplateHeader.getTemplateName() + ".log");
            IOUtils.copy(new ByteArrayInputStream(crSourceLoadFailRecords.getLogFileBlob().getBytes()), resp.getOutputStream());
        } else {
            throw new Exception("No Data Found");
        }
    }

    public SavingSourceTemplateHeadersResPo saveSourceTemplateHeadersWithoutProc(
            List<SaveSourceTemplateHeadersPo> sourceTemplateHeadersPo, HttpServletRequest request)
            throws ValidationException, Exception {
        log.info("Start of saveSourceTemplateHeaders in Service Layer ###");
        SavingSourceTemplateHeadersResPo savingSourceTemplateHeadersPo = new SavingSourceTemplateHeadersResPo();
        SourceTemplateHeaders sourceTemplateHeader = new SourceTemplateHeaders();
        List<SourceTemplateHeaders> sourceTemplateHeadersList = new ArrayList<>();
        SourceTemplateHeaders source;
        SourceTemplateHeaders sourceTemplateHeaderRes = new SourceTemplateHeaders();
        for (SaveSourceTemplateHeadersPo saveSourceTemplateHeaderPo : sourceTemplateHeadersPo) {
            source = sourceTemplateHeadersRepository
                    .getSourceTemplateHeader(saveSourceTemplateHeaderPo.getTemplateId());
            if (source != null) {
                // source.setPodId(saveSourceTemplateHeaderPo.getPodId());
                // source.setBu(saveSourceTemplateHeaderPo.getBu());
                // source.setBuSpecific(saveSourceTemplateHeaderPo.getBuSpecific());
                source.setObjectId(saveSourceTemplateHeaderPo.getObjectId());
                source.setParentObjectId(saveSourceTemplateHeaderPo.getParentObjectId());
                source.setTemplateName(saveSourceTemplateHeaderPo.getTemplateName());
                source.setProjectId(saveSourceTemplateHeaderPo.getProjectId());
                source.setMetaDataTableId(saveSourceTemplateHeaderPo.getMetaDataTableId());
                source.setStagingTableName(saveSourceTemplateHeaderPo.getStagingTableName());
                source.setViewName(saveSourceTemplateHeaderPo.getViewName());
                source.setNormalizeDataFlag(saveSourceTemplateHeaderPo.getNormalizeDataFlag());
                source.setTemplateCode(saveSourceTemplateHeaderPo.getTemplateCode());
                source.setAttribute1(saveSourceTemplateHeaderPo.getAttribute1());
                source.setAttribute2(saveSourceTemplateHeaderPo.getAttribute2());
                source.setAttribute3(saveSourceTemplateHeaderPo.getAttribute3());
                source.setAttribute4(saveSourceTemplateHeaderPo.getAttribute4());
                source.setAttribute5(saveSourceTemplateHeaderPo.getAttribute5());
                source.setLastUpdatedBy("ConvertRite");
                source.setLastUpdatedDate(new java.sql.Date(new java.util.Date().getTime()));
                sourceTemplateHeaderRes = sourceTemplateHeadersRepository.save(source);
                sourceTemplateHeadersList.add(sourceTemplateHeaderRes);
            } else {
                sourceTemplateHeader.setTemplateName(saveSourceTemplateHeaderPo.getTemplateName());
                sourceTemplateHeader.setObjectId(saveSourceTemplateHeaderPo.getObjectId());
                sourceTemplateHeader.setParentObjectId(saveSourceTemplateHeaderPo.getParentObjectId());
                // sourceTemplateHeader.setPodId(saveSourceTemplateHeaderPo.getPodId());
                sourceTemplateHeader.setProjectId(saveSourceTemplateHeaderPo.getProjectId());
                sourceTemplateHeader.setMetaDataTableId(saveSourceTemplateHeaderPo.getMetaDataTableId());
                sourceTemplateHeader.setStagingTableName(saveSourceTemplateHeaderPo.getStagingTableName());
                sourceTemplateHeader.setViewName(saveSourceTemplateHeaderPo.getViewName());
                // sourceTemplateHeader.setBu(saveSourceTemplateHeaderPo.getBu());
                // sourceTemplateHeader.setBuSpecific(saveSourceTemplateHeaderPo.getBuSpecific());
                sourceTemplateHeader.setNormalizeDataFlag(saveSourceTemplateHeaderPo.getNormalizeDataFlag());
                sourceTemplateHeader.setTemplateCode(saveSourceTemplateHeaderPo.getTemplateCode());
                sourceTemplateHeader.setAttribute1(saveSourceTemplateHeaderPo.getAttribute1());
                sourceTemplateHeader.setAttribute2(saveSourceTemplateHeaderPo.getAttribute2());
                sourceTemplateHeader.setAttribute3(saveSourceTemplateHeaderPo.getAttribute3());
                sourceTemplateHeader.setAttribute4(saveSourceTemplateHeaderPo.getAttribute4());
                sourceTemplateHeader.setAttribute5(saveSourceTemplateHeaderPo.getAttribute5());
                sourceTemplateHeader.setLastUpdatedBy("ConvertRite");
                sourceTemplateHeader.setLastUpdatedDate(new java.sql.Date(new java.util.Date().getTime()));
                sourceTemplateHeader.setCreatedBy("ConvertRite");
                sourceTemplateHeader.setCreationDate(new java.sql.Date(new java.util.Date().getTime()));
                sourceTemplateHeaderRes = sourceTemplateHeadersRepository.save(sourceTemplateHeader);
                sourceTemplateHeadersList.add(sourceTemplateHeaderRes);
            }
        }
        savingSourceTemplateHeadersPo.setSourceTemplateHeaders(sourceTemplateHeadersList);
        savingSourceTemplateHeadersPo.setMessage("Successfully saved/updated SourceTemplateHeaders");
        return savingSourceTemplateHeadersPo;
    }

    @Override
    public SaveSourceTemplateColumnsResPo saveSourceTemplateColumnsWithJpa(
            List<SaveSourceTemplatesColumnsPo> sourceTemplateColumnsPo)
            throws BadRequestException, ValidationException {
        log.info("Start of saveSourceTemplateColumnsWithJpa ######");
        SaveSourceTemplateColumnsResPo saveSourceTemplateColumnsResPo = new SaveSourceTemplateColumnsResPo();
        List<SourceTemplateColumns> srcTemplateColumnsLi = new ArrayList<>();
        for (SaveSourceTemplatesColumnsPo saveSourceTemplatesColumnsPo : sourceTemplateColumnsPo) {
            if (Validations.isNullOrEmpty(saveSourceTemplatesColumnsPo.getColumnName())
                    || saveSourceTemplatesColumnsPo.getTemplateId() == null)
                throw new BadRequestException("templateId and columnName are Mandatory fields");
            if (saveSourceTemplatesColumnsPo.getColumnId() == null) {
                SourceTemplateColumns sourceTemplateColumns = new SourceTemplateColumns();
                sourceTemplateColumns.setColumnName(saveSourceTemplatesColumnsPo.getColumnName());
                sourceTemplateColumns.setColumnType(saveSourceTemplatesColumnsPo.getColumnType());
                /*
                 * sourceTemplateColumns.setEndDate(saveSourceTemplatesColumnsPo.getEndDate());
                 * sourceTemplateColumns.setEnabledFlag(saveSourceTemplatesColumnsPo.
                 * getEnableFlag());
                 * sourceTemplateColumns.setMappingSetId(saveSourceTemplatesColumnsPo.
                 * getMappingSetId());
                 * sourceTemplateColumns.setMappingType(saveSourceTemplatesColumnsPo.
                 * getMappingType());
                 * sourceTemplateColumns.setMappingValue(saveSourceTemplatesColumnsPo.
                 * getMappingValue());
                 * sourceTemplateColumns.setNullAllowedFlag(saveSourceTemplatesColumnsPo.
                 * getNullAllowedFlag());
                 */
                sourceTemplateColumns.setWidth(saveSourceTemplatesColumnsPo.getWidth());
                sourceTemplateColumns.setOrigTransRef(saveSourceTemplatesColumnsPo.getOrigTransRef());
                sourceTemplateColumns.setSelected(saveSourceTemplatesColumnsPo.getSelected());
                sourceTemplateColumns.setSeq(saveSourceTemplatesColumnsPo.getDisplaySeq());
                /*
                 * sourceTemplateColumns.setStartDate(saveSourceTemplatesColumnsPo.getStartDate(
                 * ));
                 */
                sourceTemplateColumns.setTemplateId(saveSourceTemplatesColumnsPo.getTemplateId());
                sourceTemplateColumns.setLastUpdatedBy("ConvertRite");
                sourceTemplateColumns.setLastUpdatedDate(new java.sql.Date(new java.util.Date().getTime()));
                sourceTemplateColumns.setCreationDate(new java.sql.Date(new java.util.Date().getTime()));
                sourceTemplateColumns.setLastUpdatedBy("ConvertRite");

                srcTemplateColumnsLi.add(sourceTemplateColumns);
            } else {
                Optional<SourceTemplateColumns> srcTempColsOpt = sourceTemplateColumnsRepository
                        .findById(saveSourceTemplatesColumnsPo.getColumnId());
                if (!srcTempColsOpt.isPresent())
                    throw new ValidationException("There is no Column with provided ColumnId");
                SourceTemplateColumns sourceTemplateColumn = srcTempColsOpt.get();
                sourceTemplateColumn.setColumnName(saveSourceTemplatesColumnsPo.getColumnName());
                sourceTemplateColumn.setColumnType(saveSourceTemplatesColumnsPo.getColumnType());
                /*
                 * sourceTemplateColumn.setEndDate(saveSourceTemplatesColumnsPo.getEndDate());
                 * sourceTemplateColumn.setEnabledFlag(saveSourceTemplatesColumnsPo.
                 * getEnableFlag());
                 * sourceTemplateColumn.setMappingSetId(saveSourceTemplatesColumnsPo.
                 * getMappingSetId());
                 * sourceTemplateColumn.setMappingType(saveSourceTemplatesColumnsPo.
                 * getMappingType());
                 * sourceTemplateColumn.setMappingValue(saveSourceTemplatesColumnsPo.
                 * getMappingValue());
                 * sourceTemplateColumn.setNullAllowedFlag(saveSourceTemplatesColumnsPo.
                 * getNullAllowedFlag());
                 */
                sourceTemplateColumn.setWidth(saveSourceTemplatesColumnsPo.getWidth());
                sourceTemplateColumn.setOrigTransRef(saveSourceTemplatesColumnsPo.getOrigTransRef());
                sourceTemplateColumn.setSelected(saveSourceTemplatesColumnsPo.getSelected());
                sourceTemplateColumn.setSeq(saveSourceTemplatesColumnsPo.getDisplaySeq());
                //sourceTemplateColumn.setStartDate(saveSourceTemplatesColumnsPo.getStartDate());
                sourceTemplateColumn.setTemplateId(saveSourceTemplatesColumnsPo.getTemplateId());
                sourceTemplateColumn.setLastUpdatedBy("ConvertRite");
                sourceTemplateColumn.setLastUpdatedDate(new java.sql.Date(new java.util.Date().getTime()));
                sourceTemplateColumn.setCreationDate(new java.sql.Date(new java.util.Date().getTime()));
                sourceTemplateColumn.setLastUpdatedBy("ConvertRite");
                srcTemplateColumnsLi.add(sourceTemplateColumn);
            }
        }

        List<SourceTemplateColumns> srcTemplateColsResLi = sourceTemplateColumnsRepository
                .saveAll(srcTemplateColumnsLi);
        saveSourceTemplateColumnsResPo.setSourceTemplateColumns(Collections.singletonList(srcTemplateColsResLi));
        saveSourceTemplateColumnsResPo.setMessage("Successfully Saved/Updated SourceTemplateColumns");
        return saveSourceTemplateColumnsResPo;
    }

    @Override
    public BasicResPo repopulateOrigTransRef(ReOrigTransRefReqPo reOrigTransRefReqPo, HttpServletRequest request) throws Exception {
        log.info("Start of loadSourceData ##########");
        BasicResPo res1 = new BasicResPo();
        String res = generateOrigTransRefDaoImpl.generateReOrigTranRef(reOrigTransRefReqPo.getSrcTemplateId(),
                reOrigTransRefReqPo.getSrcStgTableName(), request, reOrigTransRefReqPo.getBatchName());
        res1.setMessage(res);
        res1.setStatusCode(HttpStatus.OK);
        return res1;
    }


    @Override
    public List<LoadSourceDataParentObjectResPo> loadSourceDataAtParentObject(
            LoadSourceDataReqAtParentObjectPo loadSourceDataReqPo,  String tenantId,String userId)
            throws ValidationException, Exception {
        log.info("Start of loadSourceDataAtParentObject in service ##########");
        List<LoadSourceDataParentObjectResPo> loadSourceDataResLi = new ArrayList<>();
        Connection con = null;
        String executeQuery = null;
        String extrnlTableName = "";
        Session jschSession = null;
        ChannelSftp channelSftp = null;
        InputStream inputStream = null;
        try {
            List<CrSourceTemplateHeaders> srcTemplateHdrs = crSourceTemplateHeadersRepo
                    .findByTemplateIdIn(loadSourceDataReqPo.getLoadSrcTemplates());

            MetaDataColumnsPo metaDataColumnsPo = getMetaDataAndColumnNames(
                    srcTemplateHdrs.get(0).getMetaDataTableId());

            String fileName = loadSourceDataReqPo.getFileName();
            extrnlTableName = srcTemplateHdrs.get(0).getStagingTableName() + "_EXT";

            log.info("extrnlTableName--->" + extrnlTableName);
            log.info("TENANT-->" +tenantId);

            con = dynamicDataSourceBasedMultiTenantConnectionProvider.getConnection(tenantId);
            String externalSql = "CREATE TABLE " + extrnlTableName + " (" + metaDataColumnsPo.getMetaDataStr() + " )"
                    + "ORGANIZATION EXTERNAL\r\n" + "(TYPE ORACLE_LOADER\r\n" + "DEFAULT DIRECTORY G2N_TAB_MAIN\r\n"
                    + "ACCESS PARAMETERS\r\n" + "(\r\n" + "records delimited by newline\r\n"
                    + "LOGFILE G2N_TAB_MAIN:'" + extrnlTableName + ".log'\r\n" + "BADFILE G2N_TAB_MAIN:'"
                    + extrnlTableName + ".bad'\r\n" + "skip 1\r\n"
                    + "fields terminated by ',' optionally enclosed BY '\"' LDRTRIM\r\n"
                    + "missing field values are null\r\n" + ")\r\n" + "LOCATION ('" + fileName
                    + "')\r\n" + ")  REJECT LIMIT UNLIMITED";
            PreparedStatement extrnlStmnt = con.prepareStatement(externalSql);
            int extrnlCount = extrnlStmnt.executeUpdate();
            log.info(extrnlCount + "########count");
            log.info(externalSql);
            extrnlStmnt.close();
            for (CrSourceTemplateHeaders srcTemplateHeaders : srcTemplateHdrs) {
                LoadSourceDataParentObjectResPo loadSourceDataResPo = new LoadSourceDataParentObjectResPo();
                PreparedStatement countStmnt = con
                        .prepareStatement("select count(*) from " + srcTemplateHeaders.getStagingTableName()
                                + " where cr_batch_name='" + loadSourceDataReqPo.getBatchName() + "'");
                ResultSet rs = countStmnt.executeQuery();
                int count = 0;
                if (rs.next()) {
                    count = rs.getInt("count(*)");
                    log.info("########" + rs.getInt("count(*)"));
                }
                countStmnt.close();
                if (count > 0)
                    throw new ValidationException("BatchName Already Exists");

                executeQuery = "insert into " + srcTemplateHeaders.getStagingTableName() + "  SELECT '"
                        + srcTemplateHeaders.getTemplateId() + "',NULL,NULL,ROWNUM,b.*,ROWNUM,'"
                        + loadSourceDataReqPo.getBatchName() + "' FROM (SELECT " + metaDataColumnsPo.getColumnNames()
                        + " FROM " + extrnlTableName + ") b";
                log.info(executeQuery + ":::::::Insert executeQuery");

                PreparedStatement extStmnt = con.prepareStatement(executeQuery);
                int insertcount = extStmnt.executeUpdate();
                log.info("insertcount########" + insertcount);
                extStmnt.close();
                generateOrigTransRefDaoImpl.generateOrigTranRef(srcTemplateHeaders.getTemplateId(),
                        srcTemplateHeaders.getStagingTableName(), userId, loadSourceDataReqPo.getBatchName());

                if("SFTP".equalsIgnoreCase(fileTransfer)) {
                    jschSession = utils.setupJschSession();
                    channelSftp = (ChannelSftp) jschSession.openChannel("sftp");
                    channelSftp.connect();
                    channelSftp.cd(fileUploadDir);
                }
                try {
                    if("SFTP".equalsIgnoreCase(fileTransfer)) {
                        inputStream = channelSftp.get(extrnlTableName + ".bad");
                    }
                    else if("NFS".equalsIgnoreCase(fileTransfer)){
                        inputStream = new FileInputStream(fileUploadDir+extrnlTableName + ".bad");
                    }
                }
                catch (Exception e) {
                    log.error(e.getMessage());

                }
                long failedCount = 0;
                String result=null;
                String logFileText =null;
                if (inputStream != null) {
                    if("SFTP".equalsIgnoreCase(fileTransfer)) {
                        failedCount = new BufferedReader(new InputStreamReader(channelSftp.get(extrnlTableName + ".bad")))
                                .lines().count();
                        result = new BufferedReader(new InputStreamReader(channelSftp.get(extrnlTableName + ".bad")))
                                .lines().collect(Collectors.joining("\n"));
                        logFileText = new BufferedReader(new InputStreamReader(channelSftp.get(extrnlTableName + ".log")))
                                .lines().collect(Collectors.joining("\n"));

                    }
                    else if("NFS".equalsIgnoreCase(fileTransfer)){
                        failedCount = new BufferedReader(new InputStreamReader(new FileInputStream(fileUploadDir+extrnlTableName + ".bad")))
                                .lines().count();
                        result = new BufferedReader(new InputStreamReader(new FileInputStream(fileUploadDir+extrnlTableName + ".bad")))
                                .lines().collect(Collectors.joining("\n"));
                        logFileText = new BufferedReader(new InputStreamReader( new FileInputStream(fileUploadDir+extrnlTableName + ".bad")))
                                .lines().collect(Collectors.joining("\n"));
                    }

                    Date date = new Date();
                    CrSourceLoadFailRecords sourceLoadFailRecords = new CrSourceLoadFailRecords();
                    sourceLoadFailRecords.setTemplateId(srcTemplateHeaders.getTemplateId());
                    sourceLoadFailRecords.setFileName(fileName);
                    sourceLoadFailRecords.setFailed(failedCount);
                    sourceLoadFailRecords.setCrBatchName(loadSourceDataReqPo.getBatchName());
                    sourceLoadFailRecords.setSuccess(insertcount);
                    sourceLoadFailRecords.setFailedClob(result);
                    sourceLoadFailRecords.setCreatedBy("ConvertRite");
                    sourceLoadFailRecords.setCreationDate(date);
                    sourceLoadFailRecords.setLastUpdateBy("ConvertRite");
                    sourceLoadFailRecords.setLastUpdatedDate(date);
                    sourceLoadFailRecords.setLogFileBlob(logFileText);
                    crSourceLoadFailRecordsRepo.save(sourceLoadFailRecords);
                    if("SFTP".equalsIgnoreCase(fileTransfer)) {
                        channelSftp.rm(extrnlTableName + ".bad");
                    }
                    else if("NFS".equalsIgnoreCase(fileTransfer)){
                        File fileToDelete = new File(fileUploadDir + extrnlTableName + ".bad");
                        try {
                            boolean isFileDeleted = fileToDelete.delete();
                            log.error("Bad deleted or not:"+isFileDeleted);
                        } catch (Exception e) {
                            log.error(e.getMessage());
                            e.printStackTrace();
                        }
                    }
                }

//                if("SFTP".equalsIgnoreCase(fileTransfer)) {
//                    channelSftp.rm(fileName);
//                }
//                else if("NFS".equalsIgnoreCase(fileTransfer)){
//                    Path filePath = Paths.get(fileUploadDir+fileName);
//                    try {
//                        filePath.toFile().deleteOnExit();
//                    } catch (Exception e) {
//                        log.error(e.getMessage());
//                        e.printStackTrace();
//                    }
//                }
                loadSourceDataResPo.setSrcTemplateName(srcTemplateHeaders.getTemplateName());
                loadSourceDataResPo.setSrcTemplateId(srcTemplateHeaders.getTemplateId());
                loadSourceDataResPo.setLoadedRecords(insertcount);
                loadSourceDataResPo.setStgTableName(srcTemplateHeaders.getStagingTableName());
                loadSourceDataResPo.setFailedRecords(failedCount);

                if (failedCount == 0)
                    loadSourceDataResPo
                            .setMessage("Successfully Loaded Data into " + srcTemplateHeaders.getStagingTableName());
                else
                    loadSourceDataResPo.setMessage(failedCount + " Records failed loading into source staging table");
                loadSourceDataResLi.add(loadSourceDataResPo);
            }
//            if("SFTP".equalsIgnoreCase(fileTransfer)) {
//                channelSftp.rm(fileName);
//            }
//            else if("NFS".equalsIgnoreCase(fileTransfer)){
//                Path filePath = Paths.get(fileUploadDir+fileName);
//                try {
//                    if (filePath != null)
//                        filePath.toFile().deleteOnExit();
//                } catch (Exception e) {
//                    log.error(e.getMessage());
//                    e.printStackTrace();
//                }
//            }

            PreparedStatement dropStmnt = con.prepareStatement("DROP TABLE " + extrnlTableName);
            int tableDeleted = dropStmnt.executeUpdate();
            log.info(tableDeleted + "-->external table Deleted");
            dropStmnt.close();
        } catch (ValidationException e) {
            throw new ValidationException(e.getMessage());
        } catch (Exception e) {
            PreparedStatement dropStmnt = con.prepareStatement("DROP TABLE " + extrnlTableName);
            int tableDeleted = dropStmnt.executeUpdate();
            log.info(tableDeleted + "-->external table Deleted");
            dropStmnt.close();
            throw new Exception(e.getMessage());
        } finally {
            if (channelSftp != null) {
                channelSftp.exit();
                channelSftp.disconnect();
            }
            if (jschSession != null)
                jschSession.disconnect();
            if (con != null)
                con.close();
        }
        return loadSourceDataResLi;
    }
    @Override
    public SrcTemplateColsUpdtRes srcTemplateColumnsUpdate(SrcTemplateColsUpdateReq req, HttpServletRequest request) {
        log.info("Start of srcTemplateColumnsUpdate in service ##");
        SrcTemplateColsUpdtRes srcTemplateColsUpdtRes = new SrcTemplateColsUpdtRes();
        String userId = request.getHeader("userId");
        srcTemplateColsUpdtRes = saveSourceTemplateColumnsDaoImpl.srcTemplateColumnsUpdate(req, userId);
        //String res=crSourceTemplateColumnsRepo.CR_SRC_COLS_MODIFY_PROC(req.getTemplateId(),req.getColumnName(),req.getColumnType(),req.getOperationType(),req.getDisplaySeq(),userId);
        //srcTemplateColsUpdtRes.setMessage(res);
        return srcTemplateColsUpdtRes;
    }

    public Map<String, Object> getSourceFailedRecordsDetails(int pageNo, int pageSize, HttpServletResponse response) throws Exception {
        List<SourceLoadFailedRecords> sourceFailedRecords = new ArrayList<>();
        Page<SourceLoadFailedRecords> page = null;
        Map<String, Object> responseBody = new HashMap<>();
        try {
            Pageable pageable = PageRequest.of(pageNo, pageSize);
            page = crSourceLoadFailRecordsRepo.findDetails(pageable);
            response.setHeader("pagecount", String.valueOf(page.getTotalPages()));
            response.setHeader("totalcount", String.valueOf(page.getTotalElements()));
            if (page.hasContent())
                sourceFailedRecords = page.getContent();
            Map<String, String> headersMap = new HashMap<>();
            for (String headerName : response.getHeaderNames()) {
                String headerValue = response.getHeader(headerName);
                headersMap.put(headerName, headerValue);
            }
            responseBody.put("headers", headersMap);
            responseBody.put("data", sourceFailedRecords);
        } catch (Exception e) {
            throw new Exception(e.getMessage());
        }
        return responseBody;
    }
}