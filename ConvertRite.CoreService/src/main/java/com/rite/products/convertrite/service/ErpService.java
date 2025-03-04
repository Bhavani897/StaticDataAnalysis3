package com.rite.products.convertrite.service;

import com.rite.products.convertrite.exception.BadRequestException;
import com.rite.products.convertrite.model.XxrCloudConfig;
import com.rite.products.convertrite.model.XxrFbdiTempCols;
import com.rite.products.convertrite.model.XxrRoleObjectLinks;
import com.rite.products.convertrite.po.CSVHelper;
import com.rite.products.convertrite.po.CloudCSVConfigPojo;
import com.rite.products.convertrite.po.CloudSourceColumnsPo;
import com.rite.products.convertrite.po.CloudTablesPo;
import com.rite.products.convertrite.po.ColumnPo;
import com.rite.products.convertrite.po.FbdiColumnSequencePo;
import com.rite.products.convertrite.po.LoadMetaDataFromCloudRes;
import com.rite.products.convertrite.po.RoleObjectLinkResPo;
import com.rite.products.convertrite.po.RoleObjectReqPo;
import com.rite.products.convertrite.po.SaveCloudAtomationRes;
import com.rite.products.convertrite.po.SaveCloudTemplateColumnsPo;
import com.rite.products.convertrite.po.SaveCloudTemplateColumnsResPo;
import com.rite.products.convertrite.po.SaveCloudTemplateHeadersPo;
import com.rite.products.convertrite.po.SaveFbdiTemplateColumnsPo;
import com.rite.products.convertrite.po.SaveFbdiTemplateColumnsResPo;
import com.rite.products.convertrite.po.SaveFbdiTemplateHeaderPo;
import com.rite.products.convertrite.po.SaveLookUpValuesPo;
import com.rite.products.convertrite.po.SaveLookUpValuesResPo;
import com.rite.products.convertrite.po.SaveTempResPo;
import com.rite.products.convertrite.po.SaveTemplateHeaderResponsePo;
import com.rite.products.convertrite.respository.ProjectEntryDaoImpl;
import com.rite.products.convertrite.respository.SaveFbdiTemplateColumnsDaoImpl;
import com.rite.products.convertrite.respository.SaveFbdiTemplateHeaderDaoImpl;
import com.rite.products.convertrite.respository.SaveLookUpValuesDaoImpl;
import com.rite.products.convertrite.respository.XxrBlobConvertriteRepository;
import com.rite.products.convertrite.respository.XxrCloudConfigRepository;
import com.rite.products.convertrite.respository.XxrCloudDataProcessRepository;
import com.rite.products.convertrite.respository.XxrCloudTableRepository;
import com.rite.products.convertrite.respository.XxrCloudTemplateColumnsRepository;
import com.rite.products.convertrite.respository.XxrCloudTemplateHeadersRepository;
import com.rite.products.convertrite.respository.XxrFbdiTempColsRepository;
import com.rite.products.convertrite.respository.XxrFbdiTempHdrsRepository;
import com.rite.products.convertrite.respository.XxrLookUpValuesRepository;
import com.rite.products.convertrite.respository.XxrLookupSetsRepository;
import com.rite.products.convertrite.respository.XxrProjectWbsTabRepository;
import com.rite.products.convertrite.respository.XxrRoleObjectLinksRepository;
import com.rite.products.convertrite.respository.XxrRolesRepository;
import com.rite.products.convertrite.utils.ControlFileParser;
import com.rite.products.convertrite.utils.DataSourceUtil;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletRequest;
import javax.validation.ValidationException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

@Service
public class ErpService {
    private static final Logger log = LoggerFactory.getLogger(ErpService.class);

    @Autowired
    XxrLookupSetsRepository xxrLookupSetsRepository;
    @Autowired
    XxrCloudDataProcessRepository xxrCloudDataProcessRepository;
    @Autowired
    ProjectEntryDaoImpl projectEntryDaoImpl;
    @Autowired
    XxrCloudTableRepository xxrCloudTableRepository;
    @Autowired
    XxrCloudConfigRepository xxrCloudConfigRepository;
    @Autowired
    SaveLookUpValuesDaoImpl saveLookUpValuesDaoImpl;
    @Autowired
    XxrRoleObjectLinksRepository xxrRoleObjectLinksRepository;
    //    @Autowired
//    SaveLookUpValueDaoImpl saveLookUpValueDaoImpl;
    @Autowired
    XxrLookUpValuesRepository xxrLookUpValuesRepository;
    @Autowired
    SaveFbdiTemplateHeaderDaoImpl saveFbdiTemplateHeaderDao;
    @Autowired
    XxrFbdiTempHdrsRepository xxrFbdiTempHdrsRepository;
    @Autowired
    SaveFbdiTemplateColumnsDaoImpl saveFbdiTemplateColumnsDaoImpl;
    @Autowired
    XxrFbdiTempColsRepository xxrFbdiTempColsRepository;
    @Autowired
    XxrProjectWbsTabRepository xxrProjectWbsTabRepository;
    @Autowired
    XxrCloudTemplateColumnsRepository xxrCloudTemplateColumnsRepository;
    @Value("${firstpartof-ctl-download-url}")
    private String ctlUrlFirstPart;
    @Value("${secondpartof-ctl-download-url}")
    private String ctlUrlSecondPart;
    @Autowired
    LoadMetaDataService loadMetaDataService;
    @Autowired
    XxrCloudTemplateHeadersRepository xxrCloudTemplateHeadersRepository;
    @Autowired
    DataSourceUtil dataSourceUtil;
    @Autowired
    FbdiService fbdiService;
    //    @Autowired
//    XxrCloudTableRepository xxrCloudTableRepository;
    @Autowired
    XxrBlobConvertriteRepository xxBlobConvertriteRepository;
    //    @Autowired
//    ProcessSourceMetaDataRecordsDaoImpl createSourceRecordsDaoImpl;
//    @Autowired
//    ProcessCloudMetaDataRecordsDaoImpl processCloudMetaDataRecordsDaoImpl;
    @Autowired
    CloudLookUpService cloudLookUpService;
    @Autowired
    XxrRolesRepository xxrRolesRepository;
    @Autowired
    XxrBlobConvertriteRepository xxrBlobConvertriteRepository;
    @Autowired
    XxrCloudService xxrCloudService;

    public String getInterfaceTableName(String objectCode) throws ValidationException {
        String applicationTableName = "";
        XxrCloudConfig xxrCloudConfig = xxrCloudConfigRepository.findByObjectCode(objectCode);
        if (xxrCloudConfig == null)
            throw new ValidationException(
                    "For " + objectCode + " Cloud Configuration is not defined,Please help to define");
        else
            applicationTableName = xxrCloudConfig.getInterfaceTableName();
        return applicationTableName;
    }

    public LoadMetaDataFromCloudRes saveCloudMetadata(MultipartFile file, Long podId, Long projectId, String version, HttpServletRequest request) throws Exception {
        CSVHelper csvHelper = new CSVHelper();
        List<CloudCSVConfigPojo> listRes = CSVHelper.csvParser(file.getInputStream());
        if (listRes.size() > 15) {
            throw new Exception("Please only send 10 rows data");
        }
        Set<CloudCSVConfigPojo> list = listRes
                .stream() // get stream for original list
                .collect(Collectors.toCollection(//distinct elements stored into new SET
                        () -> new TreeSet<>(Comparator.comparing(CloudCSVConfigPojo::getObjectCode)))
                );
        LoadMetaDataFromCloudRes loadMetaDataFromCloudRes = new LoadMetaDataFromCloudRes();
        for (CloudCSVConfigPojo li : list) {
            Long tableId = xxrCloudTableRepository.getTableId(li.getObjectCode() + "_V" + version.toUpperCase() + "_INT");
            if (tableId != null) {
                continue;
            }
            loadMetaDataFromCloudRes = loadMetaDataService.loadMetaDataFromCloud(li.getObjectCode(), li.getObjectCode() + "_V" + version.toUpperCase() + "_INT", podId, projectId, request);
        }
        return loadMetaDataFromCloudRes;
    }

    public SaveCloudAtomationRes save(MultipartFile file, Long podId, Long projectId, String version, HttpServletRequest request) throws Exception {
        SaveCloudAtomationRes saveCloudAutomationRes = new SaveCloudAtomationRes();
        SaveLookUpValuesResPo saveParentLookups = new SaveLookUpValuesResPo();
        SaveLookUpValuesResPo saveObjectLookups = new SaveLookUpValuesResPo();
        CSVHelper csvHelper = new CSVHelper();
        CloudCSVConfigPojo cloudCSVConfigPojo = new CloudCSVConfigPojo();
        List<CloudCSVConfigPojo> listRes = CSVHelper.csvParser(file.getInputStream());
        if (listRes.size() > 50) {
            throw new Exception("Please only send 50 rows data");
        }
        Set<CloudCSVConfigPojo> list = listRes
                .stream() // get stream for original list
                .collect(Collectors.toCollection(//distinct elements stored into new SET
                        () -> new TreeSet<>(Comparator.comparing(CloudCSVConfigPojo::getObjectCode)))
                );
//        saving of parent lookups
        saveParentLookups = saveParentLookUpValues(list, podId, projectId, request);
////        saving of ObjectCode lookups
        saveObjectLookups = saveObjectLookUpValues(list, podId, projectId, request);
//        saving of cloud configuration details
        List<XxrCloudConfig> xxrCloudConfigList = new ArrayList<>();
        List<XxrCloudConfig> xxrCloudConfigs = new ArrayList<>();
        for (CloudCSVConfigPojo lists : listRes) {
            log.info("saving the data into cloud config");
            Optional<XxrCloudConfig> xxrCloudConfigOptional = xxrCloudConfigRepository.findById(lists.getObjectCode());
            if (xxrCloudConfigOptional.isPresent()) {
                continue;
            }
            XxrCloudConfig xxrCloudConfig = new XxrCloudConfig();
            xxrCloudConfig.setXlsmFileName(lists.getXlsmFileName());
            xxrCloudConfig.setCreatedBy("convertrite");
            xxrCloudConfig.setCreationDate(new java.sql.Date(new java.util.Date().getTime()));
            xxrCloudConfig.setCtlFileName(lists.getCtlFileName());
            xxrCloudConfig.setObjectCode(lists.getObjectCode());
            xxrCloudConfig.setInterfaceTableName(lists.getInterfaceTableName());
            xxrCloudConfig.setLastUpdateBy("convertrite");
            xxrCloudConfig.setLastUpdatedDate(new java.sql.Date(new java.util.Date().getTime()));
            xxrCloudConfig.setSheetName(lists.getSheetName());
            xxrCloudConfigList.add(xxrCloudConfig);
        }
        xxrCloudConfigs = xxrCloudConfigRepository.saveAll(xxrCloudConfigList);
        log.info("successfully saving  the data into cloud config");
//      saving of fbdi temp headers
        SaveTemplateHeaderResponsePo saveTemplateHeaderResponsePo = saveFbdiTemplateHdrs(list, podId, projectId, version, request);
//      saving of fbdi temp columns
        SaveFbdiTemplateColumnsResPo saveFbdiTemplateColumnsResPo = saveFbdiTemplateColumns(list, podId, projectId, version, request);
        saveCloudAutomationRes.setSaveParentLookUps(saveParentLookups);
        saveCloudAutomationRes.setSaveObjectLookups(saveObjectLookups);
        saveCloudAutomationRes.setXxrCloudConfigs(xxrCloudConfigs);
        saveCloudAutomationRes.setSaveTemplateHeaderResponsePo(saveTemplateHeaderResponsePo);
        saveCloudAutomationRes.setSaveFbdiTemplateColumnsResPo(saveFbdiTemplateColumnsResPo);
        return saveCloudAutomationRes;
    }


    public SaveCloudAtomationRes automationOfcloud(MultipartFile file, Long podId, Long projectId, String version,String module, HttpServletRequest request) throws Exception {
        SaveCloudAtomationRes saveCloudAutomationRes = new SaveCloudAtomationRes();
        SaveLookUpValuesResPo saveParentLookups = new SaveLookUpValuesResPo();
        SaveLookUpValuesResPo saveObjectLookups = new SaveLookUpValuesResPo();
        CSVHelper csvHelper = new CSVHelper();
        CloudCSVConfigPojo cloudCSVConfigPojo = new CloudCSVConfigPojo();
        List<CloudCSVConfigPojo> listRes = CSVHelper.csvParser(file.getInputStream());
//        if (listRes.size() > 20) {
//            throw new Exception("Please only send 20 rows data");
//        }
        Set<CloudCSVConfigPojo> list = listRes
                .stream() // get stream for original list
                .collect(Collectors.toCollection(//distinct elements stored into new SET
                        () -> new TreeSet<>(Comparator.comparing(CloudCSVConfigPojo::getObjectCode)))
                );
//        saving of parent lookups
        saveParentLookups = saveParentLookUpValues(list, podId, projectId, request);
//       saving of ObjectCode lookups
        saveObjectLookups = saveObjectLookUpValues(list, podId, projectId, request);
//        saving of cloud configuration details
        List<XxrCloudConfig> xxrCloudConfigList = new ArrayList<>();
        List<XxrCloudConfig> xxrCloudConfigs = new ArrayList<>();
        for (CloudCSVConfigPojo lists : listRes) {
            log.info("saving the data into cloud config");
            Optional<XxrCloudConfig> xxrCloudConfigOptional = xxrCloudConfigRepository.findById(lists.getObjectCode());
            if (xxrCloudConfigOptional.isPresent()) {
                continue;
            }
            XxrCloudConfig xxrCloudConfig = new XxrCloudConfig();
            xxrCloudConfig.setXlsmFileName(lists.getXlsmFileName());
            xxrCloudConfig.setCreatedBy("convertrite");
            xxrCloudConfig.setCreationDate(new java.sql.Date(new java.util.Date().getTime()));
            xxrCloudConfig.setCtlFileName(lists.getCtlFileName());
            xxrCloudConfig.setObjectCode(lists.getObjectCode());
            xxrCloudConfig.setInterfaceTableName(lists.getInterfaceTableName());
            xxrCloudConfig.setLastUpdateBy("convertrite");
            xxrCloudConfig.setLastUpdatedDate(new java.sql.Date(new java.util.Date().getTime()));
            xxrCloudConfig.setSheetName(lists.getSheetName());
            xxrCloudConfig.setRejectionTableName(lists.getRejectionTableName());
            xxrCloudConfigList.add(xxrCloudConfig);
        }
        xxrCloudConfigs = xxrCloudConfigRepository.saveAll(xxrCloudConfigList);
        log.info("successfully saving  the data into cloud config");
//      saving of fbdi temp headers
        SaveTemplateHeaderResponsePo saveTemplateHeaderResponsePo = saveFbdiTemplateHdrs(list, podId, projectId, version, request);
//      saving of fbdi temp columns
        SaveFbdiTemplateColumnsResPo saveFbdiTemplateColumnsResPo = saveFbdiTemplateColumns(list, podId, projectId, version, request);
//      saving cloud metadata
        LoadMetaDataFromCloudRes loadMetaDataFromCloudRes = new LoadMetaDataFromCloudRes();
        for (CloudCSVConfigPojo li : list) {
            Long tableId = xxrCloudTableRepository.getTableId(li.getObjectCode() + "_V" + version.toUpperCase() + "_INT");
            if (tableId != null) {
                continue;
            }
            loadMetaDataFromCloudRes = loadMetaDataService.loadMetaDataFromCloud(li.getObjectCode(), li.getObjectCode() + "_V" + version.toUpperCase() + "_INT", podId, projectId, request);
        }
        //        saving cloud template headers
        SaveTemplateHeaderResponsePo saveCloudTempHdrs = saveCloudTempHdrs(list, podId, projectId, version, module, request);
        //        saving cloud template columns
        SaveCloudTemplateColumnsResPo saveCloudTemplateColumnsResPo = saveCloudTempCols(list, podId, projectId, version, module, request);
//        for (CloudCSVConfigPojo li : list) {
//           String selectedFlag = xxrCloudTemplateColumnsRepository.getSelectedFlag("ERP_C_" + li.getObjectCode() + "_V" + version.toUpperCase());
//
//        }
        saveCloudAutomationRes.setSaveParentLookUps(saveParentLookups);
        saveCloudAutomationRes.setSaveObjectLookups(saveObjectLookups);
        saveCloudAutomationRes.setXxrCloudConfigs(xxrCloudConfigs);
        saveCloudAutomationRes.setSaveTemplateHeaderResponsePo(saveTemplateHeaderResponsePo);
        saveCloudAutomationRes.setSaveFbdiTemplateColumnsResPo(saveFbdiTemplateColumnsResPo);
        saveCloudAutomationRes.setSaveCloudTempHeaderResponsePo(saveCloudTempHdrs);
        saveCloudAutomationRes.setSaveCloudTemplateColumnsResPo(saveCloudTemplateColumnsResPo);
        return saveCloudAutomationRes;
    }
    public RoleObjectLinkResPo savingRoleObject(MultipartFile file, Long podId, Long projectId, String role) throws Exception {
        CSVHelper csvHelper = new CSVHelper();
        List<CloudCSVConfigPojo> listRes = CSVHelper.csvFilereader(file.getInputStream());
        Set<CloudCSVConfigPojo> list = listRes
                .stream() // get stream for original list
                .collect(Collectors.toCollection(//distinct elements stored into new SET
                        () -> new TreeSet<>(Comparator.comparing(CloudCSVConfigPojo::getParentObjectName)))
                );
        List<RoleObjectReqPo> roleObjectReqPoLi = new ArrayList<>();
        RoleObjectLinkResPo roleObjectLinkResPo = new RoleObjectLinkResPo();
        Long roleId = xxrRolesRepository.getRoleId(role);
        Long parentId = xxrLookupSetsRepository.getLookupSetId("Parent Object Code");
        for (CloudCSVConfigPojo li : list) {
            Long parentObjectId = (xxrLookUpValuesRepository.getIdByValuesetId(li.getParentObjectName(), parentId));
            List<Long> roleIds = xxrRoleObjectLinksRepository.getrole(roleId, projectId, parentObjectId);
            if (roleIds.size() != 0) {
                continue;
            }
            RoleObjectReqPo ro = new RoleObjectReqPo();
            ro.setRoleId(roleId);
            ro.setEnableFlag("N");
            ro.setParentObjectId(parentObjectId);
            ro.setPodId(podId);
            ro.setProjectId(projectId);
            roleObjectReqPoLi.add(ro);
        }
        roleObjectLinkResPo = saveRoleObject(roleObjectReqPoLi);
        return roleObjectLinkResPo;
    }

    public SaveTempResPo savingCloudTemps(MultipartFile file, Long podId, Long projectId, String version,String module, HttpServletRequest request) throws Exception {
        CSVHelper csvHelper = new CSVHelper();
        List<CloudCSVConfigPojo> listRes = CSVHelper.csvParser(file.getInputStream());
        if (listRes.size() > 50) {
            throw new BadRequestException("Please only send 50 rows data");
        }
        Set<CloudCSVConfigPojo> list = listRes
                .stream() // get stream for original list
                .collect(Collectors.toCollection(//distinct elements stored into new SET
                        () -> new TreeSet<>(Comparator.comparing(CloudCSVConfigPojo::getObjectCode)))
                );
        //        saving cloud template headers
        SaveTemplateHeaderResponsePo saveTemplateHeaderResponsePo = saveCloudTempHdrs(list, podId, projectId, version, module,request);
        //        saving cloud template columns
        SaveCloudTemplateColumnsResPo saveCloudTemplateColumnsResPo = saveCloudTempCols(list, podId, projectId, version,module, request);
        SaveTempResPo saveTempResPo = new SaveTempResPo();
        saveTempResPo.setSaveTemplateHeaderResponsePo(saveTemplateHeaderResponsePo);
        saveTempResPo.setSaveCloudTemplateColumnsResPo(saveCloudTemplateColumnsResPo);
        return saveTempResPo;
    }

    public SaveTemplateHeaderResponsePo saveCloudTempHdrs(Set<CloudCSVConfigPojo> csvConfigPojos, Long podId, Long projectId, String version,String module, HttpServletRequest request) throws Exception {
        List<SaveCloudTemplateHeadersPo> saveCloudTemplateHeadersPos = new ArrayList<>();
        SaveTemplateHeaderResponsePo saveTemplateHeaderResponsePo = new SaveTemplateHeaderResponsePo();
        Long objectSetId = xxrLookupSetsRepository.getLookupSetId("Object Code");
        Long parentSetId = xxrLookupSetsRepository.getLookupSetId("Parent Object Code");
        for (CloudCSVConfigPojo li : csvConfigPojos) {
            SaveCloudTemplateHeadersPo saveCloudTemplateHeadersPo = new SaveCloudTemplateHeadersPo();
            //saveCloudTemplateHeadersPo.setPodId(podId);
            Long objectId = xxrLookUpValuesRepository.getIdByValuesetId(li.getObjectCode(), objectSetId);
            saveCloudTemplateHeadersPo.setObjectId(objectId);
            //saveCloudTemplateHeadersPo.setBuSpecific("N");
            Long cloudTablesPo = xxrCloudTableRepository.getTableId(li.getObjectCode()+"_V"+version+"_INT");
            saveCloudTemplateHeadersPo.setMetaDataTableId(cloudTablesPo);
            saveCloudTemplateHeadersPo.setProjectId(projectId);
            saveCloudTemplateHeadersPo.setParentObjectId((xxrLookUpValuesRepository.getIdByValuesetId(li.getParentObjectName(), parentSetId)));
            saveCloudTemplateHeadersPo.setTemplateName(module.toUpperCase()+"_C_" + li.getObjectCode() + "_V" + version.toUpperCase());
            saveCloudTemplateHeadersPo.setVersion(version);
            saveCloudTemplateHeadersPos.add(saveCloudTemplateHeadersPo);
        }
        return xxrCloudService.saveCloudHeaders(saveCloudTemplateHeadersPos, request);
    }

    public SaveCloudTemplateColumnsResPo saveCloudTempCols(Set<CloudCSVConfigPojo> cloudCSVConfigPojos, Long podId, Long projectId, String version,String module, HttpServletRequest request) throws Exception {
        List<SaveCloudTemplateColumnsPo> saveCloudTemplateColumnsPos = new ArrayList<>();
        for (CloudCSVConfigPojo li : cloudCSVConfigPojos) {
            CloudSourceColumnsPo cloudSourceColumnsPo = xxrCloudService.getCloudSourceColumns(null, li.getObjectCode() +"_V"+version+ "_INT");
            for (ColumnPo columnPo : cloudSourceColumnsPo.getCloudColumns()) {
                SaveCloudTemplateColumnsPo saveCloudTemplateColumnsPo = new SaveCloudTemplateColumnsPo();
                saveCloudTemplateColumnsPo.setColumnName(columnPo.getColumnName());
                saveCloudTemplateColumnsPo.setColumnType(columnPo.getColumnType());
                saveCloudTemplateColumnsPo.setDescription(columnPo.getDescription());
                //saveCloudTemplateColumnsPo.setEnableFlag("Y");
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
                saveCloudTemplateColumnsPo.setOrigTransRef("N");
                if (Objects.equals(columnPo.getNullAllowedFlag(), "Y")) {
                    saveCloudTemplateColumnsPo.setSelected("N");
                } else
                    saveCloudTemplateColumnsPo.setSelected("M");
                saveCloudTemplateColumnsPo.setTemplateId(xxrCloudTemplateHeadersRepository.getTemplateId(module+"_C_" + li.getObjectCode() + "_V" + version.toUpperCase()));
                saveCloudTemplateColumnsPos.add(saveCloudTemplateColumnsPo);
            }
        }
        return xxrCloudService.saveCloudTemplateColumns(saveCloudTemplateColumnsPos, request);
    }


    public SaveTemplateHeaderResponsePo saveFbdiTemplateHdrs(Set<CloudCSVConfigPojo> listRes, Long podId, Long projectId, String version, HttpServletRequest request) throws Exception {
        log.info("Start of saveFbdiTemplateHdrs in Service Layer ###");
        String msg = "";
        long templateId = 0;

        SaveTemplateHeaderResponsePo saveTemplateHeaderResponsePo = new SaveTemplateHeaderResponsePo();
        List<SaveFbdiTemplateHeaderPo> saveFbdiTemplateHeaderPos = new ArrayList<>();
//        List<SaveLookUpValuesPo> saveLookUpValuesPoList = new ArrayList<>();
        Long objectId = xxrLookupSetsRepository.getLookupSetId("Object Code");
        Long parentId = xxrLookupSetsRepository.getLookupSetId("Parent Object Code");
        for (CloudCSVConfigPojo dev : listRes) {
            long objectCode  = xxrLookUpValuesRepository.getIdByValuesetId(dev.getObjectCode(), objectId);
            Long checkTemplateId = xxrFbdiTempHdrsRepository.getFbdiTemplateId(dev.getCtlFileName(), version, dev.getSheetName(),objectCode);
            SaveFbdiTemplateHeaderPo saveFbdiTemplateHeaderPo = new SaveFbdiTemplateHeaderPo();
            if (checkTemplateId != null) {
                saveFbdiTemplateHeaderPo.setFbdiTemplateId(checkTemplateId);
            }
            saveFbdiTemplateHeaderPo.setFbdiTemplateName(dev.getCtlFileName());
            saveFbdiTemplateHeaderPo.setPodId(podId);
            saveFbdiTemplateHeaderPo.setObjectId(objectCode);
            saveFbdiTemplateHeaderPo.setParentObjectId(xxrLookUpValuesRepository.getIdByValuesetId(dev.getParentObjectName(), parentId));
            saveFbdiTemplateHeaderPo.setProjectId(projectId);
            saveFbdiTemplateHeaderPo.setSheetName(dev.getSheetName());
            saveFbdiTemplateHeaderPo.setVersion(version);
            saveFbdiTemplateHeaderPo.setApi(ctlUrlFirstPart + version + ctlUrlSecondPart + dev.getCtlFileName() + ".ctl");
            saveFbdiTemplateHeaderPos.add(saveFbdiTemplateHeaderPo);
        }
        saveTemplateHeaderResponsePo = fbdiService.saveFbdiTemplateHdrs(saveFbdiTemplateHeaderPos, request);
        return saveTemplateHeaderResponsePo;
    }

    //
    public SaveLookUpValuesResPo saveParentLookUpValues(Set<CloudCSVConfigPojo> listRes, Long podId, Long projectId, HttpServletRequest request)
            throws Exception {
        log.info("Start of saveLookUpValues Method in Service Class######");
        String msg = "";
        List<SaveLookUpValuesPo> saveLookUpValuesPoList = new ArrayList<>();
        Long parentId = xxrLookupSetsRepository.getLookupSetId("Parent Object Code");
        String projectName = xxrLookUpValuesRepository.getValueById(projectId);
        int value = xxrLookUpValuesRepository.getMaxAttributeValue(parentId) + 10;
        SaveLookUpValuesResPo saveLookUpValuesResPo = new SaveLookUpValuesResPo();
        for (CloudCSVConfigPojo dev : listRes) {
            Long parentObjectCheck = xxrLookUpValuesRepository.getIdByValuesetId(dev.getParentObjectName(),parentId);
            if (parentObjectCheck != null) {
                continue;
            }
            SaveLookUpValuesPo saveLookUpValuesPo = new SaveLookUpValuesPo();
            saveLookUpValuesPo.setLookUpValue(dev.getParentObjectName());
            saveLookUpValuesPo.setLookUpSetId(parentId);
            saveLookUpValuesPo.setActualValue(projectName);
            saveLookUpValuesPo.setStartDate(new java.sql.Date(new java.util.Date().getTime()));
            saveLookUpValuesPo.setAttribute1("" + value + "");
            saveLookUpValuesPo.setEnabledFlag("Y");
            saveLookUpValuesPoList.add(saveLookUpValuesPo);
            value = value + 10;
        }
        if (saveLookUpValuesPoList.size() != 0) {
            saveLookUpValuesResPo = cloudLookUpService.saveLookUpValues(saveLookUpValuesPoList, request);
        }
        return saveLookUpValuesResPo;
    }

    //
    public List<FbdiColumnSequencePo> getFbdiColumnSequence(String fileName, String version)
            throws Exception {
        log.info("Start of getFbdiColumnSequence in service ###");
        // LinkedHashMap<String, Integer> columnSequence = new LinkedHashMap<>();
        List<FbdiColumnSequencePo> columnSequencePo = new ArrayList<>();
        List<String> columnNames = new ArrayList<>();
        try {
            Path path = downloadFbdiTemplateFromServer(fileName, version);
//            log.info(path + ":::::::ctlfilepath");
            ControlFileParser ctlFileParser = new ControlFileParser(path.toString() + File.separator + fileName);
            int sequence = 10;
            columnNames = ctlFileParser.getColumnNames();
            for (String columnName : columnNames) {
//                log.info("columnName######3" + columnName);
                FbdiColumnSequencePo fbdiColumnSequencePo = new FbdiColumnSequencePo();
                fbdiColumnSequencePo.setDatabaseColumn(columnName);
                fbdiColumnSequencePo.setSequence(sequence);
                columnSequencePo.add(fbdiColumnSequencePo);
                sequence += 10;
            }

        } catch (Exception e) {
            // e.printStackTrace();
            throw new Exception(e.getMessage());
        }
        return columnSequencePo;
    }

    //
    public RoleObjectLinkResPo saveRoleObject(List<RoleObjectReqPo> roleObjectReqPoLi) throws Exception {
        log.info("Start of saveRoleObjec in service####");
        RoleObjectLinkResPo roleObjectLinkResPo = new RoleObjectLinkResPo();
        List<XxrRoleObjectLinks> xxrRoleObjectLi = new ArrayList<>();
        List<XxrRoleObjectLinks> xxrRoleObjectResLi = new ArrayList<>();
        try {
            roleObjectReqPoLi.stream().forEach(x -> {
                XxrRoleObjectLinks xxrRoleObjectLinks = new XxrRoleObjectLinks();
                xxrRoleObjectLinks.setRoleId(x.getRoleId());
               // xxrRoleObjectLinks.setPodId(x.getPodId());
                xxrRoleObjectLinks.setProjectId(x.getProjectId());
                xxrRoleObjectLinks.setParentObjectId(x.getParentObjectId());
                xxrRoleObjectLinks.setEnableFlag(x.getEnableFlag());
                xxrRoleObjectLi.add(xxrRoleObjectLinks);
            });
            xxrRoleObjectResLi = xxrRoleObjectLinksRepository.saveAll(xxrRoleObjectLi);
            roleObjectLinkResPo.setRoleObjectLink(xxrRoleObjectResLi);
            roleObjectLinkResPo.setMessage("Successfully saved RoleObject");
            log.info("Successfully saved RoleObject####");
        } catch (Exception e) {
            throw new Exception(e.getMessage());
        }
        return roleObjectLinkResPo;
    }

    private Path downloadFbdiTemplateFromServer(String fileName, String version)
            throws Exception {
//        log.info("Start of downloadFbdiTemplateFromServer Method ###");
        String url = "";
        Path target = null;
        try {
            String sb = "https://www.oracle.com/webfolder/technetwork/docs/fbdi-" + version +
                    "/fbdi/controlfiles/" +
                    fileName + ".ctl";
            url = sb;

//            resp.setHeader("API", url);
            // create Temp Directory
            target = Files.createTempDirectory("");
//            log.info("target:::::" + target);
            // website url
            URL website = new URL(url);
            File file = new File(target + File.separator + fileName);
            try (InputStream in = website.openStream(); OutputStream outputStream = new FileOutputStream(file)) {
                IOUtils.copy(in, outputStream);
            } catch (IOException e) {
                // e.printStackTrace();
                throw new Exception(e.getMessage());
            }
            // Files.copy(in, target, StandardCopyOption.REPLACE_EXISTING);
        } catch (FileNotFoundException e) {
            // e.printStackTrace();
            throw new Exception(e.getMessage());
        } catch (Exception e) {
            // e.printStackTrace();
            throw new Exception(e.getMessage());
        }
        return target;
    }

    public SaveLookUpValuesResPo saveObjectLookUpValues(Set<CloudCSVConfigPojo> listRes, Long podId, Long projectId, HttpServletRequest request)
            throws Exception {

        log.info("Start of saveLookUpValues Method in Service Class######");
        String msg = "";
        List<SaveLookUpValuesPo> saveLookUpValuesPoList = new ArrayList<>();
        SaveLookUpValuesResPo saveLookUpValuesResPo = new SaveLookUpValuesResPo();
        Long objectId = xxrLookupSetsRepository.getLookupSetId("Object Code");
        String tempParent = null;
        List<CloudCSVConfigPojo> cloudCSVConfigPojos = new ArrayList<>(listRes);
        Map<String, List<CloudCSVConfigPojo>> csvGroupinData = listRes.stream()
                .collect(Collectors.groupingBy(CloudCSVConfigPojo::getParentObjectName));
        Long maxValue = 0L;
        for (int i=0;i<cloudCSVConfigPojos.size();i++) {
            if(i == 0) {
                tempParent = cloudCSVConfigPojos.get(i).getParentObjectName();
                maxValue = xxrLookUpValuesRepository.getMaxObjectValue(objectId,cloudCSVConfigPojos.get(i).getParentObjectName());
            }

            Long ObjectCheck = xxrLookUpValuesRepository.getIdByValuesetId(cloudCSVConfigPojos.get(i).getObjectCode(),objectId);
            if (ObjectCheck != null) {
                continue;
            }
            SaveLookUpValuesPo saveLookUpValuesPo = new SaveLookUpValuesPo();

            if(! tempParent.equals(cloudCSVConfigPojos.get(i).getParentObjectName())){
                tempParent = cloudCSVConfigPojos.get(i).getParentObjectName();
                maxValue = xxrLookUpValuesRepository.getMaxObjectValue(objectId,cloudCSVConfigPojos.get(i).getParentObjectName());
                if(maxValue == null){
                    maxValue = 0L;
                }
            }
            if(maxValue != null && ! cloudCSVConfigPojos.get(i).getParentObjectName().equals(tempParent)){
                saveLookUpValuesPo.setAttribute1("" + (maxValue+1) + "");
            }
            else{
                if(maxValue == null)
                    maxValue = 0L;
                maxValue = maxValue+1;
                saveLookUpValuesPo.setAttribute1(""+maxValue+"");
            }
//            cloudCSVConfigPojos.add(dev);
            saveLookUpValuesPo.setLookUpValue(cloudCSVConfigPojos.get(i).getObjectCode());
            saveLookUpValuesPo.setLookUpSetId(objectId);
            saveLookUpValuesPo.setActualValue(cloudCSVConfigPojos.get(i).getParentObjectName());
            saveLookUpValuesPo.setStartDate(new java.sql.Date(new java.util.Date().getTime()));

//            if(maxValue != null && cloudCSVConfigPojos.get(i).getParentObjectName().equals(tempParent)){
//                saveLookUpValuesPo.setAttribute1("" + (maxValue+i)+ "");
//            }
//            if(i>0 && cloudCSVConfigPojos.get(i).getParentObjectName().equals(tempParent)){
//                saveLookUpValuesPo.setAttribute1("" + (i+1) + "");
//            }



//            if(maxValue != null) {
//                value = xxrLookUpValuesRepository.getMaxObjectValue(objectId, dev.getParentObjectName());
//                saveLookUpValuesPo.setAttribute1("" + value + "");
//                value = value + 1;
//            }
//           else
//                for (int i = 0; i < cloudCSVConfigPojos.size()-1; i++) {
//                    if (cloudCSVConfigPojos.get(i) == cloudCSVConfigPojos.get(i + 1)) {
//                        saveLookUpValuesPo.setAttribute1("" + value + "");
//                        value = value+1;
//                    }
//                }
//            saveLookUpValuesPo.setAttribute1("" + value + "");
            saveLookUpValuesPo.setEnabledFlag("Y");
            saveLookUpValuesPoList.add(saveLookUpValuesPo);
        }
        if (saveLookUpValuesPoList.size() != 0) {
            saveLookUpValuesResPo = cloudLookUpService.saveLookUpValues(saveLookUpValuesPoList, request);
        }
        return saveLookUpValuesResPo;
    }

    public SaveFbdiTemplateColumnsResPo saveFbdiTemplateColumns(Set<CloudCSVConfigPojo> listRes, Long podId, Long projectId, String version, HttpServletRequest request) throws Exception {
        log.info("Start of SaveFbdiTemplateColumnsResPo in Service Layer ###");
        String msg = "";
        List<XxrFbdiTempCols> xxrFbdiTempCols = new ArrayList<>();
        List<SaveFbdiTemplateColumnsPo> saveFbdiTemplateColumnsPos = new ArrayList<>();
        Long objectId = xxrLookupSetsRepository.getLookupSetId("Object Code");
        for (CloudCSVConfigPojo list : listRes) {
            long objectCode  = xxrLookUpValuesRepository.getIdByValuesetId(list.getObjectCode(), objectId);
            Long value = xxrFbdiTempHdrsRepository.getFbdiTemplateId(list.getCtlFileName(), version, list.getSheetName(),objectCode);
            Long count = xxrFbdiTempColsRepository.getColumnCount(value);
            if (count != null) {
                continue;
            }
            List<FbdiColumnSequencePo> fbdiColumnSequencePos = getFbdiColumnSequence(list.getCtlFileName(), version);
            for (FbdiColumnSequencePo fbdiColumnSequencePo : fbdiColumnSequencePos) {
                SaveFbdiTemplateColumnsPo saveFbdiTemplateColumnsPo = new SaveFbdiTemplateColumnsPo();
                saveFbdiTemplateColumnsPo.setFbdiTemplateId(value);
                saveFbdiTemplateColumnsPo.setFbdiColumnName(null);
                saveFbdiTemplateColumnsPo.setSequence(fbdiColumnSequencePo.getSequence());
                saveFbdiTemplateColumnsPo.setRequired(fbdiColumnSequencePo.getRequired());
                saveFbdiTemplateColumnsPo.setDatabaseTable(list.getInterfaceTableName());
                saveFbdiTemplateColumnsPo.setActiveFlag("Y");
                saveFbdiTemplateColumnsPo.setDatabaseColumn(fbdiColumnSequencePo.getDatabaseColumn());
                saveFbdiTemplateColumnsPo.setStartDate(new java.sql.Date(new java.util.Date().getTime()));
                saveFbdiTemplateColumnsPos.add(saveFbdiTemplateColumnsPo);
            }
        }
        SaveFbdiTemplateColumnsResPo saveFbdiTemplateColumnsResPo = new SaveFbdiTemplateColumnsResPo();
        if (saveFbdiTemplateColumnsPos.size() != 0) {
            saveFbdiTemplateColumnsResPo = fbdiService.saveFbdiTemplateColumns(saveFbdiTemplateColumnsPos, request);
        }
        //  saveFbdiTemplateColumnsResPo = fbdiService.saveFbdiTemplateColumns(saveFbdiTemplateColumnsPos, request);
        return saveFbdiTemplateColumnsResPo;
    }
}
