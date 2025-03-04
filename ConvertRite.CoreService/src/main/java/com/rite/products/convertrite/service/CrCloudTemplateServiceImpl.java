package com.rite.products.convertrite.service;

import com.rite.products.convertrite.Validations.Validations;
import com.rite.products.convertrite.model.*;
import com.rite.products.convertrite.po.*;
import com.rite.products.convertrite.respository.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.SQLTransientException;
import java.util.*;

@Service
public class CrCloudTemplateServiceImpl implements CrCloudTemplateService {

    private static final Logger log = LoggerFactory.getLogger(CrCloudTemplateServiceImpl.class);

    @Autowired
    CrCloudTemplateHeadersRepository crCloudTemplateHeadersRepository;
    @Autowired
    CrCloudTemplateHeadersViewRepository crCloudTemplateHeadersViewRepository;
    @Autowired
    CrCloudTemplateColumnsRepository crCloudTemplateColumnsRepository;
    @Autowired
    CrFomrulaSetsRepo crFormulaSetsRepository;
    @Autowired
    CrSourceTemplateColumnsRepo crSourceTemplateColumnsRepository;
    @Autowired
    CrSourceTemplateHeadersRepo crSourceTemplateHeadersRepository;
    @Autowired
    CrCloudTableRepository crCloudTableRepository;
    @Autowired
    CrMappingSetHeaderRepo crMappingSetHeaderRepository;
    @Autowired
    CrCloudColumnsRepository crCloudColumnsRepository;

    public List<CrCloudTemplateHeadersView> getAllCloudTemplates() throws Exception {
        return crCloudTemplateHeadersViewRepository.findAll();
    }

    public CrCloudTemplateHeaderResPo getCloudTemplateById(Long templateId) throws Exception {
        return generateCloudTemplateHeaderResPo(crCloudTemplateHeadersRepository.findById(templateId).get());
    }

    public CrCloudTemplateHeaderResPo saveCloudTemplateHeaders(CrCloudTemplateHeaderReqPo crCloudTemplateHeadersCreateReqPo) throws SQLTransientException,Exception {
        CrCloudTemplateHeaders crCloudTemplateHeaders = generateCloudTemplateHeaderReqPo(crCloudTemplateHeadersCreateReqPo);
        if ((Objects.nonNull(crCloudTemplateHeadersCreateReqPo.getTemplateId())) && (crCloudTemplateHeadersCreateReqPo.getTemplateId() != 0)) {
            CrCloudTemplateHeaders existingHeader = crCloudTemplateHeadersRepository.findById(crCloudTemplateHeadersCreateReqPo.getTemplateId()).get();
            crCloudTemplateHeaders.setCreatedBy(existingHeader.getCreatedBy());
            crCloudTemplateHeaders.setCreationDate(existingHeader.getCreationDate());
        }
        CrCloudTemplateHeaders updatedCloudTemplateHeader = crCloudTemplateHeadersRepository.save(crCloudTemplateHeaders);
        updatedCloudTemplateHeader = crCloudTemplateHeadersRepository.findById(updatedCloudTemplateHeader.getTemplateId()).get();
        return generateCloudTemplateHeaderResPo(updatedCloudTemplateHeader);
    }

    public void deleteCloudTemplateById(Long templateId) throws Exception {
        crCloudTemplateHeadersRepository.deleteById(templateId);
    }

    public List<CrCloudTemplateColumnsResPo> getCloudTemplateColumns(Long templateId) throws Exception {
        List<CrCloudTemplateColumns> cloudTemplateColumns = new ArrayList<>();
        List<CrCloudTemplateColumnsResPo> cloudTemplateColumnsRes = new ArrayList<>();
        try {
            cloudTemplateColumns = crCloudTemplateColumnsRepository.findByTemplateId(templateId);
            log.info("cloudTemplateColumns--->" + cloudTemplateColumns.size());
            if (cloudTemplateColumns != null) {
                for (CrCloudTemplateColumns crCloudTemplateColumn : cloudTemplateColumns) {
                    cloudTemplateColumnsRes.add(generateCloudTemplateColumnResPo(crCloudTemplateColumn));
                }
            }
        } catch (Exception e) {
            throw new Exception(e.getMessage());
        }
        return cloudTemplateColumnsRes;
    }

    public CrCloudTemplateColumnsResPo getCloudTemplateByColumnId(Long columnId) throws Exception {
        return generateCloudTemplateColumnResPo(crCloudTemplateColumnsRepository.findById(columnId).get());
    }

    public CrCloudTemplateColumnsResPo saveCloudTemplateColumn(CrCloudTemplateColumnsReqPo crCloudTemplateColumnsReqPo) throws Exception {
        CrCloudTemplateColumns columnTobeUpdated = generateCloudTemplateColumnReqPo(crCloudTemplateColumnsReqPo);
        if ((Objects.nonNull(crCloudTemplateColumnsReqPo.getColumnId())) && (crCloudTemplateColumnsReqPo.getColumnId() != 0)) {
            CrCloudTemplateColumns existingColumn = crCloudTemplateColumnsRepository.findById(crCloudTemplateColumnsReqPo.getColumnId()).get();
            columnTobeUpdated.setCreatedBy(existingColumn.getCreatedBy());
            columnTobeUpdated.setCreationDate(existingColumn.getCreationDate());
        }
        CrCloudTemplateColumns updatedColumn = crCloudTemplateColumnsRepository.save(columnTobeUpdated);
        updatedColumn = crCloudTemplateColumnsRepository.findById(updatedColumn.getColumnId()).get();
        return generateCloudTemplateColumnResPo(updatedColumn);
    }

    public List<CrCloudTemplateColumnsResPo> saveAllCloudTemplateColumns(List<CrCloudTemplateColumnsReqPo> crCloudTemplateColumnsReqPoList) throws SQLTransientException,Exception {
		List<CrCloudTemplateColumns> insertList = new ArrayList<CrCloudTemplateColumns>();
        List<CrCloudTemplateColumns> updateList = new ArrayList<CrCloudTemplateColumns>();
        List<Long> deleteList = new ArrayList<Long>();
        Long cloudTemplateId = 0L;
            for (CrCloudTemplateColumnsReqPo crCloudTemplateColumnsReqPo : crCloudTemplateColumnsReqPoList) {
                if (cloudTemplateId == 0L) {
                    cloudTemplateId = crCloudTemplateColumnsReqPo.getTemplateId();
                }
                if (("D").equalsIgnoreCase(crCloudTemplateColumnsReqPo.getInsertOrDelete())) {
                    deleteList.add((long) crCloudTemplateColumnsReqPo.getColumnId());
                } else {

						CrCloudTemplateColumns crCloudTemplateColumn = generateCloudTemplateColumnReqPo(crCloudTemplateColumnsReqPo);

                    if (Objects.isNull(crCloudTemplateColumnsReqPo.getColumnId())
                            || crCloudTemplateColumnsReqPo.getColumnId() == 0) {
                        insertList.add(crCloudTemplateColumn);
                    } else {
                        CrCloudTemplateColumns existingColumn = crCloudTemplateColumnsRepository.findById(crCloudTemplateColumnsReqPo.getColumnId()).get();
                        crCloudTemplateColumn.setCreatedBy(existingColumn.getCreatedBy());
                        crCloudTemplateColumn.setCreationDate(existingColumn.getCreationDate());
                        updateList.add(crCloudTemplateColumn);
                    }
                }
            }
            if (deleteList.size() > 0) {
                    crCloudTemplateColumnsRepository.deleteAllById(deleteList);
            }
            if (insertList.size() > 0) {
                    crCloudTemplateColumnsRepository.saveAll(insertList);
            }
            if (updateList.size() > 0) {
                    crCloudTemplateColumnsRepository.saveAll(updateList);
            }
        List<CrCloudTemplateColumns> updatedColumnsList = crCloudTemplateColumnsRepository.findByTemplateId(cloudTemplateId);
        List<CrCloudTemplateColumnsResPo> res = new ArrayList<>();
        if (updatedColumnsList != null) {
            for (CrCloudTemplateColumns updatedColumn : updatedColumnsList) {
                res.add(generateCloudTemplateColumnResPo(updatedColumn));
            }
        }
        return res;
    }

    public CloudTablesTemplatesPo getCloudTablesTemplates(long projectId, long objectId,
                                                          long parentObjectId) throws Exception {
        log.info("Start of getCloudTablesTemplates in Service Layer ###");
        CloudTablesTemplatesPo cloudTablesTemplatesPo = new CloudTablesTemplatesPo();
        List<CloudTablesPo> cloudDataList = new ArrayList<>();
        List<TemplatesPo> templateHeaders = new ArrayList<>();
        try {
            cloudDataList = crCloudTableRepository.getTableIdNames(objectId);
            templateHeaders = crSourceTemplateHeadersRepository.getTemplateHeaders(objectId, parentObjectId);
            cloudTablesTemplatesPo.setCloudTables(cloudDataList);
            cloudTablesTemplatesPo.setSourceTemplateHeaders(templateHeaders);
        } catch (Exception e) {
            throw new Exception(e.getMessage());
        }

        return cloudTablesTemplatesPo;
    }

    public CloudSourceColumnsPo getCloudSourceColumns(String sourceTemplateName, String cloudTableName)
            throws SQLTransientException,Exception {
        CloudSourceColumnsPo cloudSourceColumnsPo = new CloudSourceColumnsPo();
        Long templateId = null;
        List<SourceColumnsPo> sourceColumns = new ArrayList<>();
        List<ColumnPo> cloudColumns = new ArrayList<>();
            Long tableId = crCloudTableRepository.getTableId(cloudTableName);
            if (!Validations.isNullOrEmpty(sourceTemplateName))
                templateId = crSourceTemplateHeadersRepository.getTemplateId(sourceTemplateName);
            if (tableId != null)
                cloudColumns = crCloudColumnsRepository.getCloudColumnsById(tableId);
            if (templateId != null)
                sourceColumns = crSourceTemplateColumnsRepository.getColumnNames(templateId);
            cloudSourceColumnsPo.setCloudColumns(cloudColumns);
            cloudSourceColumnsPo.setSourceColumns(sourceColumns);
        return cloudSourceColumnsPo;
    }

    public CloudSourceColumnsPo getCloudSourceColumnsByIds(Long templateId, Long tableId) throws Exception {
        CloudSourceColumnsPo cloudSourceColumnsPo = new CloudSourceColumnsPo();
        List<ColumnPo> cloudColumns = new ArrayList<>();
        List<SourceColumnsPo> sourceColumns = new ArrayList<>();
        try {
            if (tableId != null)
                cloudColumns = crCloudColumnsRepository.getCloudColumnsById(tableId);
            if (templateId != null)
                sourceColumns = crSourceTemplateColumnsRepository.getColumnNames(templateId);
            cloudSourceColumnsPo.setCloudColumns(cloudColumns);
            cloudSourceColumnsPo.setSourceColumns(sourceColumns);

        } catch (Exception e) {
            throw new Exception(e.getMessage());
        }
        return cloudSourceColumnsPo;
    }

    public List<CloudMappingSetPo> getCloudMappingSetNames() throws Exception {
        List<CloudMappingSetPo> cloudMappingSetPo = new ArrayList<>();
        try {
            cloudMappingSetPo = crMappingSetHeaderRepository.getCloudMappingSetNames();
        } catch (Exception e) {
            throw new Exception(e.getMessage());
        }

        return cloudMappingSetPo;
    }

    private CrCloudTemplateHeaderResPo generateCloudTemplateHeaderResPo(CrCloudTemplateHeaders crCloudTemplateHeader) {
        CrCloudTemplateHeaderResPo crCloudTemplateHeaderResPo = new CrCloudTemplateHeaderResPo();
        crCloudTemplateHeaderResPo.setTemplateId(crCloudTemplateHeader.getTemplateId());
        crCloudTemplateHeaderResPo.setTemplateName(crCloudTemplateHeader.getTemplateName());
        crCloudTemplateHeaderResPo.setTemplateCode(crCloudTemplateHeader.getTemplateCode());
        crCloudTemplateHeaderResPo.setProjectId(crCloudTemplateHeader.getProjectId());
        crCloudTemplateHeaderResPo.setParentObjectId(crCloudTemplateHeader.getParentObjectId());
        crCloudTemplateHeaderResPo.setObjectId(crCloudTemplateHeader.getObjectId());
        crCloudTemplateHeaderResPo.setMetaDataTableId(crCloudTemplateHeader.getMetaDataTableId());
        crCloudTemplateHeaderResPo.setSourceTemplateId(crCloudTemplateHeader.getSourceTemplateId());
        crCloudTemplateHeaderResPo.setVersion(crCloudTemplateHeader.getVersion());
        crCloudTemplateHeaderResPo.setStagingTableName(crCloudTemplateHeader.getStagingTableName());
        crCloudTemplateHeaderResPo.setViewName(crCloudTemplateHeader.getViewName());
        crCloudTemplateHeaderResPo.setPrimaryTemplateFlag(crCloudTemplateHeader.getPrimaryTemplateFlag());
        crCloudTemplateHeaderResPo.setAttribute1(crCloudTemplateHeader.getAttribute1());
        crCloudTemplateHeaderResPo.setAttribute2(crCloudTemplateHeader.getAttribute2());
        crCloudTemplateHeaderResPo.setAttribute3(crCloudTemplateHeader.getAttribute3());
        crCloudTemplateHeaderResPo.setAttribute4(crCloudTemplateHeader.getAttribute4());
        crCloudTemplateHeaderResPo.setAttribute5(crCloudTemplateHeader.getAttribute5());
        return crCloudTemplateHeaderResPo;
    }

    private CrCloudTemplateHeaders generateCloudTemplateHeaderReqPo(CrCloudTemplateHeaderReqPo crCloudTemplateHeaderReqPo) {
        CrCloudTemplateHeaders crCloudTemplateHeaders = new CrCloudTemplateHeaders();
        if ((Objects.isNull(crCloudTemplateHeaderReqPo.getTemplateId())) || (crCloudTemplateHeaderReqPo.getTemplateId() == 0)) {
            crCloudTemplateHeaders.setCreationDate(new Date());
            crCloudTemplateHeaders.setCreatedBy("CoreUser");
            //TODO Need to change updatedBy and UpdatedDate as optional
            crCloudTemplateHeaders.setLastUpdatedBy("CoreUser");
            crCloudTemplateHeaders.setLastUpdatedDate(new Date());
        } else {
            crCloudTemplateHeaders.setTemplateId(crCloudTemplateHeaderReqPo.getTemplateId());
            crCloudTemplateHeaders.setLastUpdatedBy("CoreUser");
            crCloudTemplateHeaders.setLastUpdatedDate(new Date());
        }

        crCloudTemplateHeaders.setTemplateName(crCloudTemplateHeaderReqPo.getTemplateName());
        crCloudTemplateHeaders.setTemplateCode(crCloudTemplateHeaderReqPo.getTemplateCode());
        crCloudTemplateHeaders.setVersion(crCloudTemplateHeaderReqPo.getVersion());
        crCloudTemplateHeaders.setProjectId(crCloudTemplateHeaderReqPo.getProjectId());
        crCloudTemplateHeaders.setParentObjectId(crCloudTemplateHeaderReqPo.getParentObjectId());
        crCloudTemplateHeaders.setObjectId(crCloudTemplateHeaderReqPo.getObjectId());
        crCloudTemplateHeaders.setMetaDataTableId(crCloudTemplateHeaderReqPo.getMetaDataTableId());
        crCloudTemplateHeaders.setSourceTemplateId(crCloudTemplateHeaderReqPo.getSourceTemplateId());
        crCloudTemplateHeaders.setStagingTableName(crCloudTemplateHeaderReqPo.getStagingTableName());
        crCloudTemplateHeaders.setViewName(crCloudTemplateHeaderReqPo.getViewName());
        crCloudTemplateHeaders.setPrimaryTemplateFlag(crCloudTemplateHeaderReqPo.getPrimaryTemplateFlag());
        crCloudTemplateHeaders.setAttribute1(crCloudTemplateHeaderReqPo.getAttribute1());
        crCloudTemplateHeaders.setAttribute2(crCloudTemplateHeaderReqPo.getAttribute2());
        crCloudTemplateHeaders.setAttribute3(crCloudTemplateHeaderReqPo.getAttribute3());
        crCloudTemplateHeaders.setAttribute4(crCloudTemplateHeaderReqPo.getAttribute4());
        crCloudTemplateHeaders.setAttribute5(crCloudTemplateHeaderReqPo.getAttribute5());

        return crCloudTemplateHeaders;
    }

    private CrCloudTemplateColumnsResPo generateCloudTemplateColumnResPo(CrCloudTemplateColumns crCloudTemplateColumns) {
        CrCloudTemplateColumnsResPo crCloudTemplateColumnsResPo = new CrCloudTemplateColumnsResPo();

        crCloudTemplateColumnsResPo.setColumnId(crCloudTemplateColumns.getColumnId());
        crCloudTemplateColumnsResPo.setColumnName(crCloudTemplateColumns.getColumnName());
        crCloudTemplateColumnsResPo.setTemplateId(crCloudTemplateColumns.getTemplateId());
        if (!Validations.isNullOrEmpty(crCloudTemplateColumns.getDescription()))
            crCloudTemplateColumnsResPo.setDescription(crCloudTemplateColumns.getDescription());
        if (!Validations.isNullOrEmpty(crCloudTemplateColumns.getColumnType()))
            crCloudTemplateColumnsResPo.setColumnType(crCloudTemplateColumns.getColumnType());
        if (crCloudTemplateColumns.getWidth() != null)
            crCloudTemplateColumnsResPo.setWidth(crCloudTemplateColumns.getWidth());
        if (!Validations.isNullOrEmpty(crCloudTemplateColumns.getNullAllowedFlag()))
            crCloudTemplateColumnsResPo.setNullAllowedFlag(crCloudTemplateColumns.getNullAllowedFlag());
        if (crCloudTemplateColumns.getUniqueTransRef() != null)
            crCloudTemplateColumnsResPo.setUniqueTransRef(crCloudTemplateColumns.getUniqueTransRef());
        if (!Validations.isNullOrEmpty(crCloudTemplateColumns.getSelected()))
            crCloudTemplateColumnsResPo.setSelected(crCloudTemplateColumns.getSelected());
        if (crCloudTemplateColumns.getSourceColumnId() != null) {
            String sourceColumnName = "";
            sourceColumnName = crSourceTemplateColumnsRepository.getSourceColumnName(crCloudTemplateColumns.getSourceColumnId());
            crCloudTemplateColumnsResPo.setSourceColumnId(crCloudTemplateColumns.getSourceColumnId());
            if (!Validations.isNullOrEmpty(sourceColumnName))
                crCloudTemplateColumnsResPo.setSourceColumnName(sourceColumnName);

        }
        if (!Validations.isNullOrEmpty(crCloudTemplateColumns.getMappingType()))
            crCloudTemplateColumnsResPo.setMappingType(crCloudTemplateColumns.getMappingType());
        if (crCloudTemplateColumns.getMappingSetId() != null) {
            crCloudTemplateColumnsResPo.setMappingSetId(crCloudTemplateColumns.getMappingSetId());
            String mappingSetName = "";
            if ("Formula".equalsIgnoreCase(crCloudTemplateColumns.getMappingType())) {
                mappingSetName = crFormulaSetsRepository.findById(crCloudTemplateColumns.getMappingSetId()).get().formulaSetName;
            } else {
                System.out.println("crCloudTemplateColumns.getMappingSetId()--->" + crCloudTemplateColumns.getMappingSetId());
                mappingSetName = crMappingSetHeaderRepository.findById(crCloudTemplateColumns.getMappingSetId()).get().mapSetName;
            }
            if (!Validations.isNullOrEmpty(mappingSetName))
                crCloudTemplateColumnsResPo.setMappingSetName(mappingSetName);
        }
        if (!Validations.isNullOrEmpty(crCloudTemplateColumns.getMappingValue1()))
            crCloudTemplateColumnsResPo.setMappingValue1(crCloudTemplateColumns.getMappingValue1());
        if (!Validations.isNullOrEmpty(crCloudTemplateColumns.getMappingValue2()))
            crCloudTemplateColumnsResPo.setMappingValue2(crCloudTemplateColumns.getMappingValue2());
        if (!Validations.isNullOrEmpty(crCloudTemplateColumns.getMappingValue3()))
            crCloudTemplateColumnsResPo.setMappingValue3(crCloudTemplateColumns.getMappingValue3());
        if (!Validations.isNullOrEmpty(crCloudTemplateColumns.getMappingValue4()))
            crCloudTemplateColumnsResPo.setMappingValue4(crCloudTemplateColumns.getMappingValue4());
        if (!Validations.isNullOrEmpty(crCloudTemplateColumns.getMappingValue5()))
            crCloudTemplateColumnsResPo.setMappingValue5(crCloudTemplateColumns.getMappingValue5());
        if (crCloudTemplateColumns.getSeq() != null)
            crCloudTemplateColumnsResPo.setSeq(crCloudTemplateColumns.getSeq());
        if (!Validations.isNullOrEmpty(crCloudTemplateColumns.getAttribute1()))
            crCloudTemplateColumnsResPo.setAttribute1(crCloudTemplateColumns.getAttribute1());
        if (!Validations.isNullOrEmpty(crCloudTemplateColumns.getAttribute2()))
            crCloudTemplateColumnsResPo.setAttribute2(crCloudTemplateColumns.getAttribute2());
        if (!Validations.isNullOrEmpty(crCloudTemplateColumns.getAttribute3()))
            crCloudTemplateColumnsResPo.setAttribute3(crCloudTemplateColumns.getAttribute3());
        if (!Validations.isNullOrEmpty(crCloudTemplateColumns.getAttribute4()))
            crCloudTemplateColumnsResPo.setAttribute4(crCloudTemplateColumns.getAttribute4());
        if (!Validations.isNullOrEmpty(crCloudTemplateColumns.getAttribute5()))
            crCloudTemplateColumnsResPo.setAttribute5(crCloudTemplateColumns.getAttribute5());

        return crCloudTemplateColumnsResPo;
    }

    private CrCloudTemplateColumns generateCloudTemplateColumnReqPo(CrCloudTemplateColumnsReqPo crCloudTemplateColumnsReqPo) {
        CrCloudTemplateColumns crCloudTemplateColumns = new CrCloudTemplateColumns();
        if ((Objects.isNull(crCloudTemplateColumnsReqPo.getColumnId())) || (crCloudTemplateColumnsReqPo.getColumnId() == 0)) {
            crCloudTemplateColumns.setCreationDate(new Date());
            crCloudTemplateColumns.setCreatedBy("CoreUser");
            //TODO Need to change updatedBy and UpdatedDate as optional
            crCloudTemplateColumns.setLastUpdatedBy("CoreUser");
            crCloudTemplateColumns.setLastUpdatedDate(new Date());
        } else {
            crCloudTemplateColumns.setColumnId(crCloudTemplateColumnsReqPo.getColumnId());
            crCloudTemplateColumns.setLastUpdatedBy("CoreUser");
            crCloudTemplateColumns.setLastUpdatedDate(new Date());
        }

        crCloudTemplateColumns.setColumnName(crCloudTemplateColumnsReqPo.getColumnName());
        crCloudTemplateColumns.setTemplateId(crCloudTemplateColumnsReqPo.getTemplateId());
        if (!Validations.isNullOrEmpty(crCloudTemplateColumnsReqPo.getDescription()))
            crCloudTemplateColumns.setDescription(crCloudTemplateColumnsReqPo.getDescription());
        if (!Validations.isNullOrEmpty(crCloudTemplateColumnsReqPo.getColumnType()))
            crCloudTemplateColumns.setColumnType(crCloudTemplateColumnsReqPo.getColumnType());
        if (crCloudTemplateColumnsReqPo.getWidth() != null)
            crCloudTemplateColumns.setWidth(crCloudTemplateColumnsReqPo.getWidth());
        if (!Validations.isNullOrEmpty(crCloudTemplateColumnsReqPo.getNullAllowedFlag()))
            crCloudTemplateColumns.setNullAllowedFlag(crCloudTemplateColumnsReqPo.getNullAllowedFlag());
        if (crCloudTemplateColumnsReqPo.getUniqueTransRef() != null)
            crCloudTemplateColumns.setUniqueTransRef(crCloudTemplateColumnsReqPo.getUniqueTransRef());
        if (!Validations.isNullOrEmpty(crCloudTemplateColumnsReqPo.getSelected()))
            crCloudTemplateColumns.setSelected(crCloudTemplateColumnsReqPo.getSelected());
        if (crCloudTemplateColumnsReqPo.getSourceColumnId() != null)
            crCloudTemplateColumns.setSourceColumnId(crCloudTemplateColumnsReqPo.getSourceColumnId());
        if (!Validations.isNullOrEmpty(crCloudTemplateColumnsReqPo.getMappingType()))
            crCloudTemplateColumns.setMappingType(crCloudTemplateColumnsReqPo.getMappingType());
        if (crCloudTemplateColumnsReqPo.getMappingSetId() != null)
            crCloudTemplateColumns.setMappingSetId(crCloudTemplateColumnsReqPo.getMappingSetId());
        if (!Validations.isNullOrEmpty(crCloudTemplateColumnsReqPo.getMappingValue1()))
            crCloudTemplateColumns.setMappingValue1(crCloudTemplateColumnsReqPo.getMappingValue1());
        if (!Validations.isNullOrEmpty(crCloudTemplateColumnsReqPo.getMappingValue2()))
            crCloudTemplateColumns.setMappingValue2(crCloudTemplateColumnsReqPo.getMappingValue2());
        if (!Validations.isNullOrEmpty(crCloudTemplateColumnsReqPo.getMappingValue3()))
            crCloudTemplateColumns.setMappingValue3(crCloudTemplateColumnsReqPo.getMappingValue3());
        if (!Validations.isNullOrEmpty(crCloudTemplateColumnsReqPo.getMappingValue4()))
            crCloudTemplateColumns.setMappingValue4(crCloudTemplateColumnsReqPo.getMappingValue4());
        if (!Validations.isNullOrEmpty(crCloudTemplateColumnsReqPo.getMappingValue5()))
            crCloudTemplateColumns.setMappingValue5(crCloudTemplateColumnsReqPo.getMappingValue5());
        if (crCloudTemplateColumnsReqPo.getSeq() != null)
            crCloudTemplateColumns.setSeq(crCloudTemplateColumnsReqPo.getSeq());
        if (!Validations.isNullOrEmpty(crCloudTemplateColumnsReqPo.getAttribute1()))
            crCloudTemplateColumns.setAttribute1(crCloudTemplateColumnsReqPo.getAttribute1());
        if (!Validations.isNullOrEmpty(crCloudTemplateColumnsReqPo.getAttribute2()))
            crCloudTemplateColumns.setAttribute2(crCloudTemplateColumnsReqPo.getAttribute2());
        if (!Validations.isNullOrEmpty(crCloudTemplateColumnsReqPo.getAttribute3()))
            crCloudTemplateColumns.setAttribute3(crCloudTemplateColumnsReqPo.getAttribute3());
        if (!Validations.isNullOrEmpty(crCloudTemplateColumnsReqPo.getAttribute4()))
            crCloudTemplateColumns.setAttribute4(crCloudTemplateColumnsReqPo.getAttribute4());
        if (!Validations.isNullOrEmpty(crCloudTemplateColumnsReqPo.getAttribute5()))
            crCloudTemplateColumns.setAttribute5(crCloudTemplateColumnsReqPo.getAttribute5());

        return crCloudTemplateColumns;
    }
}
