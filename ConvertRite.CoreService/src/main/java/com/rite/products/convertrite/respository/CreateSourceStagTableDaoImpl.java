package com.rite.products.convertrite.respository;

import javax.persistence.*;
import javax.servlet.http.HttpServletRequest;
import javax.transaction.Transactional;

import com.rite.products.convertrite.model.CrSourceTemplateHeaders;
import com.rite.products.convertrite.po.BasicResPo;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Repository;

import com.rite.products.convertrite.model.SourceTemplateHeaders;
import com.rite.products.convertrite.po.RepopulateOrigTransRefResPo;
import com.rite.products.convertrite.po.SourceStagingTablePo;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.primitives.UnsignedInteger.valueOf;

@Repository
public class CreateSourceStagTableDaoImpl {

    @PersistenceContext
    private EntityManager entityManager;

    @Autowired
    CrSourceTemplateHeadersRepo crSourceTemplateHeadersRepo;

    private static final Logger log = LoggerFactory.getLogger(CreateSourceStagTableDaoImpl.class);

    @Transactional
    public SourceStagingTablePo createStaggingTable(Long tableId, Long templateId, String templateCode, String environment, String userId)  throws Exception {
        log.info("Start of createStaggingTable Method in DaoImpl### ");
        SourceStagingTablePo stagingPo = new SourceStagingTablePo();
        /* EntityManager entityManager = em.createEntityManager(); */

        StoredProcedureQuery createStaggingStoredProcedure = entityManager
                .createStoredProcedureQuery("CR_CREATE_STG_TABLE_PROC")
                //.registerStoredProcedureParameter("p_pod", String.class, ParameterMode.IN)
                .registerStoredProcedureParameter("p_table_id", Long.class, ParameterMode.IN)
                .registerStoredProcedureParameter("p_template_id", Long.class, ParameterMode.IN)
                .registerStoredProcedureParameter("p_template_code", String.class, ParameterMode.IN)
                .registerStoredProcedureParameter("p_calling_env", String.class, ParameterMode.IN)
                .registerStoredProcedureParameter("p_user_id", String.class, ParameterMode.IN)
                .registerStoredProcedureParameter("p_ret_code", String.class, ParameterMode.OUT)
                .registerStoredProcedureParameter("P_ret_msg", String.class, ParameterMode.OUT)

                .setParameter("p_table_id", tableId)
                .setParameter("p_template_id", templateId)
                .setParameter("p_template_code", templateCode)
                .setParameter("p_calling_env", environment)
                .setParameter("p_user_id", userId);

        createStaggingStoredProcedure.execute();
        stagingPo.setResult((String) createStaggingStoredProcedure.getOutputParameterValue("p_ret_code"));
        stagingPo.setTableName((String) createStaggingStoredProcedure.getOutputParameterValue("P_ret_msg"));
        entityManager.clear();
        entityManager.close();

        return stagingPo;
    }

    public BasicResPo loadSourceData(String dataFileName, String batchName, Long templateId, String templateName, String userId) {
        log.info("Start of repopulateOrigTransRef ###########");

        BasicResPo resPo = new BasicResPo();
        CrSourceTemplateHeaders srcTemplate = crSourceTemplateHeadersRepo.findById(templateId).get();

        if(srcTemplate.getStagingTableName() != null && srcTemplate.getStagingTableName().length() > 0) {
            Query countQuery = entityManager.createNativeQuery("SELECT count(*) from " + srcTemplate.getStagingTableName() + " where src_template_id = :templateId and cr_batch_name = :batchName")
                    .setParameter("templateId", templateId)
                    .setParameter("batchName", batchName);

            Object count = countQuery.getSingleResult();
            int existingRowCount = count != null ? Integer.parseInt(count.toString()) : 0;
            if(existingRowCount == 0 ) {
                StoredProcedureQuery crLoadSrcDataProcedure = entityManager
                        .createStoredProcedureQuery("CR_LOAD_SRC_DATA_PROC")
                        .registerStoredProcedureParameter("p_data_file_name", String.class, ParameterMode.IN)
                        .registerStoredProcedureParameter("p_batch_name", String.class, ParameterMode.IN)
                        .registerStoredProcedureParameter("p_template_id", Long.class, ParameterMode.IN)
                        .registerStoredProcedureParameter("p_template_name", String.class, ParameterMode.IN)
                        .registerStoredProcedureParameter("p_user_id", String.class, ParameterMode.IN)
                        .registerStoredProcedureParameter("p_ret_code", String.class, ParameterMode.OUT)
                        .registerStoredProcedureParameter("p_ret_msg", String.class, ParameterMode.OUT)

                        .setParameter("p_data_file_name", dataFileName)
                        .setParameter("p_batch_name", batchName)
                        .setParameter("p_template_id", templateId)
                        .setParameter("p_template_name", templateName)
                        .setParameter("p_user_id", userId);

                crLoadSrcDataProcedure.execute();

                entityManager.clear();
                entityManager.close();

                RepopulateOrigTransRefResPo jsonPayload = new RepopulateOrigTransRefResPo();
                jsonPayload.setResMsg(crLoadSrcDataProcedure.getOutputParameterValue("p_ret_msg").toString());
                jsonPayload.setResCode(crLoadSrcDataProcedure.getOutputParameterValue("p_ret_code").toString());

                resPo = new BasicResPo() {{
                    setStatusCode(HttpStatus.OK);
                    setStatus("success");
                    setMessage("Successfully imported source data");
                    setPayload(jsonPayload);
                }};
            } else {
                resPo = new BasicResPo() {{
                    setStatusCode(HttpStatus.INTERNAL_SERVER_ERROR);
                    setStatus("error");
                    setMessage("Given batch name is already used. It should be unique for a selected template");
                }};
            }
        } else {
            resPo = new BasicResPo() {{
                setStatusCode(HttpStatus.INTERNAL_SERVER_ERROR);
                setStatus("error");
                setMessage("Staging table is not available for selected source template");
            }};
        }
        return resPo;
    }

    public Object loadSourceMetaData(String type, String fileName, String userId, Long objectId) {
        log.info("Start of repopulateOrigTransRef ###########");
        Map<String,String> map = new HashMap<>();
        StoredProcedureQuery cr_load_metadata_proc = entityManager
                .createStoredProcedureQuery("cr_load_metadata_proc")
                .registerStoredProcedureParameter("p_calling_env", String.class, ParameterMode.IN)
                .registerStoredProcedureParameter("p_file_name", String.class, ParameterMode.IN)
                .registerStoredProcedureParameter("p_user_id", String.class, ParameterMode.IN)
                .registerStoredProcedureParameter("p_object_id", Long.class, ParameterMode.IN)

                .registerStoredProcedureParameter("p_ret_code", String.class, ParameterMode.OUT)
                .registerStoredProcedureParameter("p_ret_msg", String.class, ParameterMode.OUT)

                .setParameter("p_calling_env", type)
                .setParameter("p_file_name", fileName)
                .setParameter("p_user_id", userId)
                .setParameter("p_object_id", objectId);

        cr_load_metadata_proc.execute();

        entityManager.clear();
        entityManager.close();

        map.put("result",cr_load_metadata_proc.getOutputParameterValue("p_ret_code").toString());
        map.put("message",cr_load_metadata_proc.getOutputParameterValue("p_ret_msg").toString());
        return map;
    }
}