package com.rite.products.convertrite.respository;


import com.rite.products.convertrite.po.CrLoadDataFromEbsReqPo;
import com.rite.products.convertrite.po.CrLoadDataResPo;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Repository;

import javax.persistence.EntityManager;
import javax.persistence.ParameterMode;
import javax.persistence.PersistenceContext;
import javax.persistence.StoredProcedureQuery;
import javax.servlet.http.HttpServletRequest;
import javax.transaction.Transactional;

@Slf4j
@Repository
public class CrBatchWiseProcessingDaoImpl {

    @PersistenceContext
    private EntityManager entityManager;

    @Transactional
    public CrLoadDataResPo srcDataBatchProcessing(String batchName, Long batchSize, String parentStgTableName, String parentColumn, String stagingTableName) throws Exception {
        CrLoadDataResPo crLoadDataResPo = new CrLoadDataResPo();
        log.info("Start of batchWiseProcessing Method in DaoImpl### ");
        String p_ret_msg = null;
        try {
            StoredProcedureQuery batchWiseProcessing = entityManager
                    .createStoredProcedureQuery("CR_SRC_DATA_BATCH_PROC")
                    .registerStoredProcedureParameter("p_staging_table_name", String.class, ParameterMode.IN)
                    .registerStoredProcedureParameter("p_batch_name", String.class, ParameterMode.IN)
                    .registerStoredProcedureParameter("p_batch_size", Long.class, ParameterMode.IN)
                    .registerStoredProcedureParameter("p_parent_staging_table_name", String.class, ParameterMode.IN)
                    .registerStoredProcedureParameter("p_parent_column", String.class, ParameterMode.IN)
                    .registerStoredProcedureParameter("p_msg", String.class, ParameterMode.OUT)
                    .registerStoredProcedureParameter("p_result", String.class, ParameterMode.OUT)
                    .registerStoredProcedureParameter("p_batch_names_out", String.class, ParameterMode.OUT)
                    .registerStoredProcedureParameter("p_duplicate_rec_count", Long.class, ParameterMode.OUT)
                    .setParameter("p_staging_table_name", stagingTableName)
                    .setParameter("p_batch_name", batchName)
                    .setParameter("p_batch_size", batchSize)
                    .setParameter("p_parent_staging_table_name", parentStgTableName != null ? parentStgTableName : "")
                    .setParameter("p_parent_column", parentColumn != null ? parentColumn : "");
            batchWiseProcessing.execute();

            if (batchWiseProcessing.getOutputParameterValue("p_result") != null)
                crLoadDataResPo.setPresult((String) batchWiseProcessing.getOutputParameterValue("p_result"));
            if (batchWiseProcessing.getOutputParameterValue("p_batch_names_out") != null)
                crLoadDataResPo.setBatchNames((String) batchWiseProcessing.getOutputParameterValue("p_batch_names_out"));
            if (batchWiseProcessing.getOutputParameterValue("p_duplicate_rec_count") != null)
                crLoadDataResPo.setDuplicateRecCount((Long) batchWiseProcessing.getOutputParameterValue("p_duplicate_rec_count"));

            entityManager.clear();
            entityManager.close();
            p_ret_msg = (String) batchWiseProcessing.getOutputParameterValue("p_msg");
            log.info("Response message from batchWiseProcessing --->" + p_ret_msg);
        } catch (Exception e) {
            log.info("Exception in batchWiseProcessing --->" + p_ret_msg);
            throw new Exception("Failed at srcDataBatchProcessing :" + e.getMessage());
        }
        return crLoadDataResPo;
    }

}
