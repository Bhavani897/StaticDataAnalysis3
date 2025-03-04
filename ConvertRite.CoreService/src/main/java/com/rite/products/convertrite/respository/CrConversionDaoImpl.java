package com.rite.products.convertrite.respository;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Repository;

import javax.persistence.EntityManager;
import javax.persistence.ParameterMode;
import javax.persistence.PersistenceContext;
import javax.persistence.StoredProcedureQuery;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Repository
public class CrConversionDaoImpl {

    @PersistenceContext
    private EntityManager entityManager;

    public Map<String,String>  transformDataToCloud(String cloudTemplateName, String pReprocessFlag, String pBatchFlag, String pBatchName, String userId){
        Map<String,String> resMap=new HashMap<>();
        StoredProcedureQuery createStaggingStoredProcedure = entityManager
                .createStoredProcedureQuery("CR_CLD_TRANSFORM_ASYNC_PROC")
                .registerStoredProcedureParameter("p_cloud_template_name", String.class, ParameterMode.IN)
                .registerStoredProcedureParameter("p_user_id", String.class, ParameterMode.IN)
                .registerStoredProcedureParameter("p_reprocess_flag", String.class, ParameterMode.IN)
                .registerStoredProcedureParameter("p_batch_flag", String.class, ParameterMode.IN)
                .registerStoredProcedureParameter("p_batch_name", String.class, ParameterMode.IN)
                .registerStoredProcedureParameter("p_request_id", String.class, ParameterMode.OUT)
                .registerStoredProcedureParameter("p_ret_code", String.class, ParameterMode.OUT)
                .registerStoredProcedureParameter("p_ret_msg", String.class, ParameterMode.OUT)

                .setParameter("p_cloud_template_name", cloudTemplateName)
                .setParameter("p_user_id",userId )
                .setParameter("p_reprocess_flag", pReprocessFlag)
                .setParameter("p_batch_flag",pBatchFlag )
                .setParameter("p_batch_name", pBatchName);
        createStaggingStoredProcedure.execute();

        resMap.put("requestId",(String) createStaggingStoredProcedure.getOutputParameterValue("p_request_id"));
        resMap.put("responseCode",(String) createStaggingStoredProcedure.getOutputParameterValue("p_ret_code"));
        resMap.put("responseMessage",(String) createStaggingStoredProcedure.getOutputParameterValue("p_ret_msg"));

        entityManager.clear();
        entityManager.close();

        return resMap;
    }

}
