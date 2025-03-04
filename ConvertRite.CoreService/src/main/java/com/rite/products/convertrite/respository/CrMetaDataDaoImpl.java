package com.rite.products.convertrite.respository;

import com.rite.products.convertrite.po.CrMetaDataResPo;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Repository;

import javax.persistence.EntityManager;
import javax.persistence.ParameterMode;
import javax.persistence.PersistenceContext;
import javax.persistence.StoredProcedureQuery;

@Slf4j
@Repository
public class CrMetaDataDaoImpl {
    @PersistenceContext
    private EntityManager entityManager;

    public CrMetaDataResPo preLoadCldMetaData(Long projectId,String targetSystemVersion,String userId) throws Exception {
        log.info("Start of preLoadCldMetaData Method in DaoImpl### ");
        CrMetaDataResPo crMetaDataResPo = new CrMetaDataResPo();
        try {
            StoredProcedureQuery preLoadCldMetaDataProc = entityManager
                    .createStoredProcedureQuery("cr_preload_metadata_proc")
                    .registerStoredProcedureParameter("p_project_id", Long.class, ParameterMode.IN)
                    .registerStoredProcedureParameter("p_target_system_version", String.class,ParameterMode.IN)
                    .registerStoredProcedureParameter("p_user_id", String.class,ParameterMode.IN)
                    .registerStoredProcedureParameter("p_ret_code",String.class,ParameterMode.OUT)
                    .registerStoredProcedureParameter("p_ret_msg",String.class,ParameterMode.OUT)
                    .setParameter("p_project_id",projectId)
                    .setParameter("p_target_system_version",targetSystemVersion)
                    .setParameter("p_user_id",userId);
            preLoadCldMetaDataProc.execute();
            if(preLoadCldMetaDataProc.getOutputParameterValue("p_ret_code") !=null)
            crMetaDataResPo.setReturnCode((String)preLoadCldMetaDataProc.getOutputParameterValue("p_ret_code"));
            if(preLoadCldMetaDataProc.getOutputParameterValue("p_ret_msg") !=null)
                crMetaDataResPo.setReturnMessage((String)preLoadCldMetaDataProc.getOutputParameterValue("p_ret_msg"));

            entityManager.clear();
            entityManager.close();
        } catch (Exception e) {
            throw new Exception("Failed at preLoadCldMetaData :" + e.getMessage());
        }
        return crMetaDataResPo;
    }
}
