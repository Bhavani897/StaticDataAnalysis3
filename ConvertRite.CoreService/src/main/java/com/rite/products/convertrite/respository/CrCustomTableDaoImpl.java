package com.rite.products.convertrite.respository;

import com.rite.products.convertrite.po.CrMdfyCustomTblResPo;
import com.rite.products.convertrite.po.CrModifyCustomTblColumnsReqPo;
import com.rite.products.convertrite.po.SrcTemplateColsUpdtRes;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Repository;

import javax.persistence.EntityManager;
import javax.persistence.ParameterMode;
import javax.persistence.PersistenceContext;
import javax.persistence.StoredProcedureQuery;
import javax.transaction.Transactional;

@Repository
@Slf4j
public class CrCustomTableDaoImpl {

    @PersistenceContext
    private EntityManager entityManager;

    @Transactional
    public CrMdfyCustomTblResPo mdfyCustomTableColumns(CrModifyCustomTblColumnsReqPo mdfyCustomTblColumnsReqPo,String userId) {

        CrMdfyCustomTblResPo crMdfyCustomTblResPo = new CrMdfyCustomTblResPo();
        StoredProcedureQuery cutomTableColumnsMdfyPrc = entityManager
                .createStoredProcedureQuery("CR_CUSTOM_TABLE_MODIFY_PROC")
                .registerStoredProcedureParameter("P_TABLE_ID", Long.class, ParameterMode.IN)
                .registerStoredProcedureParameter("P_COLUMN_NAME", String.class, ParameterMode.IN)
                .registerStoredProcedureParameter("P_COLUMN_TYPE", String.class, ParameterMode.IN)
                .registerStoredProcedureParameter("P_OPERATION_TYPE", String.class, ParameterMode.IN)
                .registerStoredProcedureParameter("P_DISPLAY_SEQ", Long.class, ParameterMode.IN)
                .registerStoredProcedureParameter("p_user_id", String.class, ParameterMode.IN)

                .registerStoredProcedureParameter("P_RET_MSG", String.class, ParameterMode.OUT)
                .registerStoredProcedureParameter("P_RET_CODE", String.class, ParameterMode.OUT)
                .setParameter("P_TABLE_ID", mdfyCustomTblColumnsReqPo.getCustomTableId())
                .setParameter("P_COLUMN_NAME", mdfyCustomTblColumnsReqPo.getColumnName())
                .setParameter("P_COLUMN_TYPE", mdfyCustomTblColumnsReqPo.getColumnType())
                .setParameter("P_OPERATION_TYPE", mdfyCustomTblColumnsReqPo.getOperationType())
                .setParameter("P_DISPLAY_SEQ", mdfyCustomTblColumnsReqPo.getDisplaySeq())
                .setParameter("p_user_id", userId);

        cutomTableColumnsMdfyPrc.execute();

        crMdfyCustomTblResPo.setResCode((String) cutomTableColumnsMdfyPrc.getOutputParameterValue("P_RET_CODE"));
        crMdfyCustomTblResPo.setMessage((String) cutomTableColumnsMdfyPrc.getOutputParameterValue("P_RET_MSG"));

        entityManager.clear();
        entityManager.close();

        return crMdfyCustomTblResPo;
    }
}
