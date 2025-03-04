package com.rite.products.convertrite.respository;

import com.rite.products.convertrite.po.DescriptionTableResPo;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Repository;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.transaction.Transactional;
import java.math.BigDecimal;
import java.util.List;
import java.util.stream.Collectors;

@Repository
@Slf4j
public class CrDDLExecutorDaoImpl {

    @PersistenceContext
    private EntityManager entityManager;

    @Transactional
    public void executeDDL(String ddlStatement) {
        entityManager.createNativeQuery(ddlStatement).executeUpdate();
        entityManager.clear();
        entityManager.close();
    }

    @Transactional
    public List<DescriptionTableResPo> getTableDescription(String tableName) {
        String sql = "SELECT atc.column_name, atc.data_type, atc.nullable, cc.column_sequence " +
                "FROM all_tab_columns atc, cr_custom_source_table_dtls cst, cr_custom_columns cc " +
                "WHERE atc.table_name = :tableName and atc.table_name = cst.custom_table_name " +
                "and cst.metadata_table_id = cc.table_id and atc.column_name=cc.column_name " +
                " ORDER BY atc.column_id";
        List<Object[]> resultList = entityManager.createNativeQuery(sql)
                .setParameter("tableName", tableName.toUpperCase())
                .getResultList();

        return resultList
                .stream()
                .map(objArray -> new DescriptionTableResPo((String)objArray[0],(String)objArray[1],(String)objArray[2],(BigDecimal)objArray[3]))
                .collect(Collectors.toList());
    }

    @Transactional
    public boolean checkValidationTableExists(String tableName) {
        String sql = "SELECT * FROM all_tables WHERE table_name = :tableName";
        List<Object[]> resultList = entityManager.createNativeQuery(sql)
                .setParameter("tableName", tableName)
                .getResultList();
        return resultList.isEmpty()? false: true;
    }

}
