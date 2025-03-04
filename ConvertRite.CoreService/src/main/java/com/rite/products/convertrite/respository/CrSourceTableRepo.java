package com.rite.products.convertrite.respository;

import com.rite.products.convertrite.model.CrSourceTable;
import com.rite.products.convertrite.po.SourceTablesPo;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.query.Procedure;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import javax.persistence.EntityManager;
import javax.persistence.ParameterMode;
import javax.persistence.PersistenceContext;
import javax.persistence.StoredProcedureQuery;
import java.util.List;
import java.util.Map;

@Repository
public interface CrSourceTableRepo extends JpaRepository<CrSourceTable,Long> {
    @PersistenceContext
    EntityManager entityManager = null;
    List<SourceTablesPo> findAllByObjectId(Long objectId);

//    @Procedure(name = "source_metadata_sp")
//    String loadSourceMetaData(String fileName, String userId, Long objectId);

    @Procedure
    String CR_LOAD_METADATA_PROC(String type,String fileName, String userId, Long objectId);
    @Procedure
    String CR_CREATE_STG_TABLE_PROC(Long tableId, Long templateId, String templateCode, String environment, String userId);

    @Query(value = "SELECT c.tableId FROM CrSourceTable c "
            + "WHERE  lower(c.tableName) =lower(:tableName)")
    public Long getTableId(@Param("tableName") String tableName);

}
