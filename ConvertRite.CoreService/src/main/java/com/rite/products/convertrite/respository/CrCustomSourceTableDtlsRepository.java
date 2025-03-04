package com.rite.products.convertrite.respository;

import com.rite.products.convertrite.model.CrCustomSourceTableDtls;
import com.rite.products.convertrite.po.CrCustomTableDtlsResPo;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.util.List;

public interface CrCustomSourceTableDtlsRepository extends JpaRepository<CrCustomSourceTableDtls,Long> {

    @Query("select  new com.rite.products.convertrite.po.CrCustomTableDtlsResPo(a,b.tableName) from CrCustomSourceTableDtls a,CrCustomTables b where a.metadataTableId=b.tableId")
    List<CrCustomTableDtlsResPo> getCustomTableDtls();

    CrCustomSourceTableDtls findByCustomTableName(String customTableName);
}
