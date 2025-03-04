package com.rite.products.convertrite.respository;

import com.rite.products.convertrite.model.CrSourceColumns;
import com.rite.products.convertrite.po.ColumnPo;
import com.rite.products.convertrite.po.CrColumnPo;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface CrSourceColumnsRepo extends JpaRepository<CrSourceColumns,Integer> {
    @Query("select new com.rite.products.convertrite.po.CrColumnPo(x.columnId,x.columnName,x.nullAllowedFlag,x.columnType,x.width,x.columnSequence) from CrSourceColumns x where x.tableId=:tableId Order by nullAllowedFlag asc")
    public List<CrColumnPo> getSourceColumnsById(@Param("tableId") Long tableId);
   //@Query("select c from CrSourceColumns c where c.tableId=2")
   List<CrSourceColumns> findAllByTableId(Long tableId);
   // List<CrSourceColumns> findAllByTableIdOrderByColumnIdAsc(Long tableId);
}
