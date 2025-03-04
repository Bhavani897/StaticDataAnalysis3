package com.rite.products.convertrite.respository;

import com.rite.products.convertrite.model.CrCustomColumnId;
import com.rite.products.convertrite.model.CrCustomColumns;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;

public interface CrCustomColumnsRepository extends JpaRepository<CrCustomColumns, CrCustomColumnId> {

    List<CrCustomColumns> findAllByTableIdOrderByColumnSequence(Long metaDataTableId);

    @Query("SELECT e FROM CrCustomColumns e WHERE e.tableId = :metaDataTableId AND (UPPER(e.columnName) = :columnName OR e.columnSequence = :columnSequence)")
    List<CrCustomColumns> findCrColumnsForSequenceOrColumnName(@Param("metaDataTableId") Long metaDataTableId,
                                                                @Param("columnName") String columnName,
                                                                @Param("columnSequence") Integer columnSequence);
}
