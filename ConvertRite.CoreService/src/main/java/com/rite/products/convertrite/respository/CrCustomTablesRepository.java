package com.rite.products.convertrite.respository;

import com.rite.products.convertrite.model.CrCustomTables;
import org.springframework.data.jpa.repository.JpaRepository;

public interface CrCustomTablesRepository extends JpaRepository<CrCustomTables,Long> {
    CrCustomTables findByTableName(String metaDataTableName);
}
