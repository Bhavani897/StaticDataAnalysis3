package com.rite.products.convertrite.respository;

import com.rite.products.convertrite.model.CRTableSyncStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface CRTableSyncStatusRepository extends JpaRepository<CRTableSyncStatus, Long> {

  //  CRTableSyncStatus findByJobId(Long jobId);

    CRTableSyncStatus findByJobIdAndTableName(Long jobId, String baseTables);
}
