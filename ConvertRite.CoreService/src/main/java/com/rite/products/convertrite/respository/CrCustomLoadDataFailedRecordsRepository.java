package com.rite.products.convertrite.respository;

import com.rite.products.convertrite.model.CrCustomLoadDataFailedRecords;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;

public interface CrCustomLoadDataFailedRecordsRepository extends JpaRepository<CrCustomLoadDataFailedRecords, Long> {
    Optional<CrCustomLoadDataFailedRecords> findByCustomTableIdAndCrBatchName(Long customTableId, String crBatchName);

    List<CrCustomLoadDataFailedRecords> findByCustomTableId(Long customTableId);
}
