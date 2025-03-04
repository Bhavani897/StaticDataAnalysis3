package com.rite.products.convertrite.respository;

import com.rite.products.convertrite.model.CrScheduleMaster;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface CrScheduleMasterRepository extends JpaRepository<CrScheduleMaster, Long> {
    List<CrScheduleMaster> findByStatus(String status);

    boolean existsByBatchName(String batchName);

    boolean existsByScheduleName(String scheduleName);
}
