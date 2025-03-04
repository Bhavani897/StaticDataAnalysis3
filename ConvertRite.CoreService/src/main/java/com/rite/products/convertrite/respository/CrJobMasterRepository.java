package com.rite.products.convertrite.respository;

import com.rite.products.convertrite.model.CrJobMaster;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface CrJobMasterRepository extends JpaRepository<CrJobMaster, Long> {
    List<CrJobMaster> findAllByScheduleId(Long scheduleId);

    List<CrJobMaster> findAllByScheduleIdOrderByJobId(Long scheduleId);
}
