package com.rite.products.convertrite.respository;

import com.rite.products.convertrite.model.CrScheduleJobView;
import java.util.List;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface CrScheduleJobViewRepository extends JpaRepository<CrScheduleJobView, Long> {

    Page<CrScheduleJobView> findAllByStatusIn(Pageable pageable, List<String> statuses);

    CrScheduleJobView findByJobId(Long jobId);
}
