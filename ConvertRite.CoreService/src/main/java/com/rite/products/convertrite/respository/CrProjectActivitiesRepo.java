package com.rite.products.convertrite.respository;

import com.rite.products.convertrite.model.CrProjectActivities;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
@Repository
@Transactional
public interface CrProjectActivitiesRepo extends JpaRepository<CrProjectActivities,Long> {

    CrProjectActivities findByProjectIdAndTaskId(Long projectId, Long taskId);
    @Query("select c from CrProjectActivities c where c.projectId=:projectId  order by c.seq asc")
    List<CrProjectActivities> findAllByProjectId(Long projectId);

    Page<CrProjectActivities> findAllByProjectId(Long projectId, Pageable pageable);
}
