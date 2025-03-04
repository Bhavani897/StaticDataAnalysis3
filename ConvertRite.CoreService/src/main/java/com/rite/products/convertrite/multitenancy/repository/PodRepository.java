package com.rite.products.convertrite.multitenancy.repository;

import com.rite.products.convertrite.multitenancy.model.Pod;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface PodRepository extends JpaRepository<Pod, Long> {
    List<Pod> findByScheduledJobFlag(String scheduledJobFlag);

    Pod findByPodId(Long podId);
}
