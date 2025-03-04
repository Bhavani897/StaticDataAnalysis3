package com.rite.products.convertrite.respository;

import com.rite.products.convertrite.model.CrProjects;
import org.springframework.data.jpa.repository.JpaRepository;

public interface CrProjectsRepo extends JpaRepository<CrProjects, String> {
    CrProjects findByProjectId(Long projectId);
}
