package com.rite.products.convertrite.respository;

import com.rite.products.convertrite.model.CrProjectsObjects;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface CrProjectsObjectsRepo extends JpaRepository<CrProjectsObjects, Integer> {
    List<CrProjectsObjects> findAllByProjectId(Long projectId);

    @Query("SELECT C FROM CrProjectsObjects C WHERE C.projectId = :projectId AND C.parentObjectCode IS NULL")
    List<CrProjectsObjects> getAllByProjectId(Long projectId);

    @Query("SELECT C FROM CrProjectsObjects C WHERE C.projectId = :projectId AND C.parentObjectCode = :objectCode")
    List<CrProjectsObjects> getAllByObjectCode(Long projectId, String objectCode);

    @Query("SELECT p FROM CrProjectsObjects p, CrCloudTemplateHeaders c WHERE c.objectId = p.objectId AND c.templateId = :cldTemplateId")
    List<CrProjectsObjects> getAllByCloudTemplateId(Long cldTemplateId);

    CrProjectsObjects getByObjectIdAndProjectId(Integer parentObjectId, Long projectId);

    List<CrProjectsObjects> getAllByProjectIdAndConversionTypeIgnoreCaseAndParentObjectCodeIsNull(Long projectId, String conversionType);
    CrProjectsObjects findByObjectId(Integer objectId);
}

