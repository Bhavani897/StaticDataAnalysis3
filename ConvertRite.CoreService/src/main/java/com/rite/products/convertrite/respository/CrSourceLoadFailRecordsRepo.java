package com.rite.products.convertrite.respository;

import com.rite.products.convertrite.model.CrSourceLoadFailRecords;
import com.rite.products.convertrite.po.SourceLoadFailedRecords;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

@Repository
public interface CrSourceLoadFailRecordsRepo extends JpaRepository<CrSourceLoadFailRecords,Long> {

    @Query("SELECT new  com.rite.products.convertrite.po.SourceLoadFailedRecords(a.id,a.templateId, b.templateName, c.parentObjectCode, a.fileName, a.success, a.failed, a.createdBy, a.creationDate) " +
            "FROM CrSourceLoadFailRecords a, CrSourceTemplateHeaders b, CrProjectsObjects c " +
            "WHERE a.templateId = b.templateId AND b.objectId = c.objectId " )
     Page<SourceLoadFailedRecords>findDetails(Pageable pageable);

}
