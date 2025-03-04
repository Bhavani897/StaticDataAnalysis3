package com.rite.products.convertrite.respository;

import com.rite.products.convertrite.model.CrSourceTemplateHeadersView;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

@Repository
public interface CrSourceTemplateHeadersViewRepo extends JpaRepository<CrSourceTemplateHeadersView,Long> {

    @Query("select a.stagingTableName from CrSourceTemplateHeadersView a where  a.templateId=:sourceTemplateId")
    String getSrcStagingTableName(Long sourceTemplateId);
}
