package com.rite.products.convertrite.respository;

import com.rite.products.convertrite.model.CrCloudTemplateHeaders;
import com.rite.products.convertrite.model.CrCloudTemplateHeadersView;
import com.rite.products.convertrite.po.CrCldTemplateHdrsObjectGroupLinesRes;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface CrCloudTemplateHeadersViewRepository extends JpaRepository<CrCloudTemplateHeadersView, Long> {

    @Query("SELECT new com.rite.products.convertrite.po.CrCldTemplateHdrsObjectGroupLinesRes(a,b) FROM  CrObjectGroupLinesView a left join CrCloudTemplateHeadersView b on  a.objectId = b.objectId  where  a.groupId= :groupId")
    List<CrCldTemplateHdrsObjectGroupLinesRes> getCldRemplateHdrsbyGroupId(Long groupId);

    CrCloudTemplateHeadersView findByTemplateName(String cloudTemplateName);



}