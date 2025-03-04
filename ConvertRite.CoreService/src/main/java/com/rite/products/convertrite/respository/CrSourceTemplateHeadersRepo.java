package com.rite.products.convertrite.respository;

import com.rite.products.convertrite.model.CrSourceTemplateHeaders;
import com.rite.products.convertrite.model.SourceTemplateHeaders;
import com.rite.products.convertrite.po.TemplatesPo;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Repository
@Transactional
public interface CrSourceTemplateHeadersRepo extends JpaRepository<CrSourceTemplateHeaders,Long> {
    @Query("SELECT new com.rite.products.convertrite.po.TemplatesPo(h.templateId,h.templateName) from CrSourceTemplateHeaders h where h.objectId=:objectId"
            + " and h.parentObjectId=:parentObjectId")
    public List<TemplatesPo> getTemplateHeaders(@Param("objectId") long objectId,
                                                @Param("parentObjectId") long parentObjectId);

    @Query("SELECT h.templateId from CrSourceTemplateHeaders h" + " where lower(h.templateName) =lower(:templateName)")
    public Long getTemplateId(@Param("templateName") String templateName);

    List<CrSourceTemplateHeaders> findByTemplateIdIn(List<Long> loadSrcTemplates);

    CrSourceTemplateHeaders findByTemplateId(Long templateId);
}