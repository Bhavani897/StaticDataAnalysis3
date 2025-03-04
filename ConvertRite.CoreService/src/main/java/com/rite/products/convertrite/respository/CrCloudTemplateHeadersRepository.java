package com.rite.products.convertrite.respository;

import com.rite.products.convertrite.model.CrCloudTemplateHeaders;
import com.rite.products.convertrite.po.RecordsPostJobExcecutionPo;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface CrCloudTemplateHeadersRepository extends JpaRepository<CrCloudTemplateHeaders, Long> {
    CrCloudTemplateHeaders findByTemplateName(String cloudTemplateName);
    @Query("select  new com.rite.products.convertrite.po.RecordsPostJobExcecutionPo(c.templateId,c.stagingTableName,s.templateId,s.stagingTableName) from CrCloudTemplateHeaders c,CrSourceTemplateHeaders s where c.sourceTemplateId=s.templateId and c.templateName=:cloudTemplateName")
    public RecordsPostJobExcecutionPo getRecordsPostJobExecution(@Param("cloudTemplateName") String cloudTemplateName);

    @Query("SELECT hdrs.templateName FROM CrCloudTemplateColumns cols, CrCloudTemplateHeaders hdrs " +
            "WHERE cols.mappingType = :mappingType " +
            "AND cols.templateId = hdrs.templateId " +
            "AND cols.mappingSetId = :mappingSetId")
    List<String> selectCldNameFromCldTempHdsAndCols(@Param("mappingType") String mappingType, @Param("mappingSetId") Long mappingSetId);

}


