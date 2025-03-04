package com.rite.products.convertrite.respository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.rite.products.convertrite.model.XxrCloudTemplateHeader;
import com.rite.products.convertrite.po.RecordsPostJobExcecutionPo;

@Repository
public interface XxrCloudTemplateHeadersRepository extends JpaRepository<XxrCloudTemplateHeader,Long>{
	
	@Query("Select c.templateId from XxrCloudTemplateHeader c where lower(c.templateName)=lower(:templateName)")
	public Long getTemplateId(@Param("templateName") String templateName);
	
	@Query("Select c.stagingTableName from XxrCloudTemplateHeader c where c.templateName=:templateName")
	public String getTableName(@Param("templateName") String templateName);
	
	@Query("select  c from XxrCloudTemplateHeader c where c.templateId=:templateId")
	public XxrCloudTemplateHeader getCloudTemplateById(@Param("templateId") Long templateId);
	
	
	@Query("select  new com.rite.products.convertrite.po.RecordsPostJobExcecutionPo(c.templateId,c.stagingTableName,s.templateId,s.stagingTableName) from XxrCloudTemplateHeader c,SourceTemplateHeaders s where c.sourceTemplateId=s.templateId and c.templateName=:cloudTemplateName")
    public RecordsPostJobExcecutionPo getRecordsPostJobExecution(@Param("cloudTemplateName") String cloudTemplateName);
}
