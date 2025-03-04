package com.rite.products.convertrite.respository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.rite.products.convertrite.model.XxrCldTempHdrsView;
import com.rite.products.convertrite.po.CldSrcTemplateIdsResPo;

@Repository
public interface XxrCldTempHdrsViewRepository extends JpaRepository<XxrCldTempHdrsView, Long> {

	XxrCldTempHdrsView findByTemplateId(Long templateId);

	@Query("select b.stagingTableName from XxrCloudTemplateHeader a,SourceTemplateHeaders b where a.sourceTemplateId=b.templateId and a.templateId=:templateId")
	String getSrcStagingTableName(@Param("templateId") Long templateId);

	@Query("select  new com.rite.products.convertrite.po.CldSrcTemplateIdsResPo(a.templateId,a.sourceTemplateId,a.templateName,a.sourceTemplateName) from XxrCldTempHdrsView a where lower(a.parentObjectCode)=lower(:parentObjectCode) and a.roleId=:roleId")
	List<CldSrcTemplateIdsResPo> getByParentObjectCode(String parentObjectCode,Long roleId);

	@Query("select  new com.rite.products.convertrite.po.CldSrcTemplateIdsResPo(a.templateId,a.sourceTemplateId,a.templateName,a.sourceTemplateName) from XxrCldTempHdrsView a where lower(a.objectCode)=lower(:objectCode) and a.roleId=:roleId")
	List<CldSrcTemplateIdsResPo> getByObjectCode(String objectCode,Long roleId);
	
	@Query("select  new com.rite.products.convertrite.po.CldSrcTemplateIdsResPo(a.templateId,a.sourceTemplateId,a.templateName,a.sourceTemplateName) from XxrCldTempHdrsView a where lower(a.parentObjectCode)=lower(:parentObjectCode) and a.roleId=:roleId and a.attribute1='Y'")
	List<CldSrcTemplateIdsResPo> getDefaultTempByParentObjectCode(String parentObjectCode,Long roleId);

	@Query("select  new com.rite.products.convertrite.po.CldSrcTemplateIdsResPo(a.templateId,a.sourceTemplateId,a.templateName,a.sourceTemplateName) from XxrCldTempHdrsView a where lower(a.objectCode)=lower(:objectCode) and a.roleId=:roleId and a.attribute1='Y'")
	List<CldSrcTemplateIdsResPo> getDefaultTempByObjectCode(String objectCode,Long roleId);

	List<XxrCldTempHdrsView> findByRoleId(Long roleId);

}
