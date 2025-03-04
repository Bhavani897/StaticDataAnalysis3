package com.rite.products.convertrite.respository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.rite.products.convertrite.model.SourceTemplateHeaders;
import com.rite.products.convertrite.po.TemplatesPo;

@Repository
public interface SourceTemplateHeadersRepository extends JpaRepository<SourceTemplateHeaders, String> {

	@Query("SELECT h.templateId from SourceTemplateHeaders h" + " where lower(h.templateName) =lower(:templateName)")
	public Long getTemplateId(@Param("templateName") String templateName);
	
	@Query("SELECT s.templateName from SourceTemplateHeaders s where s.templateId =:templateId")
	public String getTemplateName(@Param("templateId") Long templateId);

	@Query("SELECT h.templateId from SourceTemplateHeaders h" + " where lower(h.viewName) =lower(:viewName)")
	public Long getTemplateIdByViewName(@Param("viewName") String viewName);

	/*
	 * @Query("SELECT h.templateName from SourceTemplateHeaders h where h.saasobjectCode=:objectCode"
	 * +
	 * " and h.saasParentObjectCode=:parentObjectCode and h.podId=:podId and h.projectName=:projectName"
	 * )
	 * 
	 * public String[] getTemplateHeaders(@Param("projectName") String
	 * projectName, @Param("podId") long podId,
	 * 
	 * @Param("objectCode") String objectCode, @Param("parentObjectCode") String
	 * parentObjectCode);
	 */

	@Query("SELECT new com.rite.products.convertrite.po.TemplatesPo(h.templateId,h.templateName) from SourceTemplateHeaders h where h.objectId=:objectId"
			+ " and h.parentObjectId=:parentObjectId")
	public List<TemplatesPo> getTemplateHeaders(@Param("objectId") long objectId,
			@Param("parentObjectId") long parentObjectId);

	public List<SourceTemplateHeaders> findByTemplateId(long templateId);

	@Query("select distinct(h.viewName) from SourceTemplateHeaders h where  h.projectId=:projectId and h.parentObjectId=:parentObjectId and h.objectId=:objectId")
	public String[] getSourceObjects( @Param("projectId") long projectId,
			@Param("parentObjectId") long parentObjectId, @Param("objectId") long objectId);

	@Query("select distinct(h.viewName) from SourceTemplateHeaders h")
	public String[] getAllSourceObjects();
	
	@Query("SELECT s from SourceTemplateHeaders s where s.templateId =:templateId")
	public SourceTemplateHeaders getSourceTemplateHeader(@Param("templateId") Long templateId);

	public List<SourceTemplateHeaders> findByParentObjectIdAndAttribute1(Long parentObjectId, String attribute1);

	public List<SourceTemplateHeaders> findByTemplateIdIn(List<Long> loadSrcTemplates);

}
