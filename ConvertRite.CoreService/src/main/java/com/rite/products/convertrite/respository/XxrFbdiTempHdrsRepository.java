package com.rite.products.convertrite.respository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import com.rite.products.convertrite.model.XxrFbdiTempHdrs;

@Repository
public interface XxrFbdiTempHdrsRepository extends JpaRepository<XxrFbdiTempHdrs,Long>{
  
	@Query("Select c.fbdiTemplateId from XxrFbdiTempHdrs c where lower(c.fbdiTemplateName)=lower(:fbdiTemplateName) and c.version=:version and c.sheetName=:sheetName and c.objectId=:objectId")
	public Long getFbdiTemplateId(String fbdiTemplateName,String version,String sheetName,Long objectId);
	
	public XxrFbdiTempHdrs findByObjectIdAndVersion(Long objectId,String version);

	public XxrFbdiTempHdrs findByFbdiTemplateName(String fbdiTemplateName);

	@Query("Select c.fbdiTemplateId from XxrFbdiTempHdrs c where lower(c.fbdiTemplateName)=lower(:fbdiTemplateName) and c.version=:version and c.sheetName=:sheetName")
	public Long getFbdiTemplateIds(String fbdiTemplateName,String version,String sheetName);
	
}
