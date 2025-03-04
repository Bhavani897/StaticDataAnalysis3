package com.rite.products.convertrite.respository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import com.rite.products.convertrite.model.XxrXlsmTempHdrs;

@Repository
public interface XxrXlsmTempHdrsRepository extends JpaRepository<XxrXlsmTempHdrs, String>{

	@Query("Select c.templateId from XxrXlsmTempHdrs c where lower(c.templateName)=lower(:templateName) and c.version=:version and c.sheetName=:sheetName")
	public Long getXlsmTemplateId(String templateName,String version,String sheetName);
	
	public XxrXlsmTempHdrs findByObjectIdAndVersion(Long objectId,String version);
}
