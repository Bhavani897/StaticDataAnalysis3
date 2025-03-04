package com.rite.products.convertrite.respository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import com.rite.products.convertrite.model.XxrCloudMasterLookupValueTab;

public interface XxrCloudMasterLookupValueTabRepository extends JpaRepository<XxrCloudMasterLookupValueTab,Long>{
	
	@Query("select distinct(l.lookupValue) from XxrCloudMasterLookupValueTab l where l.lookupSetId=:lookupSetId")
	public String[] getlookupValue(@Param("lookupSetId") Long lookupSetId);
}
