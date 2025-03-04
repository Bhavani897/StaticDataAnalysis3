package com.rite.products.convertrite.respository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.rite.products.convertrite.model.XxrCloudMasterLookupTab;
@Repository
public interface XxrCloudMasterLookupTabRepository extends JpaRepository<XxrCloudMasterLookupTab,Long>{

	@Query("select l.lookupSetId from XxrCloudMasterLookupTab l where lower(l.cloudObject)=lower(:objectCode) and lower(l.cloudColumn)=lower(:cloudColumn)")
	public Long getLookUpSetId(@Param("objectCode") String objectCode,@Param("cloudColumn") String cloudColumn);
	
}
