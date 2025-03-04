package com.rite.products.convertrite.respository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.rite.products.convertrite.model.XxrCloudMasterLookupValues;
@Repository
public interface XxrCloudMasterLookupValuesRepository extends JpaRepository<XxrCloudMasterLookupValues,Long>{
	
	@Query("select c from XxrCloudMasterLookupValues c where c.lookUpSetId=:lookUpSetId")
	public List<XxrCloudMasterLookupValues> getLookupSetValues(@Param("lookUpSetId") Long lookUpSetId);

}
