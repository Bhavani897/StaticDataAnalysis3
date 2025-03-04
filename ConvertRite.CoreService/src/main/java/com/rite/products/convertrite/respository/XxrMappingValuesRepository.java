package com.rite.products.convertrite.respository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import com.rite.products.convertrite.model.XxrMappingValues;

public interface XxrMappingValuesRepository extends JpaRepository<XxrMappingValues,Long>{
	
	@Query("select c from XxrMappingValues c where c.mapSetId=:mappingSetId")
	public List<XxrMappingValues> getMappingSetValues(@Param("mappingSetId") Long mappingSetId);

}
