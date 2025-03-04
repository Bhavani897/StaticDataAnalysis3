package com.rite.products.convertrite.respository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import com.rite.products.convertrite.model.XxrMappingSet;

@Repository
public interface XxrMappingSetRepository extends JpaRepository<XxrMappingSet,String>{

	@Query("select  x.mapSetId from XxrMappingSet x where x.mapSetName=:mapSetName")
	Long getMapId(String mapSetName);
	
	XxrMappingSet findByMapSetId(Long mapSetId);

}
