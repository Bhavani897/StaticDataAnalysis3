package com.rite.products.convertrite.respository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.rite.products.convertrite.model.XxrMappingSets;
import com.rite.products.convertrite.po.CloudMappingSetPo;
@Repository
public interface XxrMappingSetsRepository extends JpaRepository<XxrMappingSets, Long>{
	
	/*
	 * @Query("select new com.rite.products.convertrite.po.CloudMappingSetPo(mapSetId,mapSetName,mapSetType,cloudColumn,sourceObject1) from XxrMappingSets where objectId=:objectId"
	 * ) public List<CloudMappingSetPo> getCloudMappingSetNames(@Param("objectId")
	 * Long objectId);
	 */
	
	@Query("select c from XxrMappingSets c where  c.objectId=:objectId and c.podId=:podId and c.projectId=:projectId")
	public List<XxrMappingSets> getCloudMappingSets(@Param("podId") Long podId,@Param("projectId") Long projectId,@Param("objectId") Long objectId);
	
	@Query("select c from XxrMappingSets c where  c.mapSetId=:mapSetId")
	public List<XxrMappingSets> getCloudMappingSetsById(@Param("mapSetId") Long mapSetId);
	
	@Query("select c.mapSetId from XxrMappingSets c where lower(c.mapSetName)=lower(:mapSetName)")
	public Long getMapId(@Param("mapSetName") String mapSetName);
	
	@Query("select c.mapSetType from XxrMappingSets c where c.mapSetId=:mapSetId")
	public String getMappingType(@Param("mapSetId") Long mapSetId);
	
	@Query("select c.mapSetName from XxrMappingSets c where c.mapSetId=:mapSetId")
	public String getMappingSetNameById(@Param("mapSetId") Long mapSetId);
	
	@Query("select new com.rite.products.convertrite.po.CloudMappingSetPo(mapSetId,mapSetName,mapSetType) from XxrMappingSets")
	public List<CloudMappingSetPo> getCloudMappingSetNames();
	
	
	
}
