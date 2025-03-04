package com.rite.products.convertrite.respository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.rite.products.convertrite.model.CloudMetaData;
import com.rite.products.convertrite.po.LovValuesPo;

@Repository
public interface CloudMetaDataRepository extends JpaRepository<CloudMetaData, String> {

	/*
	 * @Query("select new com.rite.products.convertrite.po.LovValuesPo(c.value,c.id) from CloudMetaData c where c.columnId=:columnId and c.podId=:podId and c.projectId=:projectId"
	 * ) public List<LovValuesPo> getValues(@Param("columnId") String
	 * columnId, @Param("podId") long podId,
	 * 
	 * @Param("projectId") long projectId);
	 */

	/*
	 * @Query("select new com.rite.products.convertrite.po.PodsPo(c.value,c.value) from CloudMetaData c where c.columnId=:columnId"
	 * ) public List<PodsPo> getPods(@Param("columnId") String columnId);
	 */

	@Query("select new com.rite.products.convertrite.po.LovValuesPo(c.value,c.id) from CloudMetaData c where lower(c.columnId)=lower(:columnId)")
	public List<LovValuesPo> getValues(@Param("columnId") String columnId);

	/*
	 * @Query("select new com.rite.products.convertrite.po.LovValuesPo(b.lookUpValue,b.lookUpValueId) from  XxrLookupSets a, XxrLookUpValues b where a.lookUpSetId=b.lookUpSetId  and lower(a.lookUpSetName)=lower(:lookUpSetName)"
	 * ) public List<LovValuesPo> getValues(@Param("lookUpSetName") String
	 * lookUpSetName);
	 */

	@Query("select c.value  from CloudMetaData c where c.id=:id")
	public String getValueById(@Param("id") long id);

}
