package com.rite.products.convertrite.respository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.repository.query.Param;

import com.rite.products.convertrite.model.XxrErpIntegrationMetaData;

public interface XxrErpIntegrationMetaDataRepository extends JpaRepository<XxrErpIntegrationMetaData,Long>{
	
	//XxrErpIntegrationMetaData findByPodIdAndProjectIdAndParentObjectIdAndObjectId(@Param("podId") Long podId,@Param("projectId") Long projectId,@Param("parentObjectId") Long parentObjectId,@Param("objectId") Long objectId);

	XxrErpIntegrationMetaData findByParentObjectIdAndObjectId(@Param("parentObjectId") Long parentObjectId,@Param("objectId") Long objectId);

}
