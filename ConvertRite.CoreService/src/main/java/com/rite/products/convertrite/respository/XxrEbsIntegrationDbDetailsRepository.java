package com.rite.products.convertrite.respository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.rite.products.convertrite.model.XxrEbsIntegrationDbDetails;
import com.rite.products.convertrite.po.XxrEbsIntegrationDetailsResPo;

@Repository
public interface XxrEbsIntegrationDbDetailsRepository extends JpaRepository<XxrEbsIntegrationDbDetails, Long> {

	/*
	 * XxrEbsIntegrationDbDetails
	 * findByPodIdAndProjectIdAndParentObjectIdAndUserNameAndPassword(
	 * 
	 * @Param("podId") Long podId, @Param("projectId") Long projectId,
	 * 
	 * @Param("parentObjectId") Long parentObjectId, @Param("userName") String
	 * userName,
	 * 
	 * @Param("password") String password);
	 */
	
	XxrEbsIntegrationDbDetails findByPodIdAndProjectIdAndParentObjectId(
			@Param("podId") Long podId, @Param("projectId") Long projectId,
			@Param("parentObjectId") Long parentObjectId);
	
	@Query("select   new  com.rite.products.convertrite.po.XxrEbsIntegrationDetailsResPo(a,b.lookUpValue,c.lookUpValue,d.lookUpValue) from XxrEbsIntegrationDbDetails a,XxrLookUpValues b,XxrLookUpValues c,XxrLookUpValues d where a.podId=b.lookUpValueId and a.projectId=c.lookUpValueId and a.parentObjectId=d.lookUpValueId")
	public List<XxrEbsIntegrationDetailsResPo> getEbsIntegrationDetails();
	
}
