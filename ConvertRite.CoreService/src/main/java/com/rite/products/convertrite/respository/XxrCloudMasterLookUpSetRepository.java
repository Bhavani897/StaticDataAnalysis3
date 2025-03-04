package com.rite.products.convertrite.respository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import com.rite.products.convertrite.model.XxrCloudMasterLookUpSet;

public interface XxrCloudMasterLookUpSetRepository extends JpaRepository<XxrCloudMasterLookUpSet,Long>{

	/*
	 * @Query("select new com.rite.products.convertrite.po.PodsPo(podId,podName) from XxrCloudMasterLookUpSet group by podId,podName"
	 * ) public List<PodsPo> getPods();
	 * 
	 * @Query("select new com.rite.products.convertrite.po.ProjectsPo(projectId,projectName) from XxrCloudMasterLookUpSet where podId =:podId group by projectId,projectName"
	 * ) public List<ProjectsPo> getProjectsByLov(@Param("podId") long podId);
	 */
	
	/*
	 * @Query("select s.lookUpSetId from XxrCloudMasterLookUpSet s where s.podId=:podId and s.columnName=:columnName and s.projectName=:projectName"
	 * ) public Long getLookupSetId(@Param("podId") Long podId,@Param("columnName")
	 * String columnName,@Param("projectName") String projectName);
	 */
	
	@Query("select s.lookUpSetId from XxrCloudMasterLookUpSet s where  s.columnName=:columnName")
	public Long getLookupSetId(@Param("columnName") String columnName);
	
	
}
