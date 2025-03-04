package com.rite.products.convertrite.respository;

import java.sql.Array;
import java.util.ArrayList;
import java.util.List;

import com.rite.products.convertrite.po.ActivitiesPo;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.query.Procedure;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.rite.products.convertrite.model.XxrActivities;
@Repository
public interface XxrActivitiesRepository extends JpaRepository<XxrActivities,Long>{
	
	List<XxrActivities> findByprojectId(Long projectId);
	
	
	@Query("select c from XxrActivities c where c.projectId=:projectId and c.podId=:podId order by c.seq asc")
	List<XxrActivities> getActivityLinesById(@Param("projectId") Long projectId,@Param("podId") Long podId);
	
	@Query("select taskId from XxrActivities where taskNum=:preReqTask and projectId=:projectId")
	public Long getTaskIdByPreReqTask(@Param("preReqTask") String preReqTask,@Param("projectId") Long projectId);
	
	@Query("select c from XxrActivities c where c.taskOwnerId=:userId")
 	List<XxrActivities> getActivityByTaskOwnerId(@Param("userId") Long userId);

	@Procedure("xxr_project_save_pkg.xxr_proj_activities_prc")
	String saveProjectActivities(List p_xxr_proj_activities, String user_id);

	//@Query(value = "CALL xxr_project_save_pkg.xxr_proj_activities_prc(:p_xxr_proj_activities,:user_id);", nativeQuery = true)
	@Procedure(name = "in_and_out_test_1")
	String saveProjectActivities1(List p_xxr_proj_activities, String p_user_id);

//	String saveProjectActivities1(@Param("p_xxr_proj_activities") List p_xxr_proj_activities,@Param("user_id") String user_id);

	@Procedure(name = "in_and_out_test")
	String saveLoadTask(Integer p_project_id, Integer p_pod_id,String p_user_id);
}
