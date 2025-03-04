package com.rite.products.convertrite.respository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.query.Procedure;
import org.springframework.data.repository.query.Param;

import com.rite.products.convertrite.model.XxrProjects;
import com.rite.products.convertrite.model.XxrProjectsId;
import com.rite.products.convertrite.po.DashBoardProjDatesPo;
import com.rite.products.convertrite.po.XxrProjectsResPo;

public interface XxrProjectsRepository extends JpaRepository<XxrProjects,XxrProjectsId>{
	
	@Query("select projectId from XxrProjects where projectName=:projectName")
	public Long getProjectIdByProjectName(String projectName);
	
	@Query("select p.projectId from XxrProjects p where lower(p.projectName)=lower(:projectName) and p.podId=:podId")
	public Long getProjectId(@Param("projectName")String projectName,@Param("podId") Long PodId);
	
	@Query("select p from XxrProjects p where lower(p.projectName)=lower(:projectName) and p.podId=:podId")
	public XxrProjects getProject(@Param("projectName")String projectName,@Param("podId") Long PodId);
	
	
	@Query("select projectName from XxrProjects where projectId=:projectId")
	public String getProjectNameByProjId(Long projectId);

	@Query("select  new com.rite.products.convertrite.po.XxrProjectsResPo(a.projectId,a.projectName,a.description,a.podId,b.lookUpValue,a.projecManager,a.clientManager,a.kpiAggLevel,a.startDate,a.endDate,a.projectStatus,a.accessLevel,a.clientProjectNumber,a.programNumber) from XxrProjects a,XxrLookUpValues b where a.podId=b.lookUpValueId")
	public List<XxrProjectsResPo> getAllProjectHeaders();
	@Query("select new com.rite.products.convertrite.po.DashBoardProjDatesPo(p.startDate,p.endDate) from XxrProjects p where p.projectId =:projectId and p.podId =:podId")
	public DashBoardProjDatesPo getProjDatesById(@Param("projectId") long  projectId, @Param("podId") long podId);

	@Procedure("xxr_project_save_pkg.r_xxr_projects")
	String saveProjectHeaders(List p_xxr_projects,String p_user_id);


}
