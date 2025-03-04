package com.rite.products.convertrite.respository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.rite.products.convertrite.model.XxrRoleObjectLinks;
import com.rite.products.convertrite.model.XxrRoleObjectLinksId;
import com.rite.products.convertrite.po.ParentObjectCodeResPo;
import com.rite.products.convertrite.po.PodsPo;
import com.rite.products.convertrite.po.ProjectsPo;
import com.rite.products.convertrite.po.TaskOwnerLov;

@Repository
public interface XxrRoleObjectLinksRepository extends JpaRepository<XxrRoleObjectLinks, XxrRoleObjectLinksId> {

	List<XxrRoleObjectLinks> findByRoleId(long roleId);

	/*
	 * @Query("select x from XxrRoleObjectLinks x,XxrRoles y where x.roleId=y.id and y.roleName =:roleName"
	 * ) List<XxrRoleObjectLinks> getByrole(@Param("roleName") String roleName);
	 */

	@Query("select x.roleId from XxrRoleObjectLinks x where x.roleId =:roleId  and x.projectId=:projectId and x.parentObjectId =:parentObjectId")
	List<Long> getrole(@Param("roleId") long roleId, @Param("projectId") long projectId,@Param("parentObjectId")long parentObjectId);
	
	
	@Query("select  new com.rite.products.convertrite.po.ParentObjectCodeResPo(x.parentObjectId,z.lookUpValue) from XxrRoleObjectLinks x,XxrRoles y,XxrLookUpValues z where x.roleId=y.id and x.parentObjectId=z.lookUpValueId  and lower(y.roleName) =lower(:roleName) and x.projectId=:projectId")
	List<ParentObjectCodeResPo> getByrole(String roleName, Long projectId);

	@Query("select new com.rite.products.convertrite.po.ParentObjectCodeResPo(x.parentObjectId,z.lookUpValue) from XxrRoleObjectLinks x,XxrRoles y,XxrLookUpValues z where x.roleId=y.id and x.parentObjectId=z.lookUpValueId and lower(y.roleName) =lower(:roleName)")
	List<ParentObjectCodeResPo> getParentObjectCodeByrole(String roleName);

	/*
	 * @Query("select new com.rite.products.convertrite.po.PodsPo(x.podId,z.lookUpValue) from XxrRoleObjectLinks x,XxrRoles y,XxrLookUpValues z where x.roleId=y.id and x.podId=z.lookUpValueId and y.roleName =:role"
	 * ) List<PodsPo> getPodsByRole(String role);
	 */
	@Query("select new com.rite.products.convertrite.po.ProjectsPo(x.projectId,z.lookUpValue) from XxrRoleObjectLinks x,XxrRoles y,XxrLookUpValues z where x.roleId=y.id and x.projectId=z.lookUpValueId  and y.roleName =:role")
	List<ProjectsPo> getProjectsByRole(String role);

	//@Query("select new com.rite.products.convertrite.po.TaskOwnerLov(u.id,u.name) from XxrRoleObjectLinks x,XxrRoles y,XxrUserRoles z,XxrUsers u where x.roleId=y.id  and z.roleId=y.id and  z.userId=u.id and x.projectId=:projectId")
	@Query(" select new com.rite.products.convertrite.po.TaskOwnerLov(  u.id,u.name) from XxrRoleObjectLinks x,XxrUserRoles z,XxrUsers u where  z.roleId=x.roleId and  z.userId=u.id and x.projectId=:projectId group by u.id,u.name ")
	List<TaskOwnerLov> getTaskOwnerLov(@Param("projectId") Long projectId);
	
	
	
	
	
}
