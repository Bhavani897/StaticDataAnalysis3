package com.rite.products.convertrite.respository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import com.rite.products.convertrite.model.XxrUserRoles;
import com.rite.products.convertrite.model.XxrUserRolesId;

@Repository
public interface XxrUserRolesRepository extends JpaRepository<XxrUserRoles, XxrUserRolesId> {

	@Query("select x from XxrUserRoles x,XxrUsers y where x.userId=y.id and y.name=:userName")
	List<XxrUserRoles> getUserRoles(String userName);
	
	@Query("select x from XxrUserRoles x,XxrUsers y where x.userId=y.id and lower(y.emailId)=lower(:emailId)")
	List<XxrUserRoles> getUserRolesByEmail(String emailId);

}
