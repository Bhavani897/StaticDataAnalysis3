package com.rite.products.convertrite.respository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.rite.products.convertrite.model.XxrRoles;

@Repository
public interface XxrRolesRepository extends JpaRepository<XxrRoles, Long> {

	@Query("select x.roleName from XxrRoles x where x.id =:roleId")
	String getRoleName(@Param("roleId") Long roleId);

	@Query("select x.id from XxrRoles x where x.roleName =:roleName")
	Long getRoleId(@Param("roleName") String roleName);

	XxrRoles findByRoleNameIgnoreCase(String roleName);
}
