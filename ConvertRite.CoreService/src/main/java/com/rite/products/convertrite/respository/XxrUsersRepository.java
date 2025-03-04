package com.rite.products.convertrite.respository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.rite.products.convertrite.model.XxrUsers;

@Repository
public interface XxrUsersRepository extends JpaRepository<XxrUsers,Long>{

	XxrUsers findByNameIgnoreCase(String userName);
	XxrUsers findByNameIgnoreCaseAndEmailIdIgnoreCase(String name,String emailId);
	XxrUsers findByEmailIdIgnoreCase(String emailId);
	@Query("select x.name from XxrUsers x where x.id =:userId")
	String getUserName(@Param("userId") Long userId);
	
	@Query("select x from XxrUsers x where lower(x.name)=lower(:user)")
	XxrUsers getIdByName(@Param("user") String user);
}
