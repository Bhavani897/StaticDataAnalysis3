package com.rite.products.convertrite.respository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.rite.products.convertrite.model.XxrProjectWbs;
@Repository
public interface XxrProjectWbsTabRepository extends JpaRepository<XxrProjectWbs,Long> {
	
	@Query("select c from XxrProjectWbs c where c.projectId=:projectId and c.podId=:podId")
	public List<XxrProjectWbs> getByprojectId(@Param("projectId") Long projectId,@Param("podId") Long podId);
}
