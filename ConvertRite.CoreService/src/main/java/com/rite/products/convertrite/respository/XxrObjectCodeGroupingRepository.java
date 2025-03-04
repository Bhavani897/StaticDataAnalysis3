package com.rite.products.convertrite.respository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import com.rite.products.convertrite.model.XxrObjectCodeGrouping;

@Repository
public interface XxrObjectCodeGroupingRepository extends JpaRepository<XxrObjectCodeGrouping, Long> {
	@Query("select groupName from  XxrObjectCodeGrouping where groupId=:groupId")
	public String getGroupName(Long groupId);
}
