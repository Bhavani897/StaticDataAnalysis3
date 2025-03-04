package com.rite.products.convertrite.respository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import com.rite.products.convertrite.model.XxrObjectCodeGroupingLines;

@Repository
public interface XxrObjectCodeGroupingLinesRepository extends JpaRepository<XxrObjectCodeGroupingLines, Long> {

	List<XxrObjectCodeGroupingLines> findByGroupId(Long groupId);

	@Query("select groupId from  XxrObjectCodeGroupingLines where  objectId=:objectId")
	public Long getGroupIdbyObjectId(Long objectId);

}
