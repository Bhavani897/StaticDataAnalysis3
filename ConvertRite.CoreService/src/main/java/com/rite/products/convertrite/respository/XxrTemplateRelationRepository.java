package com.rite.products.convertrite.respository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.rite.products.convertrite.model.XxrTemplateRelation;

@Repository
public interface XxrTemplateRelationRepository extends JpaRepository<XxrTemplateRelation,Long>{

	@Query("select c from  XxrTemplateRelation c where c.groupId=:groupId and c.isZipped='N'")
	public List<XxrTemplateRelation> getTemplatRelations(@Param("groupId") Long groupId);
	
	public List<XxrTemplateRelation> findByGroupIdOrderByIdDesc(Long groupId);
}
