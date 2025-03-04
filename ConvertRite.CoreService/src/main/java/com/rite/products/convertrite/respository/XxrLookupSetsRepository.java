package com.rite.products.convertrite.respository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.rite.products.convertrite.model.XxrLookupSets;
import com.rite.products.convertrite.po.LookUpsetNameResPo;

@Repository
public interface XxrLookupSetsRepository extends JpaRepository<XxrLookupSets,Long>{
	
	@Query("select s.lookUpSetId from XxrLookupSets s where  lower(s.lookUpSetName)=lower(:lookUpSetName)")
	public Long getLookupSetId(@Param("lookUpSetName") String lookUpSetName);
	
	@Query("select s.lookUpSetName from XxrLookupSets s where  s.lookUpSetId=:lookUpSetId")
	public String getLookupSetNamebyId(@Param("lookUpSetId") Long lookUpSetId);
	
	@Query("select new com.rite.products.convertrite.po.LookUpsetNameResPo(s.lookUpSetId,s.lookUpSetName) from XxrLookupSets s where  lower(s.lookUpSetName) LIKE CONCAT('%',:lookUpSetName,'%')")
	public List<LookUpsetNameResPo> getLookupSetName(@Param("lookUpSetName") String lookUpSetName);

}
