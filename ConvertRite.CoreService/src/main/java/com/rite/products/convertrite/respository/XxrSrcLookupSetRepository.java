package com.rite.products.convertrite.respository;

import java.sql.Date;
import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.rite.products.convertrite.model.XxrSrcLookupSets;
import com.rite.products.convertrite.po.LookUpsetNameResPo;

@Repository
public interface XxrSrcLookupSetRepository extends JpaRepository<XxrSrcLookupSets,Long>{

	XxrSrcLookupSets findByLookupSetName(String lookupSetName);
	
	@Query("select max(creationDate) from XxrSrcLookupSets")
	public Date getCreationDate();
	
	@Query("select new com.rite.products.convertrite.po.LookUpsetNameResPo(s.lookupsetId,s.lookupSetName) from XxrSrcLookupSets s where  lower(s.lookupSetName) LIKE CONCAT('%',:lookUpSetName,'%')")
	public List<LookUpsetNameResPo> getLookupSetName(@Param("lookUpSetName") String lookUpSetName);
	
	@Query("select s.lookupSetName from XxrSrcLookupSets s where s.lookupsetId=:lookupSetId")
	public String getSrcLookupSetNameById(@Param("lookupSetId") Long lookupSetId);
}
