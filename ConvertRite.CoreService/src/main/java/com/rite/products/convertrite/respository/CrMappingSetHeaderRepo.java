package com.rite.products.convertrite.respository;

import com.rite.products.convertrite.po.CloudMappingSetPo;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import com.rite.products.convertrite.model.CrMappingSetHdr;
import org.springframework.data.repository.query.Param;

import java.util.List;

public interface CrMappingSetHeaderRepo extends JpaRepository<CrMappingSetHdr, Long>{
	
	@Query("Select C From CrMappingSetHdr C where C.mapSetCode = :mapSetCode")
	public CrMappingSetHdr findByMapSetCode(String mapSetCode);

	@Query("select new com.rite.products.convertrite.po.CloudMappingSetPo(mapSetId,mapSetName,mapSetType) from CrMappingSetHdr")
	public List<CloudMappingSetPo> getCloudMappingSetNames();

	boolean existsByMapSetCode(String mapSetCode);

	@Query("SELECT m.mapSetName FROM CrMappingSetHdr m WHERE m.lookupSetId = :lookupSetId")
	List<String> selectMappingSetNameByLookUpSetId(Long lookupSetId);
	   
}
