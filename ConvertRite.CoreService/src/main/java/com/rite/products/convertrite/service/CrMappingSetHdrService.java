package com.rite.products.convertrite.service;

import java.util.List;
import java.util.Optional;



import com.rite.products.convertrite.model.CrMappingSetHdr;
import com.rite.products.convertrite.po.CrMappingSetHdrCreateReqPo;


public interface CrMappingSetHdrService {

	public List<CrMappingSetHdr> getAllMappingsets();
	   
	public Optional<CrMappingSetHdr> findById(long mappingSetId);
	   
	public CrMappingSetHdr saveMappingSetHdr(CrMappingSetHdrCreateReqPo crMappingSetHdr);
	   
	public String deleteById(long mapingSetId);
	
	public CrMappingSetHdr findByMapCode(String mappingSetCode);
}
