package com.rite.products.convertrite.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import com.rite.products.convertrite.model.CrMappingSetValues;
import com.rite.products.convertrite.po.BasicResponsePo;
import com.rite.products.convertrite.po.CrMappingSetValuesCreateReqPo;
import com.rite.products.convertrite.po.LoadMappingValuesResPo;
import org.springframework.web.multipart.MultipartFile;

public interface CrMappingSetValuesService {
	List<CrMappingSetValues> getValuesBySetId(Long mappingSetId);
	
	List<CrMappingSetValues> saveAllMappingValues(ArrayList<CrMappingSetValuesCreateReqPo> mappingValuesReqPoList, Long mapsetHdrId);
	
	Optional<CrMappingSetValues> findByValueId(int mappingValueId);
	
	String deleteByValueId(int mappingValueId);
	
	String saveMappingSetValues(CrMappingSetValuesCreateReqPo crMappingSetValuesCreateReqPo);

	BasicResponsePo deleteAllMappingValuesBySetId(String mappingType, Long mappingSetId);

	LoadMappingValuesResPo loadMappingValues(MultipartFile file, Long mappingSetId) throws Exception;
}
