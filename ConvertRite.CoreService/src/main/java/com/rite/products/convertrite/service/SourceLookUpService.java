package com.rite.products.convertrite.service;

import java.util.List;

import org.springframework.http.HttpHeaders;

import com.rite.products.convertrite.model.XxrSrcLookupSets;
import com.rite.products.convertrite.model.XxrSrcLookupValues;
import com.rite.products.convertrite.po.LoadCustomLookupReqPo;
import com.rite.products.convertrite.po.LoadLookupResPo;
import com.rite.products.convertrite.po.LookUpSearchReqPo;
import com.rite.products.convertrite.po.LookUpsetNameResPo;

public interface SourceLookUpService {

	List<XxrSrcLookupSets> searchSrcLookups(LookUpSearchReqPo lookUpSearchReqPo, HttpHeaders header) throws Exception;

	LoadLookupResPo loadLookupsFromSrc(String dbLink) throws Exception;

	List<XxrSrcLookupValues> getSrcLookupValues(Long lookupSetId)throws Exception;

	List<LookUpsetNameResPo> getSrcLookupSetName(String lookupSetName) throws Exception;

	LoadLookupResPo loadCustomLookupsFromSrc(LoadCustomLookupReqPo loadCustomLookupReqPo);

}
