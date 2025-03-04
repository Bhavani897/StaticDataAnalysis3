package com.rite.products.convertrite.service;

import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.springframework.http.HttpHeaders;

import com.rite.products.convertrite.model.XxrCloudMasterLookUpSet;
import com.rite.products.convertrite.model.XxrCloudMasterLookupValues;
import com.rite.products.convertrite.model.XxrLookUpValues;
import com.rite.products.convertrite.model.XxrLookupSets;
import com.rite.products.convertrite.po.LookUpSearchReqPo;
import com.rite.products.convertrite.po.LookUpsetNameResPo;
import com.rite.products.convertrite.po.SaveCloudLookUpSetColumnsPo;
import com.rite.products.convertrite.po.SaveCloudLookUpSetColumnsResPo;
import com.rite.products.convertrite.po.SaveCloudLookUpSetHeadersPo;
import com.rite.products.convertrite.po.SaveCloudLookUpSetHeadersResPo;
import com.rite.products.convertrite.po.SaveLookUpSetPo;
import com.rite.products.convertrite.po.SaveLookUpValuesPo;
import com.rite.products.convertrite.po.SaveLookUpValuesResPo;

public interface CloudLookUpService {

	List<XxrCloudMasterLookUpSet> getLookupSetHeaders() throws Exception;

	List<XxrCloudMasterLookupValues> getLookupSetValues(Long lookUpSetId) throws Exception;

	SaveCloudLookUpSetHeadersResPo saveCloudLookUpSetHeaders(List<SaveCloudLookUpSetHeadersPo> cloudLookUpSetHeadersPo)
			throws Exception;

	SaveCloudLookUpSetColumnsResPo saveCloudLookUpSetValues(List<SaveCloudLookUpSetColumnsPo> cloudLookUpSetColumnsPo)
			throws Exception;

	List<String> getColumnNames() throws Exception;

	List<XxrLookupSets> getLookupSet() throws Exception;

	List<XxrLookUpValues> getlookupvalues(Long lookUpSetId) throws Exception;

	SaveCloudLookUpSetHeadersResPo saveLookUpSetHeaders(List<SaveLookUpSetPo> lookUpSetHeadersPo,
			HttpServletRequest request) throws Exception;

	SaveLookUpValuesResPo saveLookUpValues(List<SaveLookUpValuesPo> lookUpValuesPo, HttpServletRequest request)
			throws Exception;

	List<LookUpsetNameResPo> getLookupSetName(String lookupSetName) throws Exception;

	List<XxrLookupSets> searchByOperator(LookUpSearchReqPo lookUpSearchReqPo, HttpHeaders header) throws Exception;

	List<XxrLookUpValues> getlookupvaluesByVal(Long lookUpSetId, String lookUpValue);

}
