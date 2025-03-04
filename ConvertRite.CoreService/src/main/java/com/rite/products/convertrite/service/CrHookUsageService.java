package com.rite.products.convertrite.service;

import java.util.List;
import java.util.Optional;

import com.rite.products.convertrite.model.CrHookUsages;
import com.rite.products.convertrite.po.BasicResponsePo;
import com.rite.products.convertrite.po.CrHookUsagesCreateReqPo;

public interface CrHookUsageService {

	public Optional<CrHookUsages> gethookUsageById(Long hookUsageId);
	
	public List<CrHookUsages> getHookUsagesByTemplateId(Long cloudTemplateId);
	
	public String deleteHookUsage(Long hookUsageId);
	
	public List<CrHookUsages> saveHookUsage(List<CrHookUsagesCreateReqPo> crHookUsagesCreateReqPoList, Long cloudTemplateId);

	public BasicResponsePo deleteHookUsageById(Long hookUsageId);
}
