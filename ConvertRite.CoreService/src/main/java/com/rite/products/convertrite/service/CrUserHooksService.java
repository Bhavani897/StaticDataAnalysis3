package com.rite.products.convertrite.service;

import java.util.List;
import java.util.Optional;
import com.rite.products.convertrite.po.BasicResponsePo;
import com.rite.products.convertrite.model.CrUserHooks;
import com.rite.products.convertrite.po.CrUserHooksCreateReqPo;

public interface CrUserHooksService {
	
	public List<CrUserHooks> getAllUserHooks();
	
	public Optional<CrUserHooks> getUserHookById(Long hookId);
	
	public CrUserHooks getUserHookByCode(String hookCode);
	
	BasicResponsePo deleteUserHook(Long hookId);
	
	public CrUserHooks saveUserHook(CrUserHooksCreateReqPo  createReqPo);

}
