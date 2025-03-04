package com.rite.products.convertrite.service;

import java.util.List;

import com.rite.products.convertrite.po.RoleObjectLinkResPo;
import com.rite.products.convertrite.po.RoleObjectReqPo;
import com.rite.products.convertrite.po.RoleObjectRes;

public interface RoleObjectService {

	RoleObjectLinkResPo saveRoleObject(List<RoleObjectReqPo> roleObjectReqPo) throws Exception;

	List<RoleObjectRes> getRoleObjects() throws Exception;

}
