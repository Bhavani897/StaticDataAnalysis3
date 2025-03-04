package com.rite.products.convertrite.service;

import java.util.List;

import com.rite.products.convertrite.model.XxrRoles;
import com.rite.products.convertrite.po.RolesReqPo;
import com.rite.products.convertrite.po.XxrRolesResPo;

public interface RoleService {

	XxrRolesResPo saveRoles(RolesReqPo rolesReqPo) throws Exception;

	List<XxrRoles> getRoles()throws Exception;

}
