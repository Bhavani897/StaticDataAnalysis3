package com.rite.products.convertrite.service;

import java.util.List;

import com.rite.products.convertrite.po.RolesResPo;
import com.rite.products.convertrite.po.SaveUserRolesResPo;
import com.rite.products.convertrite.po.UsersRolesReqPo;
import com.rite.products.convertrite.po.XxrUserRolesRes;

public interface UserRoleService {

	SaveUserRolesResPo saveUserRoles(List<UsersRolesReqPo> userRolesReqPo) throws Exception;

	List<XxrUserRolesRes> getUserRoles()throws Exception;

	List<RolesResPo> getRolesByUserName(String userName)throws Exception;

	List<RolesResPo> getRolesByEmailId(String emailId)throws Exception;

}
