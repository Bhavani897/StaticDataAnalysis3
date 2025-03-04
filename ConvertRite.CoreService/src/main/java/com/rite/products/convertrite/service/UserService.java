package com.rite.products.convertrite.service;

import java.util.List;

import com.rite.products.convertrite.model.XxrUsers;
import com.rite.products.convertrite.po.UserRegistrationStatusRes;
import com.rite.products.convertrite.po.XxrUserResPo;

public interface UserService {

	XxrUserResPo registerUser(XxrUsers xxrUsers) throws Exception;

	UserRegistrationStatusRes userRegistrationStatus(String userName)throws Exception;

	List<XxrUsers> getUsers()throws Exception;

	UserRegistrationStatusRes userRegistrationStatusByEmail(String email)throws Exception;

}
