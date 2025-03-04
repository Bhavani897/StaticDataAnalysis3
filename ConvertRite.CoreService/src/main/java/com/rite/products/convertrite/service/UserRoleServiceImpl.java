package com.rite.products.convertrite.service;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.rite.products.convertrite.model.XxrUserRoles;
import com.rite.products.convertrite.po.RolesResPo;
import com.rite.products.convertrite.po.SaveUserRolesResPo;
import com.rite.products.convertrite.po.UsersRolesReqPo;
import com.rite.products.convertrite.po.XxrUserRolesRes;
import com.rite.products.convertrite.respository.XxrRolesRepository;
import com.rite.products.convertrite.respository.XxrUserRolesRepository;
import com.rite.products.convertrite.respository.XxrUsersRepository;

@Service
public class UserRoleServiceImpl implements UserRoleService {

	private static final Logger log = LoggerFactory.getLogger(UserRoleServiceImpl.class);

	@Autowired
	XxrUserRolesRepository xxrUserRolesRepository;
	@Autowired
	XxrRolesRepository xxrRolesRepository;
	@Autowired
	XxrUsersRepository xxrUsersRepository;

	@Override
	public SaveUserRolesResPo saveUserRoles(List<UsersRolesReqPo> userRolesReqPoLi) throws Exception {
		log.info("Start of saveUserRoles in service####");
		SaveUserRolesResPo saveUserRolesResPo=new SaveUserRolesResPo();
		List<XxrUserRoles> userRolesRes=new ArrayList<>();
		List<XxrUserRoles> userRolesLi=new ArrayList<>();
		try {
			for(UsersRolesReqPo userRolesReqPo:userRolesReqPoLi) {
			XxrUserRoles xxrUserRoles=new XxrUserRoles();
			xxrUserRoles.setRoleId(userRolesReqPo.getRoleId());
			xxrUserRoles.setUserId(userRolesReqPo.getUserId());
			xxrUserRoles.setCreationDate(userRolesReqPo.getCreationDate());
			
			userRolesLi.add(xxrUserRoles);
			}
			userRolesRes=xxrUserRolesRepository.saveAll(userRolesLi);
			saveUserRolesResPo.setMessage("Successfully saved userroles");
			saveUserRolesResPo.setUserRoles(userRolesRes);
			
		}catch(Exception e) {
			throw new Exception(e.getMessage());
		}
		return saveUserRolesResPo;
	}

	@Override
	public List<XxrUserRolesRes> getUserRoles() throws Exception {
		log.info("Start of getUserRole####");
		List<XxrUserRoles> userRolesLi = new ArrayList<>();
		List<XxrUserRolesRes> userRolesResLi = new ArrayList<>();
		try {
			userRolesLi = xxrUserRolesRepository.findAll();

			userRolesLi.stream().forEach(x -> {
				XxrUserRolesRes xxrUserRolesRes = new XxrUserRolesRes();
				xxrUserRolesRes.setRoleId(x.getRoleId());
				xxrUserRolesRes.setUserId(x.getUserId());
				xxrUserRolesRes.setRoleName(xxrRolesRepository.getRoleName(x.getRoleId()));
				xxrUserRolesRes.setUserName(xxrUsersRepository.getUserName(x.getUserId()));
				userRolesResLi.add(xxrUserRolesRes);
			});

		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return userRolesResLi;
	}

	@Override
	public List<RolesResPo> getRolesByUserName(String userName) throws Exception {
		log.info("Start of getRolesByUserId#########");
			List<XxrUserRoles> userRolesLi=new ArrayList<>();
			List<RolesResPo> rolesResPoLi=new ArrayList<>();
		try {
			
			userRolesLi=xxrUserRolesRepository.getUserRoles(userName);
			userRolesLi.stream().forEach(x->{
				RolesResPo rolesResPo=new RolesResPo();
				rolesResPo.setRoleId(x.getRoleId());
				rolesResPo.setRoleName(xxrRolesRepository.getRoleName(x.getRoleId()));
				
				rolesResPoLi.add(rolesResPo);
			});
		}catch(Exception e) {
			throw new Exception(e.getMessage());
		}
		return rolesResPoLi;
	}

	@Override
	public List<RolesResPo> getRolesByEmailId(String emailId) throws Exception {
		log.info("Start of getRolesByEmailId#########");
		List<XxrUserRoles> userRolesLi = new ArrayList<>();
		List<RolesResPo> rolesResPoLi = new ArrayList<>();
		try {

		userRolesLi=xxrUserRolesRepository.getUserRolesByEmail(emailId);
		userRolesLi.stream().forEach(x->{
			RolesResPo rolesResPo=new RolesResPo();
			rolesResPo.setRoleId(x.getRoleId());
			rolesResPo.setRoleName(xxrRolesRepository.getRoleName(x.getRoleId()));
			
			rolesResPoLi.add(rolesResPo);
		});
	}catch(Exception e) {
		throw new Exception(e.getMessage());
	}
	return rolesResPoLi;
	}
}
