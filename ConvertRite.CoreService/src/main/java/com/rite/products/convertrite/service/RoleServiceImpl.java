package com.rite.products.convertrite.service;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.rite.products.convertrite.model.XxrRoles;
import com.rite.products.convertrite.po.RolesReqPo;
import com.rite.products.convertrite.po.XxrRolesResPo;
import com.rite.products.convertrite.respository.XxrRolesRepository;

@Service
public class RoleServiceImpl implements RoleService {

	private static final Logger log = LoggerFactory.getLogger(RoleServiceImpl.class);

	@Autowired
	XxrRolesRepository xxrRolesRepository;

	@Override
	public XxrRolesResPo saveRoles(RolesReqPo rolesReqPo) throws Exception {
		log.info("Start of saveRoles####");
		XxrRolesResPo xxrRolesResPo = new XxrRolesResPo();
		XxrRoles roleRes = new XxrRoles();
		try {
			XxrRoles roles = xxrRolesRepository.findByRoleNameIgnoreCase(rolesReqPo.getRoleName());
			if (roles != null) {				
				xxrRolesResPo.setMessage("This role is already Exist");
				roleRes.setId(roles.getId());
				roleRes.setRoleName(roles.getRoleName());
				roleRes.setIsReadOnly(roles.getIsReadOnly());
				roleRes.setCreationDate(roles.getCreationDate());
				roleRes.setEndDate(roles.getEndDate());
				roleRes.setLastUpdateDate(roles.getLastUpdateDate());
				xxrRolesResPo.setXxrRoles(roleRes);				
			} else {
				XxrRoles newRole = new XxrRoles();
				newRole.setRoleName(rolesReqPo.getRoleName());
				newRole.setIsReadOnly(rolesReqPo.getIsReadOnly());
				newRole.setCreationDate(rolesReqPo.getCreationDate());
				newRole.setEndDate(rolesReqPo.getEndDate());
				newRole.setLastUpdateDate(rolesReqPo.getLastUpdateDate());
				roleRes = xxrRolesRepository.save(newRole);
				xxrRolesResPo.setMessage("Successfully saved roles");
			}
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		xxrRolesResPo.setXxrRoles(roleRes);
//		xxrRolesResPo.setMessage("Successfully saved roles");
		return xxrRolesResPo;
	}

	@Override
	public List<XxrRoles> getRoles() throws Exception {
		log.info("Start of getRoles####");
		List<XxrRoles> rolesLi = new ArrayList<>();
		try {
			rolesLi=xxrRolesRepository.findAll();
		}catch(Exception e) {
			throw new Exception(e.getMessage());
		}
		return rolesLi;
	}

}
