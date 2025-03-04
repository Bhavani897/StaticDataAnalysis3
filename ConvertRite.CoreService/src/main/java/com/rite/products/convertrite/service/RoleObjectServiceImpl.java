package com.rite.products.convertrite.service;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.rite.products.convertrite.model.XxrRoleObjectLinks;
import com.rite.products.convertrite.po.RoleObjectLinkResPo;
import com.rite.products.convertrite.po.RoleObjectReqPo;
import com.rite.products.convertrite.po.RoleObjectRes;
import com.rite.products.convertrite.respository.XxrLookUpValuesRepository;
import com.rite.products.convertrite.respository.XxrRoleObjectLinksRepository;
import com.rite.products.convertrite.respository.XxrRolesRepository;

@Service
public class RoleObjectServiceImpl implements RoleObjectService {

	private static final Logger log = LoggerFactory.getLogger(RoleObjectServiceImpl.class);

	@Autowired
	XxrRoleObjectLinksRepository xxrRoleObjectLinksRepository;
	@Autowired
	XxrRolesRepository xxrRolesRepository;
	@Autowired
	XxrLookUpValuesRepository xxrLookUpValuesRepository;

	@Override
	public RoleObjectLinkResPo saveRoleObject(List<RoleObjectReqPo> roleObjectReqPoLi) throws Exception {
		log.info("Start of saveRoleObjec in service####");
		RoleObjectLinkResPo roleObjectLinkResPo = new RoleObjectLinkResPo();
		List<XxrRoleObjectLinks> xxrRoleObjectLi = new ArrayList<>();
		List<XxrRoleObjectLinks> xxrRoleObjectResLi = new ArrayList<>();
		try {
			roleObjectReqPoLi.stream().forEach(x -> {
				XxrRoleObjectLinks xxrRoleObjectLinks = new XxrRoleObjectLinks();
				xxrRoleObjectLinks.setRoleId(x.getRoleId());
				//xxrRoleObjectLinks.setPodId(x.getPodId());
				xxrRoleObjectLinks.setProjectId(x.getProjectId());
				xxrRoleObjectLinks.setParentObjectId(x.getParentObjectId());
				xxrRoleObjectLinks.setEnableFlag(x.getEnableFlag());

				xxrRoleObjectLi.add(xxrRoleObjectLinks);
			});
			xxrRoleObjectResLi = xxrRoleObjectLinksRepository.saveAll(xxrRoleObjectLi);
			roleObjectLinkResPo.setRoleObjectLink(xxrRoleObjectResLi);
			roleObjectLinkResPo.setMessage("Successfully saved RoleObject");
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return roleObjectLinkResPo;
	}

	@Override
	public List<RoleObjectRes> getRoleObjects() throws Exception {
		log.info("Start of getRoleObjects######");
		List<RoleObjectRes> roleObjectLi = new ArrayList<>();
		List<XxrRoleObjectLinks> xxrRoleObjectLi = new ArrayList<>();
		try {
			xxrRoleObjectLi = xxrRoleObjectLinksRepository.findAll();
			xxrRoleObjectLi.stream().forEach(x -> {
				RoleObjectRes roleObjectRes = new RoleObjectRes();
				String projectName = xxrLookUpValuesRepository.getValueById(x.getProjectId());
				String parentObjectCode = xxrLookUpValuesRepository.getValueById(x.getParentObjectId());
				//String podName = xxrLookUpValuesRepository.getValueById(x.getPodId());

				roleObjectRes.setRoleId(x.getRoleId());
				roleObjectRes.setRoleName(xxrRolesRepository.getRoleName(x.getRoleId()));
				//roleObjectRes.setPodId(x.getPodId());
				//roleObjectRes.setPodName(podName);
				roleObjectRes.setProjectId(x.getProjectId());
				roleObjectRes.setProjectName(projectName);
				roleObjectRes.setParentObjectId(x.getParentObjectId());
				roleObjectRes.setParentObjectCode(parentObjectCode);
				roleObjectRes.setEnableFlag(x.getEnableFlag());

				roleObjectLi.add(roleObjectRes);
			});
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return roleObjectLi;
	}

}
