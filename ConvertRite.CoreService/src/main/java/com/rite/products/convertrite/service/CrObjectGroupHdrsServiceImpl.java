package com.rite.products.convertrite.service;

import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.rite.products.convertrite.model.CrObjectGroupHdrs;
import com.rite.products.convertrite.model.CrObjectGroupHdrsView;
import com.rite.products.convertrite.po.CrObjectGroupHdrReqPo;
import com.rite.products.convertrite.respository.CrObjectGroupHdrsRepo;

@Service
public class CrObjectGroupHdrsServiceImpl implements CrObjectGroupHdrsService{

	@Autowired
	CrObjectGroupHdrsRepo crObjectGroupHdrsRepo;
	
	@Override
	public List<CrObjectGroupHdrs> getAllObjectGroups() {
		
		return crObjectGroupHdrsRepo.findAll();
	}

	@Override
	public List<CrObjectGroupHdrsView> getAllObjectGroupHdrsView() {
		
		return crObjectGroupHdrsRepo.getAllObjectGroupsHdrs();
	}
	@Override
	public Optional<CrObjectGroupHdrs> getObjectGroupHdrsById(Long objectGroupId) {
		
		return crObjectGroupHdrsRepo.findById(objectGroupId);
	}

	@Override
	public String deleteObjectGroup(Long objectGroupId) {
		crObjectGroupHdrsRepo.deleteById(objectGroupId);
		return "Success";
	}

	@Override
	public CrObjectGroupHdrs saveObjectGroupHdr(CrObjectGroupHdrReqPo crObjectGroupHdrReqPo) {
		CrObjectGroupHdrs crObjectGroupHdrs = new CrObjectGroupHdrs();
		crObjectGroupHdrs.setAttribute1(crObjectGroupHdrReqPo.getAttribute1());
		crObjectGroupHdrs.setAttribute2(crObjectGroupHdrReqPo.getAttribute2());
		crObjectGroupHdrs.setAttribute3(crObjectGroupHdrReqPo.getAttribute3());
		crObjectGroupHdrs.setAttribute4(crObjectGroupHdrReqPo.getAttribute4());
		crObjectGroupHdrs.setAttribute5(crObjectGroupHdrReqPo.getAttribute5());
		crObjectGroupHdrs.setDescription(crObjectGroupHdrReqPo.getDescription());
		crObjectGroupHdrs.setGroupCode(crObjectGroupHdrReqPo.getGroupCode());
		crObjectGroupHdrs.setGroupName(crObjectGroupHdrReqPo.getGroupName());
		crObjectGroupHdrs.setParentObjectId(crObjectGroupHdrReqPo.getParentObjectId());
		crObjectGroupHdrs.setProjectId(crObjectGroupHdrReqPo.getProjectId());
		if(Objects.isNull(crObjectGroupHdrReqPo.getGroupId())) {
			crObjectGroupHdrs.setCreatedBy("Somu");
			crObjectGroupHdrs.setCreationDate(new Date());
		} else {
			crObjectGroupHdrs.setLastUpdateBy("Somu");
			crObjectGroupHdrs.setLastUpdatedDate(new Date());
			crObjectGroupHdrs.setGroupId(crObjectGroupHdrReqPo.getGroupId());
		}
		crObjectGroupHdrsRepo.save(crObjectGroupHdrs);
		return crObjectGroupHdrsRepo.findByGroupCode(crObjectGroupHdrs.getGroupCode());
	}



}
