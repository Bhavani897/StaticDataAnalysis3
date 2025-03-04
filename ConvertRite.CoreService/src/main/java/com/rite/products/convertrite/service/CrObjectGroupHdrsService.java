package com.rite.products.convertrite.service;

import java.util.List;
import java.util.Optional;

import com.rite.products.convertrite.model.CrObjectGroupHdrs;
import com.rite.products.convertrite.model.CrObjectGroupHdrsView;
import com.rite.products.convertrite.po.CrObjectGroupHdrReqPo;

public interface CrObjectGroupHdrsService {

	public List<CrObjectGroupHdrs> getAllObjectGroups();
	
	public Optional<CrObjectGroupHdrs> getObjectGroupHdrsById(Long objectGroupId);
	
	public String deleteObjectGroup(Long objectGroupId);
	
	public CrObjectGroupHdrs saveObjectGroupHdr(CrObjectGroupHdrReqPo crObjectGroupHdrReqPo);
	
	public List<CrObjectGroupHdrsView> getAllObjectGroupHdrsView();
}
