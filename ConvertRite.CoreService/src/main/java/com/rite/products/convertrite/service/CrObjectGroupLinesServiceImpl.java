package com.rite.products.convertrite.service;

import java.util.Date;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import com.rite.products.convertrite.model.CrProjectsObjects;
import com.rite.products.convertrite.po.BasicResPo;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import com.rite.products.convertrite.model.CrObjectGroupLines;
import com.rite.products.convertrite.model.CrObjectGroupLinesView;
import com.rite.products.convertrite.po.CrObjectGroupLinesReqPo;
import com.rite.products.convertrite.respository.CrObjectGroupLinesRepo;

@Service
@Slf4j
public class CrObjectGroupLinesServiceImpl implements CrObjectGroupLinesService {

	@Autowired
	CrObjectGroupLinesRepo crObjectGroupLinesRepo;

	@Override
	public List<CrObjectGroupLines> getAllByGroupId(Long groupId) {

		return crObjectGroupLinesRepo.getobjectLinesByGroupId(groupId);
	}

	@Override
	public List<CrObjectGroupLines> saveAllObjectGroupLinesByGroupId(List<CrObjectGroupLinesReqPo> lineList,
																	 Long groupId) {
		List<CrObjectGroupLines> insertList = new ArrayList<CrObjectGroupLines>();
		List<CrObjectGroupLines> updateList = new ArrayList<CrObjectGroupLines>();
		List<Long> deleteList = new ArrayList<Long>();

		for (CrObjectGroupLinesReqPo crObjectGroupLinesReqPo : lineList) {
			if (crObjectGroupLinesReqPo.getInsertOrDelete().equalsIgnoreCase("D")) {
				deleteList.add(crObjectGroupLinesReqPo.getObjGrpLineId());
			} else {
				CrObjectGroupLines crObjectGroupLines = new CrObjectGroupLines();
				crObjectGroupLines.setAttribute1(crObjectGroupLinesReqPo.getAttribute1());
				crObjectGroupLines.setAttribute2(crObjectGroupLinesReqPo.getAttribute2());
				crObjectGroupLines.setAttribute3(crObjectGroupLinesReqPo.getAttribute3());
				crObjectGroupLines.setAttribute4(crObjectGroupLinesReqPo.getAttribute4());
				crObjectGroupLines.setAttribute5(crObjectGroupLinesReqPo.getAttribute5());
				crObjectGroupLines.setGroupId(crObjectGroupLinesReqPo.getGroupId());
				crObjectGroupLines.setObjectId(crObjectGroupLinesReqPo.getObjectId());
				crObjectGroupLines.setSequence(crObjectGroupLinesReqPo.getSequence());
				if (Objects.isNull(crObjectGroupLinesReqPo.getObjGrpLineId())) {
					crObjectGroupLines.setCreatedBy("Somu");
					crObjectGroupLines.setCreationDate(new Date());
					insertList.add(crObjectGroupLines);
				} else {
					crObjectGroupLines.setLastUpdateBy("Somu");
					crObjectGroupLines.setLastUpdatedDate(new Date());
					crObjectGroupLines.setObjGrpLineId(crObjectGroupLinesReqPo.getObjGrpLineId());
					updateList.add(crObjectGroupLines);
				}
			}
		}
		if (deleteList.size() > 0) {
			try {
				crObjectGroupLinesRepo.deleteAllById(deleteList);
			} catch (Exception e) {
			}
		}
		if (insertList.size() > 0) {
			try {
				crObjectGroupLinesRepo.saveAll(insertList);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		if (updateList.size() > 0) {
			try {
				crObjectGroupLinesRepo.saveAll(updateList);
			} catch (Exception e) {
				e.printStackTrace();
			}

		}
		return crObjectGroupLinesRepo.getobjectLinesByGroupId(groupId);
	}

	@Override
	public List<CrObjectGroupLinesView> saveAllObjectGrpLinesByGroupId(List<CrObjectGroupLinesReqPo> lineList,
																	   Long groupId) {
		List<CrObjectGroupLines> insertList = new ArrayList<CrObjectGroupLines>();
		List<CrObjectGroupLines> updateList = new ArrayList<CrObjectGroupLines>();
		List<Long> deleteList = new ArrayList<Long>();

		for (CrObjectGroupLinesReqPo crObjectGroupLinesReqPo : lineList) {
			if (crObjectGroupLinesReqPo.getInsertOrDelete().equalsIgnoreCase("D")) {
				deleteList.add(crObjectGroupLinesReqPo.getObjGrpLineId());
			} else {
				CrObjectGroupLines crObjectGroupLines = new CrObjectGroupLines();
				crObjectGroupLines.setAttribute1(crObjectGroupLinesReqPo.getAttribute1());
				crObjectGroupLines.setAttribute2(crObjectGroupLinesReqPo.getAttribute2());
				crObjectGroupLines.setAttribute3(crObjectGroupLinesReqPo.getAttribute3());
				crObjectGroupLines.setAttribute4(crObjectGroupLinesReqPo.getAttribute4());
				crObjectGroupLines.setAttribute5(crObjectGroupLinesReqPo.getAttribute5());
				crObjectGroupLines.setGroupId(crObjectGroupLinesReqPo.getGroupId());
				crObjectGroupLines.setObjectId(crObjectGroupLinesReqPo.getObjectId());
				crObjectGroupLines.setSequence(crObjectGroupLinesReqPo.getSequence());
				if (Objects.isNull(crObjectGroupLinesReqPo.getObjGrpLineId())) {
					crObjectGroupLines.setCreatedBy("Somu");
					crObjectGroupLines.setCreationDate(new Date());
					insertList.add(crObjectGroupLines);
				} else {
					crObjectGroupLines.setLastUpdateBy("Somu");
					crObjectGroupLines.setLastUpdatedDate(new Date());
					crObjectGroupLines.setObjGrpLineId(crObjectGroupLinesReqPo.getObjGrpLineId());
					updateList.add(crObjectGroupLines);
				}
			}
		}
		if (deleteList.size() > 0) {
			try {
				crObjectGroupLinesRepo.deleteAllById(deleteList);
			} catch (Exception e) {
				log.error(e.getMessage());
			}
		}
		if (insertList.size() > 0) {
			try {
				crObjectGroupLinesRepo.saveAll(insertList);
			} catch (Exception e) {
				log.error(e.getMessage());
			}
		}
		if (updateList.size() > 0) {
			try {
				crObjectGroupLinesRepo.saveAll(updateList);
			} catch (Exception e) {
				log.error(e.getMessage());
			}

		}
		return crObjectGroupLinesRepo.getObjectLinesViewByGroupId(groupId);
	}

	@Override
	public String deleteAllLinesByGroupId(Long groupId) {
		crObjectGroupLinesRepo.deleteAllLinesByGroupId(groupId);

		return "Success";

	}

	@Override
	public List<CrObjectGroupLinesView> getAllLinesViewByGroupId(Long groupId) {
		return crObjectGroupLinesRepo.getObjectLinesViewByGroupId(groupId);
	}

	@Override
	public BasicResPo deleteLinesByobjGrpLineId(Long objGrpLineId) {
		BasicResPo resPo = new BasicResPo();
		try {
			crObjectGroupLinesRepo.deleteLinesByobjGrpLineId(objGrpLineId);
			resPo.setStatus("Success");
			resPo.setMessage("Successfully deleted");
			resPo.setStatusCode(HttpStatus.valueOf(HttpStatus.OK.value()));
		} catch (Exception e) {
			log.error("Error in deleteLinesByobjGrpLineId--->"+ e.getMessage());
			resPo.setStatus("Error");
			resPo.setMessage("Failed to delete");
			resPo.setStatusCode(HttpStatus.valueOf(HttpStatus.INTERNAL_SERVER_ERROR.value()));
		}
		return resPo;
	}
	@Override
	public BasicResPo getUngroupedObjects(Long projectId, String parentObjectCode) {
		BasicResPo resPo = new BasicResPo();
		try {
			List<CrProjectsObjects> res = crObjectGroupLinesRepo.getUngroupedObjects(projectId, parentObjectCode);
			resPo.setStatus("Success");
			resPo.setMessage("Successfully fetched Ungrouped Objects");
			resPo.setPayload(res);
			resPo.setStatusCode(HttpStatus.valueOf(HttpStatus.OK.value()));
		} catch (Exception e) {
			log.error("Error in getUngroupedObjects--->" + e);
			resPo.setPayload(e);
			resPo.setStatus("Error");
			resPo.setMessage("Error in getting UngroupedObjects");
			resPo.setStatusCode(HttpStatus.valueOf(HttpStatus.INTERNAL_SERVER_ERROR.value()));
		}
		return resPo;
	}
}

