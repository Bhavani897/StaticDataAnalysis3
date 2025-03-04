package com.rite.products.convertrite.service;

import java.util.List;

import com.rite.products.convertrite.po.BuLovPo;
import com.rite.products.convertrite.po.CommonLovPo;
import com.rite.products.convertrite.po.LovValuesPo;
import com.rite.products.convertrite.po.ObjectCodeLovPo;
import com.rite.products.convertrite.po.ParentObjectCodeResPo;
import com.rite.products.convertrite.po.PodsPo;
import com.rite.products.convertrite.po.ProjectEntryLovPo;
import com.rite.products.convertrite.po.ProjectsPo;

public interface LovService {

	ProjectEntryLovPo getProjectEntryLovs(String[] lovValues) throws Exception;

	List<Object> getPods() throws Exception;

	CommonLovPo getCommonLov(Long podId) throws Exception;

	List<LovValuesPo> getRelatedToLov(String lovValue)throws Exception;

	List<LovValuesPo> getActualValueLov(String relatedTo) throws Exception;

	List<BuLovPo> getBuLov() throws Exception;

	List<LovValuesPo> getLovByLookUpSetName(String lookupSetName)throws Exception;

	List<ObjectCodeLovPo> getObjectCodes(Long parentObjectId)throws Exception;

	List<ObjectCodeLovPo> getAllObjectCodes()throws Exception;

	List<ParentObjectCodeResPo> getParentObjectCodeByRole(String role, Long podId, Long projectId) throws Exception;

	List<PodsPo> getPodsByRole(String role)throws Exception;

	List<ProjectsPo> getProjectsByRole(String role, Long podId)throws Exception;

	List<ParentObjectCodeResPo> getParentObjectCodes(String role)throws Exception;

	List<ProjectsPo> getProjectsByRole(String role)throws Exception;

	List<ParentObjectCodeResPo> getParentObjectCodeByRole(String role, Long projectId) throws Exception;


}
