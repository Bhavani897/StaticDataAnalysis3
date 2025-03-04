package com.rite.products.convertrite.service;

import java.util.List;
import java.util.Optional;

import com.rite.products.convertrite.model.CrLookUpSets;
import com.rite.products.convertrite.po.CrLookUpSetsCreatePo;

public interface CrLookUpSetsService {

	public List<CrLookUpSets> getAllLookupSets();

	public Optional<CrLookUpSets> getLookupSetById(Long lookUpSetId);

	public CrLookUpSets saveLookupSet(CrLookUpSetsCreatePo crLookUpSetsCreatePo);

	public String deleteLookupSet(Long lookUpSetId);

}
