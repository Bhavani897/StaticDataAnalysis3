package com.rite.products.convertrite.service;

import java.util.List;
import java.util.Optional;

import com.rite.products.convertrite.model.CrFormulaSets;
import com.rite.products.convertrite.po.BasicResponsePo;
import com.rite.products.convertrite.po.CrFormulaSetsCreateReqPo;

public interface CrFormulaSetsService {

	public List<CrFormulaSets> getAllFormulaSets();
	
	public Optional<CrFormulaSets> getFormulaSetById(Long FormulaSetId);
	
	public CrFormulaSets saveFormulaSet(CrFormulaSetsCreateReqPo createReqPo);
	
	public BasicResponsePo deleteFormulaSetById(String mappingType, Long formulaId);
	
	public CrFormulaSets getFormulaByCode(String formulaCode);
}
