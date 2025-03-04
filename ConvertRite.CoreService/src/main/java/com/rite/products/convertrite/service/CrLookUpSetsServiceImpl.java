package com.rite.products.convertrite.service;

import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.rite.products.convertrite.model.CrLookUpSets;
import com.rite.products.convertrite.po.CrLookUpSetsCreatePo;
import com.rite.products.convertrite.respository.CrLookupSetsRepo;

@Service
@Slf4j
public class CrLookUpSetsServiceImpl implements CrLookUpSetsService {

	@Autowired
	CrLookupSetsRepo crLookupSetsRepo;

	@Override
	public List<CrLookUpSets> getAllLookupSets() {
		return crLookupSetsRepo.findAll();
	}

	@Override
	public Optional<CrLookUpSets> getLookupSetById(Long lookUpSetId) {
		return crLookupSetsRepo.findById(lookUpSetId);
	}

	@Override
	public CrLookUpSets saveLookupSet(CrLookUpSetsCreatePo crLookUpSetsCreatePo) {
		log.info("Start saveLookupSet...");
		try {
			CrLookUpSets crLookUpSets = new CrLookUpSets();

			if (Objects.isNull(crLookUpSetsCreatePo.getLookUpSetId())) {
				boolean exists = crLookupSetsRepo.existsByLookUpSetCode(crLookUpSetsCreatePo.getLookUpSetCode());
				if (exists) {
					throw new RuntimeException("Formula set with code " + crLookUpSetsCreatePo.getLookUpSetCode() + " already exists.");
				}

				crLookUpSets.setCreatedBy("Somu");
				crLookUpSets.setCreationDate(new Date());
			} else {

				crLookUpSets.setLastUpdateBy("Somu");
				crLookUpSets.setLastUpdatedDate(new Date());
				crLookUpSets.setLookUpSetId(crLookUpSetsCreatePo.getLookUpSetId());
			}

			crLookUpSets.setAttribute1(crLookUpSetsCreatePo.getAttribute1());
			crLookUpSets.setAttribute2(crLookUpSetsCreatePo.getAttribute2());
			crLookUpSets.setAttribute3(crLookUpSetsCreatePo.getAttribute3());
			crLookUpSets.setAttribute4(crLookUpSetsCreatePo.getAttribute4());
			crLookUpSets.setAttribute5(crLookUpSetsCreatePo.getAttribute5());
			crLookUpSets.setLookUpSetName(crLookUpSetsCreatePo.getLookUpSetName());
			crLookUpSets.setLookUpSetCode(crLookUpSetsCreatePo.getLookUpSetCode());
			crLookUpSets.setLookUpFlag(crLookUpSetsCreatePo.getLookUpFlag());
			crLookUpSets.setRelatedTo(crLookUpSetsCreatePo.getRelatedTo());
			crLookUpSets.setDescription(crLookUpSetsCreatePo.getDescription());


			crLookUpSets = crLookupSetsRepo.save(crLookUpSets);

			return crLookUpSets;
		} catch (Exception e) {
			log.error("Error occurred while saving lookup set: {}", e.getMessage());
			throw new RuntimeException("Error occurred while saving lookup set: " + e.getMessage());
		}
	}

	@Override
	public String deleteLookupSet(Long lookUpSetId) {
		crLookupSetsRepo.deleteById(lookUpSetId);
		return "Success";
	}

	public CrLookUpSets getLookupSetByCode(String lookUpSetCode) {

		return crLookupSetsRepo.getLookupSetByCode(lookUpSetCode);
	}

}
