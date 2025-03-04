package com.rite.products.convertrite.service;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import com.rite.products.convertrite.po.BasicResponsePo;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.rite.products.convertrite.model.CrHookUsages;
import com.rite.products.convertrite.po.CrHookUsagesCreateReqPo;
import com.rite.products.convertrite.respository.CrHookUsagesRepo;

@Service
@Slf4j
public class CrHookUsageServiceImpl implements CrHookUsageService {
	@Autowired
	CrHookUsagesRepo crHookUsagesRepo;

	@Override
	public Optional<CrHookUsages> gethookUsageById(Long hookUsageId) {

		return crHookUsagesRepo.findById(hookUsageId);
	}

	@Override
	public List<CrHookUsages> getHookUsagesByTemplateId(Long cloudTemplateId) {
		return crHookUsagesRepo.getHooksByTemplateId(cloudTemplateId);
	}

	@Override
	public String deleteHookUsage(Long hookUsageId) {
		crHookUsagesRepo.deleteById(hookUsageId);
		return "Success";
	}

	@Override
	public List<CrHookUsages> saveHookUsage(List<CrHookUsagesCreateReqPo> crHookUsagesCreateReqPoList,
			Long cloudTemplateId) {
		List<Long> deleteList = new ArrayList<Long>();
		List<CrHookUsages> insertList = new ArrayList<CrHookUsages>();
		List<CrHookUsages> updateList = new ArrayList<CrHookUsages>();
		for (CrHookUsagesCreateReqPo createReqPo : crHookUsagesCreateReqPoList) {
			if (createReqPo.getInsertOrDelete().equalsIgnoreCase("D")) {
				deleteList.add(createReqPo.getHookUsageId());
			} else {
				CrHookUsages crHookUsages = new CrHookUsages();
				crHookUsages.setAttribute1(createReqPo.getAttribute1());
				crHookUsages.setAttribute2(createReqPo.getAttribute2());
				crHookUsages.setAttribute3(createReqPo.getAttribute3());
				crHookUsages.setAttribute4(createReqPo.getAttribute4());
				crHookUsages.setAttribute5(createReqPo.getAttribute5());
				crHookUsages.setHookId(createReqPo.getHookId());
				crHookUsages.setTemplateId(createReqPo.getTemplateId());
				crHookUsages.setUsageType(createReqPo.getUsageType());

				if (Objects.isNull(createReqPo.getHookUsageId())) {
					crHookUsages.setCreatedBy("Somu");
					crHookUsages.setCreationDate(new Date());
					insertList.add(crHookUsages);
				} else {
					crHookUsages.setLastUpdateBy("Somu");
					crHookUsages.setLastUpdatedDate(new Date());
					crHookUsages.setHookUsageId(createReqPo.getHookUsageId());
					updateList.add(crHookUsages);
				}
			}

		}
		if (deleteList.size() > 0) {
			try {
				crHookUsagesRepo.deleteAllById(deleteList);
			} catch (Exception e) {
				log.error(e.getMessage());
			}
		}
		if (insertList.size() > 0) {
			try {
				crHookUsagesRepo.saveAll(insertList);
			} catch (Exception e) {
				log.error(e.getMessage());
			}
		}
		if (updateList.size() > 0) {
			try {
				crHookUsagesRepo.saveAll(updateList);
			} catch (Exception e) {
				log.error(e.getMessage());
			}

		}
		return crHookUsagesRepo.getHooksByTemplateId(cloudTemplateId);
	}


	@Override
	public BasicResponsePo deleteHookUsageById(Long hookUsageId) {
		BasicResponsePo respo = new BasicResponsePo();
		crHookUsagesRepo.deleteById(hookUsageId);
		respo.setMessage("UserHook deleted Successfully");
		return respo;
	}

}
