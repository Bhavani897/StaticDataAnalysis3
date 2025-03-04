package com.rite.products.convertrite.service;

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.rite.products.convertrite.exception.ValidationException;
import com.rite.products.convertrite.model.XxrUserHooksDetails;
import com.rite.products.convertrite.model.XxrUserHooksDetailsView;
import com.rite.products.convertrite.po.SaveUserHooksResPo;
import com.rite.products.convertrite.po.UserHooksDetailsReqPo;
import com.rite.products.convertrite.respository.XxrUserHooksDetailsRepository;
import com.rite.products.convertrite.respository.XxrUserHooksDetailsViewRepository;

@Service
public class UserHookServiceImpl implements UserHookService {

	private static final Logger log = LoggerFactory.getLogger(UserHookServiceImpl.class);

	@Autowired
	XxrUserHooksDetailsRepository xxrUserHooksDetailsRepository;
	@Autowired
	XxrUserHooksDetailsViewRepository xxrUserHooksDetailsViewRepository;

	@Override
	public SaveUserHooksResPo saveUserHooksDetails(UserHooksDetailsReqPo userHooksDetailsReqPo)
			throws ValidationException {
		log.info("Start of saveUserHooksDetails in service ####");
		SaveUserHooksResPo saveUserHooksResPo = new SaveUserHooksResPo();
		XxrUserHooksDetails xxrUserHooksDetailsRes = new XxrUserHooksDetails();
		if (userHooksDetailsReqPo.getId() == null) {
			XxrUserHooksDetails xxrUserHooksDetails = new XxrUserHooksDetails();
			xxrUserHooksDetails.setCldTemplateId(userHooksDetailsReqPo.getCldTemplateId());
			xxrUserHooksDetails.setCldTemplateName(userHooksDetailsReqPo.getCldTemplateName());
			xxrUserHooksDetails.setSrcTemplateId(userHooksDetailsReqPo.getSrcTemplateId());
			xxrUserHooksDetails.setSrcTemplateName(userHooksDetailsReqPo.getSrcTemplateName());
			xxrUserHooksDetails.setPodId(userHooksDetailsReqPo.getPodId());
			xxrUserHooksDetails.setProjectId(userHooksDetailsReqPo.getProjectId());
			xxrUserHooksDetails.setParentObjectId(userHooksDetailsReqPo.getParentObjectId());
			xxrUserHooksDetails.setProjectId(userHooksDetailsReqPo.getProjectId());
			xxrUserHooksDetails.setObjectId(userHooksDetailsReqPo.getObjectId());
			xxrUserHooksDetails.setExtFlag(userHooksDetailsReqPo.getExtFlag());
			xxrUserHooksDetails.setValFlag(userHooksDetailsReqPo.getValFlag());
			xxrUserHooksDetails.setCldImportFlag(userHooksDetailsReqPo.getCldImportFlag());
			xxrUserHooksDetails.setExtractionPreHook(userHooksDetailsReqPo.getExtractionPreHook());
			xxrUserHooksDetails.setExtractionPostHook(userHooksDetailsReqPo.getExtractionPostHook());
			xxrUserHooksDetails.setCldImportPreHook(userHooksDetailsReqPo.getCldImportPreHook());
			xxrUserHooksDetails.setCldImportPostHook(userHooksDetailsReqPo.getCldImportPostHook());
			xxrUserHooksDetails.setValidationPostHook(userHooksDetailsReqPo.getValidationPostHook());
			xxrUserHooksDetails.setValidationPreHook(userHooksDetailsReqPo.getValidationPreHook());
			xxrUserHooksDetails.setCreationDate(new java.sql.Date(new java.util.Date().getTime()));
			xxrUserHooksDetails.setCreatedBy("ConvertRite");
			xxrUserHooksDetails.setLastUpdatedDate(new java.sql.Date(new java.util.Date().getTime()));
			xxrUserHooksDetails.setLastUpdateBy("ConvertRite");
			xxrUserHooksDetailsRes = xxrUserHooksDetailsRepository.save(xxrUserHooksDetails);
		} else {
			Optional<XxrUserHooksDetails> userHooksDtls = xxrUserHooksDetailsRepository
					.findById(userHooksDetailsReqPo.getId());
			if (userHooksDtls.isPresent()) {
				XxrUserHooksDetails xxrUserHooksDetailsExist = userHooksDtls.get();
				xxrUserHooksDetailsExist.setCldTemplateId(userHooksDetailsReqPo.getCldTemplateId());
				xxrUserHooksDetailsExist.setCldTemplateName(userHooksDetailsReqPo.getCldTemplateName());
				xxrUserHooksDetailsExist.setSrcTemplateId(userHooksDetailsReqPo.getSrcTemplateId());
				xxrUserHooksDetailsExist.setSrcTemplateName(userHooksDetailsReqPo.getSrcTemplateName());
				xxrUserHooksDetailsExist.setPodId(userHooksDetailsReqPo.getPodId());
				xxrUserHooksDetailsExist.setProjectId(userHooksDetailsReqPo.getProjectId());
				xxrUserHooksDetailsExist.setParentObjectId(userHooksDetailsReqPo.getParentObjectId());
				xxrUserHooksDetailsExist.setProjectId(userHooksDetailsReqPo.getProjectId());
				xxrUserHooksDetailsExist.setObjectId(userHooksDetailsReqPo.getObjectId());
				xxrUserHooksDetailsExist.setExtFlag(userHooksDetailsReqPo.getExtFlag());
				xxrUserHooksDetailsExist.setValFlag(userHooksDetailsReqPo.getValFlag());
				xxrUserHooksDetailsExist.setCldImportFlag(userHooksDetailsReqPo.getCldImportFlag());
				xxrUserHooksDetailsExist.setExtractionPreHook(userHooksDetailsReqPo.getExtractionPreHook());
				xxrUserHooksDetailsExist.setExtractionPostHook(userHooksDetailsReqPo.getExtractionPostHook());
				xxrUserHooksDetailsExist.setCldImportPreHook(userHooksDetailsReqPo.getCldImportPreHook());
				xxrUserHooksDetailsExist.setCldImportPostHook(userHooksDetailsReqPo.getCldImportPostHook());
				xxrUserHooksDetailsExist.setValidationPostHook(userHooksDetailsReqPo.getValidationPostHook());
				xxrUserHooksDetailsExist.setValidationPreHook(userHooksDetailsReqPo.getValidationPreHook());
				xxrUserHooksDetailsExist.setLastUpdatedDate(new java.sql.Date(new java.util.Date().getTime()));
				xxrUserHooksDetailsExist.setLastUpdateBy("ConvertRite");
				xxrUserHooksDetailsRes = xxrUserHooksDetailsRepository.save(xxrUserHooksDetailsExist);

			} else {
				throw new ValidationException("UserHook Details are not present for the given Id");
			}
		}
		saveUserHooksResPo.setMessage("Successfully saved/updated UserHooksDetails");
		saveUserHooksResPo.setXxrUserHooksDetails(xxrUserHooksDetailsRes);
		return saveUserHooksResPo;
	}

	@Override
	public XxrUserHooksDetailsView getUserHooksDtls(Long cldTemplateId) {
		log.info("Start of getUserHooksDtls Method in service ######");
		XxrUserHooksDetailsView xxrUserHooksDetailsView = new XxrUserHooksDetailsView();
		xxrUserHooksDetailsView = xxrUserHooksDetailsViewRepository.findByCldTemplateId(cldTemplateId);
		return xxrUserHooksDetailsView;
	}

}
