package com.rite.products.convertrite.service;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.rite.products.convertrite.model.XxrCloudConfig;
import com.rite.products.convertrite.po.SaveCloudObjectRelatedConfigDetailsResPo;
import com.rite.products.convertrite.po.SaveCloudObjectRelatedConfigReqPo;
import com.rite.products.convertrite.respository.XxrCloudConfigRepository;

@Service
public class CloudObjectRelatedConfigServiceImpl implements CloudObjectRelatedConfigService {
	private static final Logger log = LoggerFactory.getLogger(CloudObjectRelatedConfigServiceImpl.class);

	@Autowired
	XxrCloudConfigRepository xxrCloudConfigRepository;

	@Override
	public List<XxrCloudConfig> getCloudObjectRelatedConfig() throws Exception {
		log.info("Start of getCloudObjectRelatedConfig Method in service ######");
		List<XxrCloudConfig> xxrCloudConfigLi = new ArrayList<>();
		try {
			xxrCloudConfigLi = xxrCloudConfigRepository.findAll();
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return xxrCloudConfigLi;
	}

	@Override
	public SaveCloudObjectRelatedConfigDetailsResPo saveCloudObjectRelatedConfig(
			SaveCloudObjectRelatedConfigReqPo saveCloudObjectRelatedConfigReqPo) throws Exception {
		log.info("Start of saveCloudObjectRelatedConfig Method in service ######");
		SaveCloudObjectRelatedConfigDetailsResPo saveCloudObjectRelatedConfigDetailsResPo = new SaveCloudObjectRelatedConfigDetailsResPo();
		XxrCloudConfig xxrCloudConfigRes = new XxrCloudConfig();
		try {
			XxrCloudConfig xxrCloudConfig = xxrCloudConfigRepository
					.findByObjectCode(saveCloudObjectRelatedConfigReqPo.getObjectCode());
			if (xxrCloudConfig == null) {
				XxrCloudConfig cloudConfig = new XxrCloudConfig();
				cloudConfig.setCtlFileName(saveCloudObjectRelatedConfigReqPo.getCtlFileName());
				cloudConfig.setInterfaceTableName(saveCloudObjectRelatedConfigReqPo.getInterfaceTableName());
				cloudConfig.setObjectCode(saveCloudObjectRelatedConfigReqPo.getObjectCode());
				cloudConfig.setXlsmFileName(saveCloudObjectRelatedConfigReqPo.getXmlsmFileName());
				cloudConfig.setSheetName(saveCloudObjectRelatedConfigReqPo.getSheetName());
				cloudConfig.setCreationDate(new java.sql.Date(new java.util.Date().getTime()));
				cloudConfig.setRejectionTableName(saveCloudObjectRelatedConfigReqPo.getRejectionTableName());
				cloudConfig.setEssJobStatusCol(saveCloudObjectRelatedConfigReqPo.getEssJobSatusColumn());
				cloudConfig.setCreatedBy("ConvertRite");
				cloudConfig.setLastUpdatedDate(new java.sql.Date(new java.util.Date().getTime()));
				cloudConfig.setLastUpdateBy("ConvertRite");
				xxrCloudConfigRes = xxrCloudConfigRepository.save(cloudConfig);

			} else {
				xxrCloudConfig.setCtlFileName(saveCloudObjectRelatedConfigReqPo.getCtlFileName());
				xxrCloudConfig.setInterfaceTableName(saveCloudObjectRelatedConfigReqPo.getInterfaceTableName());
				xxrCloudConfig.setXlsmFileName(saveCloudObjectRelatedConfigReqPo.getXmlsmFileName());
				xxrCloudConfig.setSheetName(saveCloudObjectRelatedConfigReqPo.getSheetName());
				xxrCloudConfig.setRejectionTableName(saveCloudObjectRelatedConfigReqPo.getRejectionTableName());
				xxrCloudConfig.setEssJobStatusCol(saveCloudObjectRelatedConfigReqPo.getEssJobSatusColumn());
				xxrCloudConfig.setLastUpdatedDate(new java.sql.Date(new java.util.Date().getTime()));
				xxrCloudConfig.setLastUpdateBy("ConvertRite");
				xxrCloudConfigRes = xxrCloudConfigRepository.save(xxrCloudConfig);
			}
			saveCloudObjectRelatedConfigDetailsResPo.setXxrCloudConfig(xxrCloudConfigRes);
			saveCloudObjectRelatedConfigDetailsResPo
					.setMessage("Successfully saved cloud object related config details");

		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return saveCloudObjectRelatedConfigDetailsResPo;
	}

}
