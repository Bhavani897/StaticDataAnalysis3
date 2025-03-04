package com.rite.products.convertrite.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.rite.products.convertrite.model.XxrCloudDataProcessConfig;
import com.rite.products.convertrite.po.SaveCloudConfigDetailsResPo;
import com.rite.products.convertrite.po.SaveCloudConfigReqPo;
import com.rite.products.convertrite.po.XxrCloudConfigResPo;
import com.rite.products.convertrite.respository.XxrCloudDataProcessConfigRepository;
import com.rite.products.convertrite.respository.XxrLookUpValuesRepository;

@Service
public class CloudConfigServiceImpl implements CloudConfigService {

	private static final Logger log = LoggerFactory.getLogger(CloudConfigServiceImpl.class);

	@Autowired
	XxrCloudDataProcessConfigRepository xxrCloudDataProcessConfigRepository;
	@Autowired
	XxrLookUpValuesRepository xxrLookUpValuesRepository;

	@Override
	public List<XxrCloudConfigResPo> getCloudConfig() throws Exception {
		log.info("Start of getCloudConfig in service   #####");
		List<XxrCloudDataProcessConfig> cloudDataConfig = new ArrayList<>();
		List<XxrCloudConfigResPo> cloudDataConfigResLi = new ArrayList<>();
		// XxrCloudDataProcessConfig xxrCloudDataProcessConfig = new
		// XxrCloudDataProcessConfig();
		try {
			cloudDataConfig = xxrCloudDataProcessConfigRepository.findAll();

			cloudDataConfig.stream().forEach(x -> {
				XxrCloudConfigResPo xxrCloudConfigResPo = new XxrCloudConfigResPo();
				xxrCloudConfigResPo.setCloudUrl(x.getCloudUrl());
				xxrCloudConfigResPo.setUserName(x.getUserName());
				xxrCloudConfigResPo.setPassword(x.getPassword());
				xxrCloudConfigResPo.setProjectId(x.getProjectId());
				xxrCloudConfigResPo.setProjectName(xxrLookUpValuesRepository.getValueById(x.getProjectId()));
				xxrCloudConfigResPo.setPodId(x.getPodId());
				xxrCloudConfigResPo.setPodName(xxrLookUpValuesRepository.getValueById(x.getPodId()));
				xxrCloudConfigResPo.setCreatedBy(x.getCreatedBy());
				xxrCloudConfigResPo.setCreationDate(x.getCreationDate());
				xxrCloudConfigResPo.setLastUpdateBy(x.getLastUpdateBy());
				xxrCloudConfigResPo.setLastUpdatedDate(x.getLastUpdatedDate());
				xxrCloudConfigResPo.setId(x.getId());
				cloudDataConfigResLi.add(xxrCloudConfigResPo);
			});

			/*
			 * if (cloudDataConfig != null && !cloudDataConfig.isEmpty())
			 * xxrCloudDataProcessConfig = cloudDataConfig.get(0);
			 */
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new Exception(e.getMessage());
		}
		return cloudDataConfigResLi;
	}

	@Override
	public SaveCloudConfigDetailsResPo saveCloudConfigDetails(SaveCloudConfigReqPo saveCloudConfigReqPo)
			 {
		log.info("Start of saveCloudConfigDetails Method in service##");
		SaveCloudConfigDetailsResPo saveCloudConfigDetailsResPo = new SaveCloudConfigDetailsResPo();
		XxrCloudDataProcessConfig xxrCloudDataProcessConfigRes = new XxrCloudDataProcessConfig();
		try {

			Optional<XxrCloudDataProcessConfig> cloudprocessConfig = xxrCloudDataProcessConfigRepository
					.findByPodIdAndProjectId(saveCloudConfigReqPo.getPodId(), saveCloudConfigReqPo.getProjectId());
			if (cloudprocessConfig.isPresent()) {
				XxrCloudDataProcessConfig xxrCloudDataProcessConfig = cloudprocessConfig.get();
				xxrCloudDataProcessConfig.setCloudUrl(saveCloudConfigReqPo.getCloudUrl());
				xxrCloudDataProcessConfig.setPassword(saveCloudConfigReqPo.getPassword());
				xxrCloudDataProcessConfig.setUserName(saveCloudConfigReqPo.getUserName());
				xxrCloudDataProcessConfig.setPodId(saveCloudConfigReqPo.getPodId());
				xxrCloudDataProcessConfig.setProjectId(saveCloudConfigReqPo.getProjectId());
				xxrCloudDataProcessConfig.setLastUpdatedDate(new java.sql.Date(new java.util.Date().getTime()));
				xxrCloudDataProcessConfig.setLastUpdateBy("ConvertRite");
				xxrCloudDataProcessConfigRes = xxrCloudDataProcessConfigRepository.save(xxrCloudDataProcessConfig);
			} else {
				XxrCloudDataProcessConfig xxrCloudDataProcessConfig = new XxrCloudDataProcessConfig();
				UUID requestId = UUID.randomUUID();
				xxrCloudDataProcessConfig.setId(requestId.toString());
				xxrCloudDataProcessConfig.setCloudUrl(saveCloudConfigReqPo.getCloudUrl());
				xxrCloudDataProcessConfig.setPassword(saveCloudConfigReqPo.getPassword());
				xxrCloudDataProcessConfig.setUserName(saveCloudConfigReqPo.getUserName());
				xxrCloudDataProcessConfig.setPodId(saveCloudConfigReqPo.getPodId());
				xxrCloudDataProcessConfig.setProjectId(saveCloudConfigReqPo.getProjectId());
				xxrCloudDataProcessConfig.setCreationDate(new java.sql.Date(new java.util.Date().getTime()));
				xxrCloudDataProcessConfig.setCreatedBy("ConvertRite");
				xxrCloudDataProcessConfig.setLastUpdatedDate(new java.sql.Date(new java.util.Date().getTime()));
				xxrCloudDataProcessConfig.setLastUpdateBy("ConvertRite");
				xxrCloudDataProcessConfigRes = xxrCloudDataProcessConfigRepository.save(xxrCloudDataProcessConfig);
			}
			saveCloudConfigDetailsResPo.setXxrCloudDataProcessConfig(xxrCloudDataProcessConfigRes);
			saveCloudConfigDetailsResPo.setMessage("Successfully saved cloud config details");
		} catch (Exception e) {
			log.error(e.getMessage());
		}
		return saveCloudConfigDetailsResPo;
	}

}
