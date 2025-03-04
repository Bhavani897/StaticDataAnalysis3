package com.rite.products.convertrite.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.rite.products.convertrite.model.XxrDateConfiguration;
import com.rite.products.convertrite.po.DateConfigurationResPo;
import com.rite.products.convertrite.po.SaveDateConfigurationReqPo;
import com.rite.products.convertrite.po.SaveDateConfigurationResPo;
import com.rite.products.convertrite.respository.XxrDateConfiguartionRepository;
import com.rite.products.convertrite.respository.XxrLookUpValuesRepository;

@Service
public class DateConfigurationServiceImpl implements DateConfigurationService {

	private static final Logger log = LoggerFactory.getLogger(DateConfigurationService.class);

	@Autowired
	XxrDateConfiguartionRepository xxrDateConfiguartionRepository;
	@Autowired
	XxrLookUpValuesRepository xxrLookUpValuesRepository;

	@Override
	public List<DateConfigurationResPo> getDateConfiguration() throws Exception {
		log.info("Start of getDateConfiguration Method#########");
		List<XxrDateConfiguration> xxrDateConfiguartionLi = new ArrayList<>();
		List<DateConfigurationResPo> dateConfiguartionResLi = new ArrayList<>();
		try {
			xxrDateConfiguartionLi = xxrDateConfiguartionRepository.findAll();

			xxrDateConfiguartionLi.stream().forEach(x -> {
				DateConfigurationResPo dateConfigurationResPo = new DateConfigurationResPo();
				String podName = xxrLookUpValuesRepository.getValueById(x.getPodId());
				String projectName = xxrLookUpValuesRepository.getValueById(x.getProjectId());
				String objectCode = xxrLookUpValuesRepository.getValueById(x.getObjectId());
				String parentObjectCode = xxrLookUpValuesRepository.getValueById(x.getParentObjectId());
				dateConfigurationResPo.setId(x.getId());
				dateConfigurationResPo.setPodId(x.getPodId());
				dateConfigurationResPo.setProjectId(x.getProjectId());
				dateConfigurationResPo.setSourceDateFormat(x.getSourceDateFormat());
				dateConfigurationResPo.setCloudDateFormat(x.getCloudDateFormat());
				dateConfigurationResPo.setPodName(podName);
				dateConfigurationResPo.setProjectName(projectName);
				dateConfigurationResPo.setObjectId(x.getObjectId());
				dateConfigurationResPo.setObjectCode(objectCode);
				dateConfigurationResPo.setParentObjectId(x.getParentObjectId());
				dateConfigurationResPo.setParentObjectCode(parentObjectCode);

				dateConfiguartionResLi.add(dateConfigurationResPo);
			});
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return dateConfiguartionResLi;
	}

	@Override
	public SaveDateConfigurationResPo saveDateConfiguration(SaveDateConfigurationReqPo saveDateConfigurationReqPo)
			throws Exception {
		log.info("start of saveDateConfiguration method#####");
		SaveDateConfigurationResPo saveDateConfigurationResPo = new SaveDateConfigurationResPo();
		XxrDateConfiguration dateConfigurationRes = new XxrDateConfiguration();
		try {

			if (saveDateConfigurationReqPo.getId() != null) {
				Optional<XxrDateConfiguration> dateConfigOpt = xxrDateConfiguartionRepository
						.findById(saveDateConfigurationReqPo.getId());
				if (dateConfigOpt.isPresent()) {
					XxrDateConfiguration xxrDateConfiguration = dateConfigOpt.get();
					xxrDateConfiguration.setPodId(saveDateConfigurationReqPo.getPodId());
					xxrDateConfiguration.setProjectId(saveDateConfigurationReqPo.getProjectId());
					xxrDateConfiguration.setObjectId(saveDateConfigurationReqPo.getObjectId());
					xxrDateConfiguration.setParentObjectId(saveDateConfigurationReqPo.getParentObjectId());
					xxrDateConfiguration.setSourceDateFormat(saveDateConfigurationReqPo.getSourceDateFormat());
					xxrDateConfiguration.setCloudDateFormat(saveDateConfigurationReqPo.getCloudDateFormat());
					xxrDateConfiguration.setLastUpdatedDate(new java.sql.Date(new java.util.Date().getTime()));
					xxrDateConfiguration.setLastUpdateBy("ConvertRite");
					dateConfigurationRes = xxrDateConfiguartionRepository.save(xxrDateConfiguration);
				}
			} else {
				XxrDateConfiguration xxrDateConfiguration = new XxrDateConfiguration();
				xxrDateConfiguration.setPodId(saveDateConfigurationReqPo.getPodId());
				xxrDateConfiguration.setProjectId(saveDateConfigurationReqPo.getProjectId());
				xxrDateConfiguration.setObjectId(saveDateConfigurationReqPo.getObjectId());
				xxrDateConfiguration.setParentObjectId(saveDateConfigurationReqPo.getParentObjectId());
				xxrDateConfiguration.setSourceDateFormat(saveDateConfigurationReqPo.getSourceDateFormat());
				xxrDateConfiguration.setCloudDateFormat(saveDateConfigurationReqPo.getCloudDateFormat());
				xxrDateConfiguration.setCreationDate(new java.sql.Date(new java.util.Date().getTime()));
				xxrDateConfiguration.setCreatedBy("ConvertRite");
				xxrDateConfiguration.setLastUpdatedDate(new java.sql.Date(new java.util.Date().getTime()));
				xxrDateConfiguration.setLastUpdateBy("ConvertRite");
				dateConfigurationRes = xxrDateConfiguartionRepository.save(xxrDateConfiguration);
			}
			saveDateConfigurationResPo.setId(dateConfigurationRes.getId());
			saveDateConfigurationResPo.setMessage("Date Configuration is saved successfully");
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return saveDateConfigurationResPo;
	}

}
