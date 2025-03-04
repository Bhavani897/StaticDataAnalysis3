package com.rite.products.convertrite.service;

import java.util.List;

import com.rite.products.convertrite.model.XxrCloudConfig;
import com.rite.products.convertrite.po.SaveCloudObjectRelatedConfigDetailsResPo;
import com.rite.products.convertrite.po.SaveCloudObjectRelatedConfigReqPo;

public interface CloudObjectRelatedConfigService {

	List<XxrCloudConfig> getCloudObjectRelatedConfig() throws Exception;

	SaveCloudObjectRelatedConfigDetailsResPo saveCloudObjectRelatedConfig(
			SaveCloudObjectRelatedConfigReqPo saveCloudObjectRelatedConfigReqPo) throws Exception;

}
