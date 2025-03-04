package com.rite.products.convertrite.service;

import java.util.List;

import org.springframework.stereotype.Service;

import com.rite.products.convertrite.po.SaveCloudConfigDetailsResPo;
import com.rite.products.convertrite.po.SaveCloudConfigReqPo;
import com.rite.products.convertrite.po.XxrCloudConfigResPo;

@Service
public interface CloudConfigService {

	List<XxrCloudConfigResPo> getCloudConfig() throws Exception;

	SaveCloudConfigDetailsResPo saveCloudConfigDetails(SaveCloudConfigReqPo saveCloudConfigReqPo) throws Exception;

}
