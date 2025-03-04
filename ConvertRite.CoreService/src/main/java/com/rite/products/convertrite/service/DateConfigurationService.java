package com.rite.products.convertrite.service;

import java.util.List;

import org.springframework.stereotype.Service;

import com.rite.products.convertrite.po.DateConfigurationResPo;
import com.rite.products.convertrite.po.SaveDateConfigurationReqPo;
import com.rite.products.convertrite.po.SaveDateConfigurationResPo;

@Service
public interface DateConfigurationService {

	List<DateConfigurationResPo> getDateConfiguration() throws Exception;

	SaveDateConfigurationResPo saveDateConfiguration(SaveDateConfigurationReqPo saveDateConfigurationReqPo) throws Exception;

}
