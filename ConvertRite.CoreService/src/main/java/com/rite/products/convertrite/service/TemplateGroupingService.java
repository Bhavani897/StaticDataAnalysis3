package com.rite.products.convertrite.service;

import java.util.List;

import org.springframework.stereotype.Service;

import com.rite.products.convertrite.po.SaveTemplateGroupingReqPo;
import com.rite.products.convertrite.po.SaveTemplateGroupingResPo;
import com.rite.products.convertrite.po.XxrTemplateRelationResPo;

@Service
public interface TemplateGroupingService {

	SaveTemplateGroupingResPo saveTemplateGrouping(SaveTemplateGroupingReqPo saveTemplateGroupingReqPo) throws Exception;

	List<XxrTemplateRelationResPo> getTemplateGroupings()throws Exception;

}
