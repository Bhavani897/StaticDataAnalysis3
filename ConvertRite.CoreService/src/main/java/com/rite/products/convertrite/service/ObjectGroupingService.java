package com.rite.products.convertrite.service;

import java.util.List;

import org.springframework.stereotype.Service;

import com.rite.products.convertrite.model.XxrObjectCodeGrouping;
import com.rite.products.convertrite.po.SaveObjectCodeGroupingLinesPo;
import com.rite.products.convertrite.po.SaveObjectCodeGroupingLinesResPo;
import com.rite.products.convertrite.po.SaveObjectCodeGroupingPo;
import com.rite.products.convertrite.po.SaveObjectCodeGroupingResPo;
import com.rite.products.convertrite.po.XxrObjectCodeGroupingLinesResPo;

@Service
public interface ObjectGroupingService {

	SaveObjectCodeGroupingResPo saveObjectCodeGrouping(SaveObjectCodeGroupingPo saveObjectCodeGroupingPo) throws Exception;

	SaveObjectCodeGroupingLinesResPo saveObjectCodeGroupingLines(
			List<SaveObjectCodeGroupingLinesPo> saveObjectCodeGroupingLinesPo)throws Exception;

	List<XxrObjectCodeGrouping> getObjectCodeGroupings()throws Exception;

	List<XxrObjectCodeGroupingLinesResPo> getObjectCodeGroupingLines(Long groupId)throws Exception;
	

}
