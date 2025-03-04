package com.rite.products.convertrite.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.rite.products.convertrite.model.XxrCloudTemplateHeader;
import com.rite.products.convertrite.model.XxrTemplateRelation;
import com.rite.products.convertrite.po.SaveTemplateGroupingReqPo;
import com.rite.products.convertrite.po.SaveTemplateGroupingResPo;
import com.rite.products.convertrite.po.XxrTemplateRelationResPo;
import com.rite.products.convertrite.respository.XxrCloudTemplateHeadersRepository;
import com.rite.products.convertrite.respository.XxrObjectCodeGroupingRepository;
import com.rite.products.convertrite.respository.XxrTemplateRelationRepository;

@Service
public class TemplateGroupingServiceImpl implements TemplateGroupingService {

	private static final Logger log = LoggerFactory.getLogger(TemplateGroupingServiceImpl.class);

	@Autowired
	XxrTemplateRelationRepository xxrTemplateRelationRepository;
	@Autowired
	XxrObjectCodeGroupingRepository xxrObjectCodeGroupingRepository;
	@Autowired
	XxrCloudTemplateHeadersRepository xxrCloudTemplateHeadersRepository;

	@Override
	public SaveTemplateGroupingResPo saveTemplateGrouping(SaveTemplateGroupingReqPo saveTemplateGroupingReqPo)
			throws Exception {
		log.info("Start Of saveTemplateGrouping Method ######");
		XxrTemplateRelation xxrTemplateRelation = new XxrTemplateRelation();
		XxrTemplateRelation exitingTemplateRelation =null;
		XxrTemplateRelation xxrTemplateRelationRes = new XxrTemplateRelation();
		SaveTemplateGroupingResPo saveTemplateGroupingResPo = new SaveTemplateGroupingResPo();
		Optional<XxrTemplateRelation> tempRelationOpt = null;
		try {
			if (saveTemplateGroupingReqPo.getId() != null) {
				tempRelationOpt = xxrTemplateRelationRepository.findById(saveTemplateGroupingReqPo.getId());
				if (tempRelationOpt.isPresent())
					exitingTemplateRelation = tempRelationOpt.get();
			}
			if (exitingTemplateRelation != null) {
				exitingTemplateRelation.setGroupId(saveTemplateGroupingReqPo.getGroupId());
				exitingTemplateRelation.setIsZipped("N");
				exitingTemplateRelation.setVersion(saveTemplateGroupingReqPo.getVersion());
				exitingTemplateRelation.setTemplateIds(saveTemplateGroupingReqPo.getTemplateIds());
				exitingTemplateRelation.setLastUpdatedDate(new java.sql.Date(new java.util.Date().getTime()));
				exitingTemplateRelation.setLastUpdateBy("ConvertRite");
				xxrTemplateRelationRes = xxrTemplateRelationRepository.save(exitingTemplateRelation);
			} else {
				xxrTemplateRelation.setGroupId(saveTemplateGroupingReqPo.getGroupId());
				xxrTemplateRelation.setIsZipped("N");
				xxrTemplateRelation.setVersion(saveTemplateGroupingReqPo.getVersion());
				xxrTemplateRelation.setTemplateIds(saveTemplateGroupingReqPo.getTemplateIds());
				xxrTemplateRelation.setCreationDate(new java.sql.Date(new java.util.Date().getTime()));
				xxrTemplateRelation.setCreatedBy("ConvertRite");
				xxrTemplateRelation.setLastUpdatedDate(new java.sql.Date(new java.util.Date().getTime()));
				xxrTemplateRelation.setLastUpdateBy("ConvertRite");
				xxrTemplateRelationRes = xxrTemplateRelationRepository.save(xxrTemplateRelation);
			}
			saveTemplateGroupingResPo.setId(xxrTemplateRelationRes.getId());
			saveTemplateGroupingResPo.setMessage("Successfully saved Template Grouping");

		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return saveTemplateGroupingResPo;
	}

	@Override
	public List<XxrTemplateRelationResPo> getTemplateGroupings() throws Exception {
		log.info("Start of getTemplateGroupings Method######");
		List<XxrTemplateRelationResPo> xxrTemplateRelationResPoLi = new ArrayList<>();
		try {
			List<XxrTemplateRelation> xxrTemplateRelationLi = xxrTemplateRelationRepository.findAll();
			xxrTemplateRelationLi.stream().forEach(x -> {
				XxrTemplateRelationResPo xxrTemplateRelationResPo = new XxrTemplateRelationResPo();
				xxrTemplateRelationResPo.setGroupId(x.getGroupId());
				String groupName = xxrObjectCodeGroupingRepository.getGroupName(x.getGroupId());
				xxrTemplateRelationResPo.setGroupName(groupName);
				xxrTemplateRelationResPo.setId(x.getId());
				xxrTemplateRelationResPo.setIsZipped(x.getIsZipped());
				xxrTemplateRelationResPo.setTemplateIds(x.getTemplateIds());
				String[] templateIdsArr=x.getTemplateIds().split(",");
				StringBuffer templateSb=new StringBuffer();
				for(int i=0;i<templateIdsArr.length;i++) {
					
					long templateId = Long.parseLong(templateIdsArr[i]);
					XxrCloudTemplateHeader cloudTemplateHeader = xxrCloudTemplateHeadersRepository.getCloudTemplateById(templateId);
					//XxrCloudTemplateHeader cloudTemplateHeader = cloudTemplateHeaderLi.get(0);
					String templateName=  cloudTemplateHeader.getTemplateName();
					templateSb.append(templateName);
					if(i!=templateIdsArr.length-1)
					templateSb.append(",");
				}
				xxrTemplateRelationResPo.setTemplateNames(templateSb.toString());			
				xxrTemplateRelationResPo.setVersion(x.getVersion());
				
				xxrTemplateRelationResPoLi.add(xxrTemplateRelationResPo);
			});
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return xxrTemplateRelationResPoLi;
	}

}
