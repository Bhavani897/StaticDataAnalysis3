package com.rite.products.convertrite.service;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.rite.products.convertrite.exception.ValidationException;
import com.rite.products.convertrite.model.XxrObjectCodeGrouping;
import com.rite.products.convertrite.model.XxrObjectCodeGroupingLines;
import com.rite.products.convertrite.po.SaveObjectCodeGroupingLinesPo;
import com.rite.products.convertrite.po.SaveObjectCodeGroupingLinesResPo;
import com.rite.products.convertrite.po.SaveObjectCodeGroupingPo;
import com.rite.products.convertrite.po.SaveObjectCodeGroupingResPo;
import com.rite.products.convertrite.po.XxrObjectCodeGroupingLinesResPo;
import com.rite.products.convertrite.respository.XxrLookUpValuesRepository;
import com.rite.products.convertrite.respository.XxrObjectCodeGroupingLinesRepository;
import com.rite.products.convertrite.respository.XxrObjectCodeGroupingRepository;

@Service
public class ObjectGroupingServiceImpl implements ObjectGroupingService {

	private static final Logger log = LoggerFactory.getLogger(ObjectGroupingServiceImpl.class);

	@Autowired
	XxrObjectCodeGroupingRepository xxrObjectCodeGroupingRepository;
	@Autowired
	XxrObjectCodeGroupingLinesRepository xxrObjectCodeGroupingLinesRepository;
	@Autowired
	XxrLookUpValuesRepository xxrLookUpValuesRepository;

	@Override
	public SaveObjectCodeGroupingResPo saveObjectCodeGrouping(SaveObjectCodeGroupingPo saveObjectCodeGroupingPo)
			throws Exception {
		log.info("Start of saveObjectCodeGrouping Method #####");
		XxrObjectCodeGrouping xxrObjectCodeGrouping = new XxrObjectCodeGrouping();
		SaveObjectCodeGroupingResPo saveObjectCodeGroupingResPo = new SaveObjectCodeGroupingResPo();
		try {

			xxrObjectCodeGrouping.setGroupName(saveObjectCodeGroupingPo.getGroupName());
			xxrObjectCodeGrouping.setCtlFile(saveObjectCodeGroupingPo.getCtlFile());
			xxrObjectCodeGrouping.setVersion(saveObjectCodeGroupingPo.getVersion());
			xxrObjectCodeGrouping.setCreationDate(new java.sql.Date(new java.util.Date().getTime()));
			xxrObjectCodeGrouping.setCreatedBy("ConvertRite");
			xxrObjectCodeGrouping.setLastUpdatedDate(new java.sql.Date(new java.util.Date().getTime()));
			xxrObjectCodeGrouping.setLastUpdateBy("ConvertRite");
			
			XxrObjectCodeGrouping xxrObjectCodeGroupingResp = xxrObjectCodeGroupingRepository
					.save(xxrObjectCodeGrouping);
			
			saveObjectCodeGroupingResPo.setGroupId(xxrObjectCodeGroupingResp.getGroupId());
			saveObjectCodeGroupingResPo.setMessage("Successfully saved objectcodegrouping");

		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return saveObjectCodeGroupingResPo;
	}

	@Override
	public SaveObjectCodeGroupingLinesResPo saveObjectCodeGroupingLines(
			List<SaveObjectCodeGroupingLinesPo> saveObjectCodeGroupingLinesPo) throws Exception {
		log.info("Start of saveObjectCodeGrouping Method######");
		SaveObjectCodeGroupingLinesResPo saveObjectCodeGroupingLinesResPo = new SaveObjectCodeGroupingLinesResPo();
		Long groupId = null;
		List<XxrObjectCodeGroupingLines> xxrObjectCodeGroupingLinesLis = new ArrayList<>();
		try {
			for (SaveObjectCodeGroupingLinesPo saveObjectCodeGroupingLine : saveObjectCodeGroupingLinesPo) {

				groupId = xxrObjectCodeGroupingLinesRepository
						.getGroupIdbyObjectId(saveObjectCodeGroupingLine.getObjectId());

				if (groupId != null)
					throw new ValidationException("This ObjectCode already exists,Please select unique Objectcode");

				XxrObjectCodeGroupingLines xxrObjectCodeGroupingLines = new XxrObjectCodeGroupingLines();
				xxrObjectCodeGroupingLines.setGroupId(saveObjectCodeGroupingLine.getGroupId());
				xxrObjectCodeGroupingLines.setObjectId(saveObjectCodeGroupingLine.getObjectId());
				xxrObjectCodeGroupingLines.setCreationDate(new java.sql.Date(new java.util.Date().getTime()));
				xxrObjectCodeGroupingLines.setCreatedBy("ConvertRite");
				xxrObjectCodeGroupingLines.setLastUpdatedDate(new java.sql.Date(new java.util.Date().getTime()));
				xxrObjectCodeGroupingLines.setLastUpdateBy("ConvertRite");
				XxrObjectCodeGroupingLines xxrObjectCodeGroupingLine = xxrObjectCodeGroupingLinesRepository
						.save(xxrObjectCodeGroupingLines);
				xxrObjectCodeGroupingLinesLis.add(xxrObjectCodeGroupingLine);
			}
			saveObjectCodeGroupingLinesResPo.setXxrObjectGroupingLines(xxrObjectCodeGroupingLinesLis);
			saveObjectCodeGroupingLinesResPo.setMessage("Succesfully saved ObjectCode grouping Lines");
		} catch (ValidationException e) {
			throw new ValidationException(e.getMessage());
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return saveObjectCodeGroupingLinesResPo;
	}

	@Override
	public List<XxrObjectCodeGrouping> getObjectCodeGroupings() throws Exception {
		log.info("Start of getObjectCodeGroupings Method######");
		List<XxrObjectCodeGrouping> xxrObjectCodeGrouping = new ArrayList<>();
		try {
			xxrObjectCodeGrouping = xxrObjectCodeGroupingRepository.findAll();
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new Exception(e.getMessage());
		}
		return xxrObjectCodeGrouping;
	}

	@Override
	public List<XxrObjectCodeGroupingLinesResPo> getObjectCodeGroupingLines(Long groupId) throws Exception {
		log.info("Start of getObjectCodeGroupingLines Method######");
		List<XxrObjectCodeGroupingLinesResPo> respLi = new ArrayList<>();
		try {
			List<XxrObjectCodeGroupingLines> xxrObjectCodeGroupingLinesLi = xxrObjectCodeGroupingLinesRepository
					.findByGroupId(groupId);

			xxrObjectCodeGroupingLinesLi.stream().forEach(x -> {

				XxrObjectCodeGroupingLinesResPo xxrObjectCodeGroupingLinesResPo = new XxrObjectCodeGroupingLinesResPo();
				xxrObjectCodeGroupingLinesResPo.setGroupId(x.getGroupId());
				xxrObjectCodeGroupingLinesResPo.setGroupLineId(x.getGroupLineId());
				xxrObjectCodeGroupingLinesResPo.setObjectId(x.getObjectId());

				String objectCode = xxrLookUpValuesRepository.getValueById(x.getObjectId());
				xxrObjectCodeGroupingLinesResPo.setObjectCode(objectCode);
				String groupName = xxrObjectCodeGroupingRepository.getGroupName(x.getGroupId());
				xxrObjectCodeGroupingLinesResPo.setGroupName(groupName);

				respLi.add(xxrObjectCodeGroupingLinesResPo);

			});
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new Exception(e.getMessage());
		}
		return respLi;
	}
}
