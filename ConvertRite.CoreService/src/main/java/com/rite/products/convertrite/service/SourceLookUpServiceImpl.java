package com.rite.products.convertrite.service;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Service;

import com.rite.products.convertrite.model.XxrSrcLookupSets;
import com.rite.products.convertrite.model.XxrSrcLookupValues;
import com.rite.products.convertrite.po.LoadCustomLookupReqPo;
import com.rite.products.convertrite.po.LoadLookupResPo;
import com.rite.products.convertrite.po.LookUpSearchReqPo;
import com.rite.products.convertrite.po.LookUpsetNameResPo;
import com.rite.products.convertrite.respository.SourceLookupDaoImpl;
import com.rite.products.convertrite.respository.XxrSrcLookupSetRepository;
import com.rite.products.convertrite.respository.XxrSrcLookupValuesRepository;

@Service
public class SourceLookUpServiceImpl implements SourceLookUpService {
	private static final Logger log = LoggerFactory.getLogger(SourceLookUpServiceImpl.class);

	@Autowired
	SourceLookupDaoImpl sourceLookupDaoImpl;
	@Autowired
	XxrSrcLookupValuesRepository xxrSrcLookupValuesRepository;
	@Autowired
	XxrSrcLookupSetRepository xxrSrcLookupSetRepository;

	@Override
	public List<XxrSrcLookupSets> searchSrcLookups(LookUpSearchReqPo lookUpSearchReqPo, HttpHeaders header)
			throws Exception {
		log.info("Start of searchSrcLookups in service########");
		List<XxrSrcLookupSets> xxrSrcLookupsLi = new ArrayList<>();
		try {
			xxrSrcLookupsLi = sourceLookupDaoImpl.searchSrcLookups(lookUpSearchReqPo, header);
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return xxrSrcLookupsLi;
	}

	@Override
	public LoadLookupResPo loadLookupsFromSrc(String dbLink) throws Exception {
		log.info("Start of loadLookupsFromSrc in service########");
		LoadLookupResPo loadLookupResPo = new LoadLookupResPo();
		try {
			loadLookupResPo = sourceLookupDaoImpl.loadLookupsFromSrc(dbLink);
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return loadLookupResPo;
	}

	@Override
	public List<XxrSrcLookupValues> getSrcLookupValues(Long lookupSetId) throws Exception {
		
		log.info("Start of getSrcLookupValues#########");
		List<XxrSrcLookupValues> srcLookupValuesLi = new ArrayList<>();
		try {
			srcLookupValuesLi = xxrSrcLookupValuesRepository.findByLookupSetId(lookupSetId);
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return srcLookupValuesLi;
	}

	@Override
	public List<LookUpsetNameResPo> getSrcLookupSetName(String lookupSetName) throws Exception {
		
		log.info("Start of getSrcLookupValues#########");
		List<LookUpsetNameResPo> lookupSetResLi = new ArrayList<>();
		try {
			lookupSetResLi = xxrSrcLookupSetRepository.getLookupSetName(lookupSetName.toLowerCase());
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return lookupSetResLi;
	}

	@Override
	public LoadLookupResPo loadCustomLookupsFromSrc(LoadCustomLookupReqPo loadCustomLookupReqPo) {
		log.info("Start of loadCustomLookupsFromSrc#####");
		LoadLookupResPo loadLookupResPo = new LoadLookupResPo();
		loadLookupResPo = sourceLookupDaoImpl.loadCustomLookupsFromSrc(loadCustomLookupReqPo);
		return loadLookupResPo;
	}

}
