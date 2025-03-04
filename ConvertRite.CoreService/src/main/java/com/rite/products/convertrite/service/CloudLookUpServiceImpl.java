package com.rite.products.convertrite.service;

import java.util.ArrayList;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Service;

import com.rite.products.convertrite.Validations.Validations;
import com.rite.products.convertrite.exception.BadRequestException;
import com.rite.products.convertrite.exception.ValidationException;
import com.rite.products.convertrite.model.XxrCloudMasterLookUpSet;
import com.rite.products.convertrite.model.XxrCloudMasterLookupValues;
import com.rite.products.convertrite.model.XxrLookUpValues;
import com.rite.products.convertrite.model.XxrLookupSets;
import com.rite.products.convertrite.po.LookUpSearchReqPo;
import com.rite.products.convertrite.po.LookUpsetNameResPo;
import com.rite.products.convertrite.po.SaveCloudLookUpSetColumnsPo;
import com.rite.products.convertrite.po.SaveCloudLookUpSetColumnsResPo;
import com.rite.products.convertrite.po.SaveCloudLookUpSetHeadersPo;
import com.rite.products.convertrite.po.SaveCloudLookUpSetHeadersResPo;
import com.rite.products.convertrite.po.SaveLookUpSetPo;
import com.rite.products.convertrite.po.SaveLookUpValuesPo;
import com.rite.products.convertrite.po.SaveLookUpValuesResPo;
import com.rite.products.convertrite.respository.CloudLookupSearchDaoImpl;
import com.rite.products.convertrite.respository.CloudMetaDataRepository;
import com.rite.products.convertrite.respository.SaveCloudLookUpSetColumnsDaoImpl;
import com.rite.products.convertrite.respository.SaveCloudLookUpSetHeadersDaoImpl;
import com.rite.products.convertrite.respository.SaveLookUpSetDaoImpl;
import com.rite.products.convertrite.respository.SaveLookUpValuesDaoImpl;
import com.rite.products.convertrite.respository.XxrCloudMasterLookUpSetRepository;
import com.rite.products.convertrite.respository.XxrCloudMasterLookupValuesRepository;
import com.rite.products.convertrite.respository.XxrLookUpValuesRepository;
import com.rite.products.convertrite.respository.XxrLookupSetsRepository;
import com.rite.products.convertrite.utils.DataSourceUtil;

@Service
public class CloudLookUpServiceImpl implements CloudLookUpService {

	private static final Logger log = LoggerFactory.getLogger(CloudLookUpServiceImpl.class);

	@Autowired
	XxrCloudMasterLookUpSetRepository xxrCloudMasterLookUpSetRepository;
	@Autowired
	XxrCloudMasterLookupValuesRepository xxrCloudMasterLookupValuesRepository;
	@Autowired
	SaveCloudLookUpSetHeadersDaoImpl saveCloudLookUpSetHeadersDaoImpl;
	@Autowired
	SaveCloudLookUpSetColumnsDaoImpl saveCloudLookUpSetColumnsDaoImpl;
	@Autowired
	CloudMetaDataRepository cloudMetaDataRepository;
	@Autowired
	XxrLookupSetsRepository xxrLookupSetsRepository;
	@Autowired
	XxrLookUpValuesRepository xxrLookUpValuesRepository;
	@Autowired
	SaveLookUpSetDaoImpl saveLookUpSetDaoImpl;
	@Autowired
	SaveLookUpValuesDaoImpl saveLookUpValuesDaoImpl;
	@Autowired
	CloudLookupSearchDaoImpl cloudLookupSearchDaoImpl;
	@Autowired
	DataSourceUtil dataSourceUtil;


	@Override
	public List<XxrCloudMasterLookUpSet> getLookupSetHeaders() throws Exception {
		log.info("Start of getLookupSetHeaders Method in Service Class######");
		List<XxrCloudMasterLookUpSet> cloudLookUpSetList = new ArrayList<>();
		try {
			cloudLookUpSetList = xxrCloudMasterLookUpSetRepository.findAll();

		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}

		return cloudLookUpSetList;
	}

	@Override
	public List<XxrCloudMasterLookupValues> getLookupSetValues(Long lookUpSetId) throws Exception {
		log.info("Start of getLookupSetValues Method in Service Class #######");
		List<XxrCloudMasterLookupValues> cloudLookUpValuesList = new ArrayList<>();
		try {
			cloudLookUpValuesList = xxrCloudMasterLookupValuesRepository.getLookupSetValues(lookUpSetId);

		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}

		return cloudLookUpValuesList;
	}

	@Override
	public SaveCloudLookUpSetHeadersResPo saveCloudLookUpSetHeaders(
			List<SaveCloudLookUpSetHeadersPo> cloudLookUpSetHeadersPo) throws Exception {
		log.info("Start of saveCloudLookUpSetHeaders Method in Service Class######");
		String msg = "";
		Long lookupSetId;
		SaveCloudLookUpSetHeadersResPo saveCloudLookUpSetHeadersResPo = new SaveCloudLookUpSetHeadersResPo();
		try {
			msg = saveCloudLookUpSetHeadersDaoImpl.saveCloudLookUpSetHeaders(cloudLookUpSetHeadersPo);
			saveCloudLookUpSetHeadersResPo.setMessage(msg);

			SaveCloudLookUpSetHeadersPo saveCloudLookUpSetHeadersPo = cloudLookUpSetHeadersPo.get(0);
			String columnName = saveCloudLookUpSetHeadersPo.getColumnName();
			// String projectName = saveCloudLookUpSetHeadersPo.getProjectName();
			// Long podId = saveCloudLookUpSetHeadersPo.getPodId();

			if (!Validations.isNullOrEmpty(columnName)) {
				// lookupSetId = xxrCloudMasterLookUpSetRepository.getLookupSetId(podId,
				// columnName, projectName);
				lookupSetId = xxrCloudMasterLookUpSetRepository.getLookupSetId(columnName);
				saveCloudLookUpSetHeadersResPo.setLookUpSetId(lookupSetId);
			}

		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return saveCloudLookUpSetHeadersResPo;
	}

	@Override
	public SaveCloudLookUpSetColumnsResPo saveCloudLookUpSetValues(
			List<SaveCloudLookUpSetColumnsPo> cloudLookUpSetColumnsPo) throws Exception {
		log.info("Start of saveCloudLookUpSetValues Method in Service Class######");
		String msg = "";
		List<XxrCloudMasterLookupValues> cloudLookUpValuesList = new ArrayList<>();
		SaveCloudLookUpSetColumnsResPo saveCloudLookUpSetColumnsResPo = new SaveCloudLookUpSetColumnsResPo();
		try {
			msg = saveCloudLookUpSetColumnsDaoImpl.saveCloudLookUpSetValues(cloudLookUpSetColumnsPo);
			saveCloudLookUpSetColumnsResPo.setMessage(msg);
			Long lookUpSetId = cloudLookUpSetColumnsPo.get(0).getLookUpSetId();
			if (lookUpSetId != null) {
				cloudLookUpValuesList = xxrCloudMasterLookupValuesRepository.getLookupSetValues(lookUpSetId);
				saveCloudLookUpSetColumnsResPo.setCloudMasterLookUpColumns(cloudLookUpValuesList);
			}

		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return saveCloudLookUpSetColumnsResPo;
	}

	@Override
	public List<String> getColumnNames() throws Exception {
		log.info("Start of getColumnNames Method in Service Class######");
		List<String> columnNames = new ArrayList<>();
		try {
			cloudMetaDataRepository.findAll().stream().forEach(x -> {
				if (!columnNames.contains(x.getColumnId()))
					columnNames.add(x.getColumnId());
			});
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return columnNames;
	}

	@Override
	public List<XxrLookupSets> getLookupSet() throws Exception {
		log.info("Start of getLookupSet Method in Service Class######");
		List<XxrLookupSets> lookUpSetList = new ArrayList<>();
		try {
			lookUpSetList = xxrLookupSetsRepository.findAll();

		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}

		return lookUpSetList;
	}

	@Override
	public List<XxrLookUpValues> getlookupvalues(Long lookUpSetId) throws Exception {
		log.info("Start of getlookupvalues Method in Service Class######");
		List<XxrLookUpValues> lookUpValuesList = new ArrayList<>();
		try {
			lookUpValuesList = xxrLookUpValuesRepository.getLookupSetValues(lookUpSetId);

		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}

		return lookUpValuesList;
	}

	@Override
	public SaveCloudLookUpSetHeadersResPo saveLookUpSetHeaders(List<SaveLookUpSetPo> lookUpSetHeadersPo,
			HttpServletRequest request) throws ValidationException, Exception {
		log.info("Start of saveLookUpSetHeaders Method in Service Class######");
		String msg = "";
		Long lookupSetId;
		SaveCloudLookUpSetHeadersResPo saveCloudLookUpSetHeadersResPo = new SaveCloudLookUpSetHeadersResPo();
		try {
			msg = saveLookUpSetDaoImpl.saveLookUpSet(lookUpSetHeadersPo, request);
			saveCloudLookUpSetHeadersResPo.setMessage(msg);

			SaveLookUpSetPo saveLookUpSetHeadersPo = lookUpSetHeadersPo.get(0);
			String lookupSetName = saveLookUpSetHeadersPo.getLookUpSetName();

			if (!Validations.isNullOrEmpty(lookupSetName)) {
				// lookupSetId = xxrCloudMasterLookUpSetRepository.getLookupSetId(podId,
				// columnName, projectName);
				lookupSetId = xxrLookupSetsRepository.getLookupSetId(lookupSetName);
				saveCloudLookUpSetHeadersResPo.setLookUpSetId(lookupSetId);
			}

		} catch (ValidationException e) {
			throw new ValidationException(e.getMessage());
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return saveCloudLookUpSetHeadersResPo;
	}

	@Override
	public SaveLookUpValuesResPo saveLookUpValues(List<SaveLookUpValuesPo> lookUpValuesPo, HttpServletRequest request)
			throws ValidationException, BadRequestException, Exception {
		log.info("Start of saveLookUpValues Method in Service Class######");
		String msg = "";
		List<XxrLookUpValues> xxrLookUpValuesList = new ArrayList<>();
		SaveLookUpValuesResPo saveLookUpValuesResPo = new SaveLookUpValuesResPo();
		try {
			msg = saveLookUpValuesDaoImpl.saveLookUpValues(lookUpValuesPo, request);
			saveLookUpValuesResPo.setMessage(msg);
			Long lookUpSetId = lookUpValuesPo.get(0).getLookUpSetId();
			if (lookUpSetId != null) {
				xxrLookUpValuesList = xxrLookUpValuesRepository.getLookupSetValues(lookUpSetId);
				saveLookUpValuesResPo.setXxrLookUpValues(xxrLookUpValuesList);
			}

		} catch (ValidationException e) {
			throw new ValidationException(e.getMessage());
		} catch (BadRequestException e) {
			throw new BadRequestException(e.getMessage());
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return saveLookUpValuesResPo;
	}

	@Override
	public List<LookUpsetNameResPo> getLookupSetName(String lookupSetName) throws Exception {
		log.info("Start of getLookupSetName Method in Service Class######");
		List<LookUpsetNameResPo> xxrLookupSets = new ArrayList<>();
		try {
			xxrLookupSets = xxrLookupSetsRepository.getLookupSetName(lookupSetName.toLowerCase());
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return xxrLookupSets;
	}

	@Override
	public List<XxrLookupSets> searchByOperator(LookUpSearchReqPo lookUpSearchReqPo, HttpHeaders header)
			throws Exception {
		log.info("Start of searchByOperator Method in Service Class######");
		List<XxrLookupSets> xxrLookupSets = new ArrayList<>();
		try {

			xxrLookupSets = cloudLookupSearchDaoImpl.searchByOperator(lookUpSearchReqPo, header);

		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return xxrLookupSets;
	}

	@Override
	public List<XxrLookUpValues> getlookupvaluesByVal(Long lookUpSetId, String lookUpValue) {
		log.info("Start of getlookupvaluesByVal in service #####");
		List<XxrLookUpValues> lookUpValuesList = new ArrayList<>();
		lookUpValuesList = xxrLookUpValuesRepository.getlookupvaluesByVal(lookUpSetId,lookUpValue.toLowerCase());
		return lookUpValuesList;
	}

}
