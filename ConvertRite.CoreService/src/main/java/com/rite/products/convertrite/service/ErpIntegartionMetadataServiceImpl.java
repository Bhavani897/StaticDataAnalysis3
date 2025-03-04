package com.rite.products.convertrite.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.rite.products.convertrite.exception.ValidationException;
import com.rite.products.convertrite.model.XxrErpIntegrationMetaData;
import com.rite.products.convertrite.po.ErpIntegrationMetaDataReqPo;
import com.rite.products.convertrite.po.SaveErpIntegrationMetaDataResPo;
import com.rite.products.convertrite.po.XxrErpIntegrationMetaDataResPo;
import com.rite.products.convertrite.respository.XxrErpIntegrationMetaDataRepository;
import com.rite.products.convertrite.respository.XxrLookUpValuesRepository;

@Service
public class ErpIntegartionMetadataServiceImpl implements ErpIntegartionMetadataService {

	@Autowired
	XxrErpIntegrationMetaDataRepository xxrErpIntegrationMetaDataRepository;
	@Autowired
	XxrLookUpValuesRepository xxrLookUpValuesRepository;

	@Override
	public SaveErpIntegrationMetaDataResPo saveErpIntegrationMetaData(
			ErpIntegrationMetaDataReqPo erpIntegrationMetaDataReqPo) throws ValidationException,Exception {
		SaveErpIntegrationMetaDataResPo erpIntegrationMetaDataResPo = new SaveErpIntegrationMetaDataResPo();
		XxrErpIntegrationMetaData erpIntegrationMetaDataRes = new XxrErpIntegrationMetaData();
		XxrErpIntegrationMetaData x = new XxrErpIntegrationMetaData();
		try {
		
			if (erpIntegrationMetaDataReqPo.getId() == null) {
				x = xxrErpIntegrationMetaDataRepository.findByParentObjectIdAndObjectId(
						erpIntegrationMetaDataReqPo.getParentObjectId(), erpIntegrationMetaDataReqPo.getObjectId());
				if (x != null)
					throw new ValidationException("combination of objectId and parentObjectId should be unique");
				XxrErpIntegrationMetaData erpIntegrationMetaData = new XxrErpIntegrationMetaData();
//			erpIntegrationMetaData.setPodId(erpIntegrationMetaDataReqPo.getPodId());
//			erpIntegrationMetaData.setProjectId(erpIntegrationMetaDataReqPo.getProjectId());
				erpIntegrationMetaData.setObjectId(erpIntegrationMetaDataReqPo.getObjectId());
				erpIntegrationMetaData.setParentObjectId(erpIntegrationMetaDataReqPo.getParentObjectId());
				erpIntegrationMetaData.setInterfaceDetails(erpIntegrationMetaDataReqPo.getInterfaceDetails());
				erpIntegrationMetaData.setJobName(erpIntegrationMetaDataReqPo.getJobName());
				erpIntegrationMetaData.setDocumentAccount(erpIntegrationMetaDataReqPo.getDocumentAccount());
				erpIntegrationMetaData.setDocumentSecurityGroup(erpIntegrationMetaDataReqPo.getDocumentSecurityGroup());
				erpIntegrationMetaDataRes = xxrErpIntegrationMetaDataRepository.save(erpIntegrationMetaData);
				erpIntegrationMetaDataRes.setCreationDate(new java.sql.Date(new java.util.Date().getTime()));
				erpIntegrationMetaDataRes.setCreatedBy("ConvertRite");
				erpIntegrationMetaDataRes.setLastUpdatedDate(new java.sql.Date(new java.util.Date().getTime()));
				erpIntegrationMetaDataRes.setLastUpdateBy("ConvertRite");
			} else {
				XxrErpIntegrationMetaData exisitngErpIntegrationMetaData = new XxrErpIntegrationMetaData();
				Optional<XxrErpIntegrationMetaData> existingIntegrationMetaDataOpt = xxrErpIntegrationMetaDataRepository
						.findById(erpIntegrationMetaDataReqPo.getId());
				if (existingIntegrationMetaDataOpt.isPresent())
					exisitngErpIntegrationMetaData = existingIntegrationMetaDataOpt.get();
				else
					throw new Exception(
							"There is no existing record with this id :" + erpIntegrationMetaDataReqPo.getId());

				exisitngErpIntegrationMetaData.setObjectId(erpIntegrationMetaDataReqPo.getObjectId());
				exisitngErpIntegrationMetaData.setParentObjectId(erpIntegrationMetaDataReqPo.getParentObjectId());
				exisitngErpIntegrationMetaData.setInterfaceDetails(erpIntegrationMetaDataReqPo.getInterfaceDetails());
				exisitngErpIntegrationMetaData.setJobName(erpIntegrationMetaDataReqPo.getJobName());
				exisitngErpIntegrationMetaData.setDocumentAccount(erpIntegrationMetaDataReqPo.getDocumentAccount());
				exisitngErpIntegrationMetaData
						.setDocumentSecurityGroup(erpIntegrationMetaDataReqPo.getDocumentSecurityGroup());
				exisitngErpIntegrationMetaData.setLastUpdatedDate(new java.sql.Date(new java.util.Date().getTime()));
				exisitngErpIntegrationMetaData.setLastUpdateBy("ConvertRite");
				erpIntegrationMetaDataRes = xxrErpIntegrationMetaDataRepository.save(exisitngErpIntegrationMetaData);

			}
			erpIntegrationMetaDataResPo.setId(erpIntegrationMetaDataRes.getId());
			erpIntegrationMetaDataResPo.setMessage("ErpIntegrationMetadata details is saved successfully");

		} catch (ValidationException e) {
			throw new ValidationException(e.getMessage());
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return erpIntegrationMetaDataResPo;
	}

	@Override
	public List<XxrErpIntegrationMetaDataResPo> getErpIntegrationMetaData() throws Exception {
		List<XxrErpIntegrationMetaData> xxrErpIntegrationMetaDataLi = new ArrayList<>();
		List<XxrErpIntegrationMetaDataResPo> erpIntegrationMetaDataResPo = new ArrayList<>();
		try {
			xxrErpIntegrationMetaDataLi = xxrErpIntegrationMetaDataRepository.findAll();

			xxrErpIntegrationMetaDataLi.stream().forEach(x -> {
				XxrErpIntegrationMetaDataResPo xxrErpIntegrationMetaDataResPo = new XxrErpIntegrationMetaDataResPo();

				// String podName = xxrLookUpValuesRepository.getValueById(x.getPodId());
				// String projectName =
				// xxrLookUpValuesRepository.getValueById(x.getProjectId());
				String objectCode = xxrLookUpValuesRepository.getValueById(x.getObjectId());
				String parentObjectCode = xxrLookUpValuesRepository.getValueById(x.getParentObjectId());

				xxrErpIntegrationMetaDataResPo.setId(x.getId());
				xxrErpIntegrationMetaDataResPo.setInterfaceDetails(x.getInterfaceDetails());
				xxrErpIntegrationMetaDataResPo.setJobName(x.getJobName());
				xxrErpIntegrationMetaDataResPo.setDocumentAccount(x.getDocumentAccount());
				xxrErpIntegrationMetaDataResPo.setObjectId(x.getObjectId());
				xxrErpIntegrationMetaDataResPo.setObjectCode(objectCode);
				xxrErpIntegrationMetaDataResPo.setParentObjectId(x.getParentObjectId());
				xxrErpIntegrationMetaDataResPo.setParentObjectCode(parentObjectCode);
				/*
				 * xxrErpIntegrationMetaDataResPo.setPodId(x.getPodId());
				 * xxrErpIntegrationMetaDataResPo.setPodName(podName);
				 * xxrErpIntegrationMetaDataResPo.setProjectId(x.getProjectId());
				 * xxrErpIntegrationMetaDataResPo.setProjectName(projectName);
				 */
				xxrErpIntegrationMetaDataResPo.setDocumentSecurityGroup(x.getDocumentSecurityGroup());

				erpIntegrationMetaDataResPo.add(xxrErpIntegrationMetaDataResPo);

			});

		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return erpIntegrationMetaDataResPo;
	}

	/*
	 * @Override public XxrErpIntegrationMetaDataResPo
	 * getErpIntegrationMetaDataByIds(Long podId, Long projectId, Long
	 * parentObjectId, Long objectId) throws Exception { // TODO Auto-generated
	 * method stub XxrErpIntegrationMetaData x = new XxrErpIntegrationMetaData();
	 * XxrErpIntegrationMetaDataResPo xxrErpIntegrationMetaDataResPo = new
	 * XxrErpIntegrationMetaDataResPo(); try { x =
	 * xxrErpIntegrationMetaDataRepository.
	 * findByPodIdAndProjectIdAndParentObjectIdAndObjectId(podId, projectId,
	 * parentObjectId, objectId); if (x != null) { String podName =
	 * xxrLookUpValuesRepository.getValueById(x.getPodId()); String projectName =
	 * xxrLookUpValuesRepository.getValueById(x.getProjectId()); String objectCode =
	 * xxrLookUpValuesRepository.getValueById(x.getObjectId()); String
	 * parentObjectCode =
	 * xxrLookUpValuesRepository.getValueById(x.getParentObjectId());
	 * 
	 * xxrErpIntegrationMetaDataResPo.setId(x.getId());
	 * xxrErpIntegrationMetaDataResPo.setInterfaceDetails(x.getInterfaceDetails());
	 * xxrErpIntegrationMetaDataResPo.setJobName(x.getJobName());
	 * xxrErpIntegrationMetaDataResPo.setDocumentAccount(x.getDocumentAccount());
	 * xxrErpIntegrationMetaDataResPo.setObjectId(x.getObjectId());
	 * xxrErpIntegrationMetaDataResPo.setObjectCode(objectCode);
	 * xxrErpIntegrationMetaDataResPo.setParentObjectId(x.getParentObjectId());
	 * xxrErpIntegrationMetaDataResPo.setParentObjectCode(parentObjectCode);
	 * xxrErpIntegrationMetaDataResPo.setPodId(x.getPodId());
	 * xxrErpIntegrationMetaDataResPo.setPodName(podName);
	 * xxrErpIntegrationMetaDataResPo.setProjectId(x.getProjectId());
	 * xxrErpIntegrationMetaDataResPo.setProjectName(projectName);
	 * xxrErpIntegrationMetaDataResPo.setDocumentSecurityGroup(x.
	 * getDocumentSecurityGroup()); }
	 * 
	 * } catch (Exception e) { throw new Exception(e.getMessage()); } return
	 * xxrErpIntegrationMetaDataResPo; }
	 */

	@Override
	public XxrErpIntegrationMetaDataResPo getErpIntegrationMetaDataByIds(Long parentObjectId, Long objectId)
			throws Exception {
		XxrErpIntegrationMetaData x = new XxrErpIntegrationMetaData();
		XxrErpIntegrationMetaDataResPo xxrErpIntegrationMetaDataResPo = new XxrErpIntegrationMetaDataResPo();
		try {
			x = xxrErpIntegrationMetaDataRepository.findByParentObjectIdAndObjectId(parentObjectId, objectId);
			if (x != null) {
				String objectCode = xxrLookUpValuesRepository.getValueById(x.getObjectId());
				String parentObjectCode = xxrLookUpValuesRepository.getValueById(x.getParentObjectId());

				xxrErpIntegrationMetaDataResPo.setId(x.getId());
				xxrErpIntegrationMetaDataResPo.setInterfaceDetails(x.getInterfaceDetails());
				xxrErpIntegrationMetaDataResPo.setJobName(x.getJobName());
				xxrErpIntegrationMetaDataResPo.setDocumentAccount(x.getDocumentAccount());
				xxrErpIntegrationMetaDataResPo.setObjectId(x.getObjectId());
				xxrErpIntegrationMetaDataResPo.setObjectCode(objectCode);
				xxrErpIntegrationMetaDataResPo.setParentObjectId(x.getParentObjectId());
				xxrErpIntegrationMetaDataResPo.setParentObjectCode(parentObjectCode);
				xxrErpIntegrationMetaDataResPo.setDocumentSecurityGroup(x.getDocumentSecurityGroup());
			}

		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return xxrErpIntegrationMetaDataResPo;
	}

}
