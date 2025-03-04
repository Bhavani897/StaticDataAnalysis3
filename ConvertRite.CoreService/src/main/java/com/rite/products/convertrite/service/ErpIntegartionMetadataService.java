package com.rite.products.convertrite.service;

import java.util.List;

import com.rite.products.convertrite.exception.ValidationException;
import com.rite.products.convertrite.po.ErpIntegrationMetaDataReqPo;
import com.rite.products.convertrite.po.SaveErpIntegrationMetaDataResPo;
import com.rite.products.convertrite.po.XxrErpIntegrationMetaDataResPo;

public interface ErpIntegartionMetadataService {

	SaveErpIntegrationMetaDataResPo saveErpIntegrationMetaData(ErpIntegrationMetaDataReqPo erpIntegrationMetaDataReqPo)
			throws ValidationException,Exception;

	List<XxrErpIntegrationMetaDataResPo> getErpIntegrationMetaData() throws Exception;

	XxrErpIntegrationMetaDataResPo getErpIntegrationMetaDataByIds(Long parentObjectId, Long objectId) throws Exception;

	/*
	 * XxrErpIntegrationMetaDataResPo getErpIntegrationMetaDataByIds(Long podId,
	 * Long projectId, Long parentObjectId, Long objectId) throws Exception;
	 */

}
