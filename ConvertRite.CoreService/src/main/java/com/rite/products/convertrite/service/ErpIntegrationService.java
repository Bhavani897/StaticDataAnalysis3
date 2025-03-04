package com.rite.products.convertrite.service;

import java.util.List;

import javax.servlet.http.HttpServletResponse;

import com.rite.products.convertrite.po.*;
import org.springframework.http.HttpHeaders;

import com.rite.products.convertrite.exception.ValidationException;
import com.rite.products.convertrite.model.XxrErpIntegration;

public interface ErpIntegrationService {

	LoadandImportDataResPo loadAndImportData(LoadandImportDataReqPo loadandImportDataReqPo) throws Exception;

	LoadImportJobStatusResPo getJobStatus(Long resultId,Long cldTemplateId)throws Exception;

	List<XxrErpIntegration> getErpIntegrationDetails(ErpIntegrationPageReqPo erpIntegrationPageReqPo, HttpHeaders httpHeaders)throws Exception;

	byte[] downloadExportOutput(String requestId,Long cldTemplateId,HttpServletResponse response)throws Exception;

	LoadandImportDataResPo loadAndImportDataV1(LoadandImportDataReqPo loadandImportDataReqPo, String bearerToken) throws ValidationException,Exception;

	LoadandImportDataResPo loadAndImportDataV2(LoadandImportDataReqPo loadandImportDataReqPo, String bearerToken);

	BasicResponsePo getCldImportStatus(Long cldTemplateId, String batchName);


}
