package com.rite.products.convertrite.service;

import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.rite.products.convertrite.po.*;
import org.springframework.http.HttpHeaders;

import com.rite.products.convertrite.exception.ValidationException;
import com.rite.products.convertrite.model.XxrCloudDataProcess;

public interface HcmDataImportService {

	DataSetStausResPo getDataSetStatus(String contentId, String processId, Long cldTemplateId)
			throws ValidationException, Exception;

	HcmLoadAndImportDataRes hcmLoadAndImportData(HcmLoadandImportDataReqPo hcmLoadandImportDataReqPo, String bearerToken, HttpServletResponse response)
			throws ValidationException, Exception;

	List<XxrHcmDataLoaderResPo> getHcmDataLoaderDetails(HcmDetailsPageReqPo hcmDetailsPageReqPo,HttpHeaders httpHeaders) throws Exception;

	CrCloudDataProcessResPo processHdlReconcile(Long cldTemplateId,String batchName,HttpServletRequest request)
			throws Exception;

	CrCloudDataProcessResPo processHdlSummary(String batchName,Long cldTemplateId, HttpServletRequest request) throws Exception;

	XxrCloudDataProcess processHdlStatus(String contentId,Long podId,Long projectId, HttpServletRequest request) throws Exception;
	void hdlReport(Long statusId, Long id, String reportType,HttpServletResponse response) throws ValidationException,Exception;
	CrHcmCloudImportStatusResPo hcmCldImportSummary(Long statusId, Long id) throws ValidationException,Exception;
}
