package com.rite.products.convertrite.service;

import java.io.IOException;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.http.HttpHeaders;

import com.rite.products.convertrite.exception.ValidationException;
import com.rite.products.convertrite.model.CallBackReqPo;
import com.rite.products.convertrite.model.XxrCloudDataProcess;
import com.rite.products.convertrite.model.XxrProcessJobs;
import com.rite.products.convertrite.model.XxrProcessRequestsView;
import com.rite.products.convertrite.model.XxrSourceLoadFailRecordsView;
import com.rite.products.convertrite.po.DeleteStagingDataReq;
import com.rite.products.convertrite.po.DeleteStagingDataRes;
import com.rite.products.convertrite.po.GetRecordsPostJobExecution;
import com.rite.products.convertrite.po.ProcessJobPo;
import com.rite.products.convertrite.po.ProcessReqByRequestTypeReq;
import com.rite.products.convertrite.po.ProcessSourceDataResPo;
import com.rite.products.convertrite.po.SourceTemplateStatisticsRes;
import com.rite.products.convertrite.po.SyncCloudInterfaceDataReq;
import com.rite.products.convertrite.po.SyncCloudInterfaceDataResPo;
import com.rite.products.convertrite.po.UpdateFailedRecReqPo;
import com.rite.products.convertrite.po.UpdateFailedRecResp;
import com.rite.products.convertrite.po.XxrSourceFailRecordsReqPo;
import com.rite.products.convertrite.po.XxrSourceLoadFailRecordsResPo;

public interface DataService {

	String generateFbdi(String cloudTemplateName, HttpServletResponse response) throws Exception;

	ProcessJobPo processJob(String cloudTemplateName, String type,HttpServletRequest request) throws Exception;

	List<XxrProcessJobs> getProcessJobs() throws Exception;

	void getRecordsPostJobExecution( GetRecordsPostJobExecution getRecordsPostJobExecution, HttpServletResponse response)
			throws Exception;

	ProcessSourceDataResPo processSourceStagingData(String fileName, String templateName,String batchName,HttpServletRequest request) throws Exception;

	void processSourceData(String fileName, String templateName, String path) throws Exception;

	void exportFailedRecords(Long sourceTemplateId,Long id,HttpServletResponse response,String type)throws Exception;

	XxrSourceLoadFailRecordsResPo getStageDataLoadStatus(Long sourceTemplateId)throws Exception;

	void generateFbdiFromLob(Long cloudTemplateId, HttpServletResponse response)throws Exception;

	List<XxrSourceLoadFailRecordsView> getAllStageDataLoadStatus(XxrSourceFailRecordsReqPo xxrSourceFailRecordsReqPo,HttpHeaders httpHeaders)throws Exception;

	List<SourceTemplateStatisticsRes> getSourceTemplatesStatistics()throws Exception;

	
	 List<XxrCloudDataProcess> processReconcile(Long cloudTemplateId,HttpServletRequest request) throws Exception;

	void processReconcileReport(Long cloudTemplateId, List<String> requestId, HttpServletResponse response)throws Exception;

	void generateHdlFromLob(String cloudTemplateId, String batchName, String isIntialLoad, HttpServletResponse response)
			throws Exception;

	UpdateFailedRecResp updateEditedFailedRecords(UpdateFailedRecReqPo updateFailedRecReqPo)throws Exception;

	void getTransformationReport(Long cloudTemplateId, String batchName,HttpServletResponse response)throws Exception;

	DeleteStagingDataRes deleteStagingData(DeleteStagingDataReq deleteStagingDataReq)throws Exception;

	void generateXlsFromLob(Long cloudTemplateId, HttpServletResponse response) throws IOException, Exception;

	SyncCloudInterfaceDataResPo syncCloudInterfaceTbleData(SyncCloudInterfaceDataReq syncCloudInterfaceDataReq, HttpServletRequest request)  throws Exception;

	void onFallbackmethod(CallBackReqPo request) throws ValidationException;

	List<String> getBatchNames(Long templateId) throws Exception;

	void generateFbdiFromLobV1(Long cloudTemplateId, String batchName,HttpServletResponse response) throws Exception;

	List<XxrProcessRequestsView> getPrcRequestsByRequestType(ProcessReqByRequestTypeReq processReqByRequestTypeReq,
			HttpHeaders httpHeaders);
}
