package com.rite.products.convertrite.service;

import java.util.List;

import javax.servlet.http.HttpServletResponse;

import com.rite.products.convertrite.exception.BadRequestException;
import com.rite.products.convertrite.po.DeleteRes;
import org.springframework.http.HttpHeaders;

import com.rite.products.convertrite.model.XxrCloudDataProcess;
import com.rite.products.convertrite.model.XxrCloudDataProcessView;
import com.rite.products.convertrite.po.CloudDataProcessPagReqPo;
import com.rite.products.convertrite.po.CloudDataProcessingReqPo;

public interface CloudDataProcessingService {

	XxrCloudDataProcess cloudDataProcessingRequests(CloudDataProcessingReqPo cloudDataProcessingReqPo) throws  Exception,BadRequestException;

	List<XxrCloudDataProcessView> getAllCloudDataRequests(CloudDataProcessPagReqPo cloudDataProcessPagReqPo,HttpHeaders httpHeaders)throws Exception;

	void downloadCsvFile(String requestId, HttpServletResponse response) throws Exception;


	void deleteAdhocData(String requestId)throws Exception;


}
