package com.rite.products.convertrite.service;

import java.io.PrintWriter;

import javax.servlet.http.HttpServletRequest;

import org.springframework.web.multipart.MultipartFile;

import com.rite.products.convertrite.exception.ValidationException;
import com.rite.products.convertrite.po.LoadMetaDataFromCloudRes;


public interface LoadMetaDataService {

	String processSourceRecords(String fileName,String metaDataFileType,HttpServletRequest request) throws Exception;
	void downloadSourceFile(PrintWriter writer, String objectCode, String metaDataFileType, String environment) throws Exception;
	void downloadCloudFile(PrintWriter writer,String objectCode, String metaDataFileType) throws Exception;
	String processCloudRecords(String fileName, String metaDataFileType,HttpServletRequest request) throws Exception;
	String uploadFile(MultipartFile file) throws Exception;
	//ResponseEntity<XxrCloudDataProcess> processCloudMetaDataQuery(String objectCode)throws Exception;
	//LoadMetaDataFromCloudRes loadMetaDataFromCloud(String objectCode, String metaDataTableName, String requestId) throws Exception;
	LoadMetaDataFromCloudRes loadMetaDataFromCloud(String objectCode, String metaDataTableName,Long podId,Long projectId,
			HttpServletRequest request) throws ValidationException,Exception;
	LoadMetaDataFromCloudRes loadHdlCloudMetaData(String objectCode, String metaDataTableName, MultipartFile file) throws Exception;
	String uploadFileToFtp(MultipartFile file)  throws Exception ;
	LoadMetaDataFromCloudRes loadRejectionMetaDataFromCloud(String objectCode, Long podId, Long projectId,
			HttpServletRequest request,String metaDataTableName)throws Exception;
}
