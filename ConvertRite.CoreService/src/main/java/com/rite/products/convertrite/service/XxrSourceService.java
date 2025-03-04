package com.rite.products.convertrite.service;

import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.rite.products.convertrite.exception.BadRequestException;
import com.rite.products.convertrite.exception.ValidationException;
import com.rite.products.convertrite.model.CrSourceTemplateColumns;
import com.rite.products.convertrite.model.CrSourceTemplateHeaders;
import com.rite.products.convertrite.model.CrSourceTemplateHeadersView;
import com.rite.products.convertrite.model.SourceTemplateHeaders;
import com.rite.products.convertrite.po.*;

public interface XxrSourceService {
	List<CrSourceTemplateHeadersView> getSourceTemplates() throws Exception;
	List<SourceTemplateHeadersResPo> getSourceTemplateHeaderById(long templateId) throws Exception;

	List<SourceTemplateHeaders> getSourceTemplatesByPo(SourceTemplatePo sourceTemplatePo) throws Exception;

	List<Object> getSourceTemplateColumns(long templateId) throws Exception;

	List<CrColumnPo> getSourceColumnsByName(String sourceTableName) throws Exception;

	List<ColumnPo> getSourceColumnsById(long sourceTableId) throws Exception;

	SaveTemplateHeaderResponsePo saveSourceTemplateHeaders(CrSourceTemplateHeaders sourceTemplateHeadersPo, HttpServletRequest request) throws Exception;

	SaveSourceTemplateColumnsResPo saveSourceTemplateColumns(List<CrSourceTemplateColumns> sourceTemplateColumnsPo, HttpServletRequest request) throws Exception;

	SourceStagingTablePo createSourceStaggingTab(Long tableId, Long templateId, String templateCode, String environment, HttpServletRequest request) throws Exception;

	CreateDynamicViewPo createDynamicView(Long templateId, String stgTableName,HttpServletRequest request) throws Exception;

	List<SourceTablesPo> getSourceTableNames(Long objectId) throws Exception;

	SaveTemplateHeaderResponsePo copySourceTemplate(String newTemplateName, Long templateId,HttpServletRequest request)throws Exception;
	BasicResPo loadSourceData(LoadSourceDataReqPo loadSourceDataReqPo,String tenantId,String userId) throws Exception;
	void downloadFailedRecLogFile(Long failRecId, HttpServletResponse resp)throws Exception;
	SaveSourceTemplateColumnsResPo saveSourceTemplateColumnsWithJpa(
			List<SaveSourceTemplatesColumnsPo> sourceTemplateColumnsPo) throws BadRequestException,ValidationException;
	SavingSourceTemplateHeadersResPo saveSourceTemplateHeadersWithoutProc(List<SaveSourceTemplateHeadersPo> sourceTemplateHeadersPo,
																		  HttpServletRequest request) throws Exception;

	BasicResPo repopulateOrigTransRef(ReOrigTransRefReqPo reOrigTransRefReqPo
			,HttpServletRequest request) throws Exception;

	List<LoadSourceDataParentObjectResPo> loadSourceDataAtParentObject(LoadSourceDataReqAtParentObjectPo loadSourceDataReqPo,
																	  String tenantId,String userId) throws ValidationException,Exception;

	SrcTemplateColsUpdtRes srcTemplateColumnsUpdate(SrcTemplateColsUpdateReq srcTemplateColsUpdateReq,HttpServletRequest request);

	Object loadSourceTemplateMetaData(String type,String fileName, Long objectId, HttpServletRequest request);

	BasicResPo loadSourceData(String dataFileName, String batchName, Long templateId, String templateName, String userId);
}