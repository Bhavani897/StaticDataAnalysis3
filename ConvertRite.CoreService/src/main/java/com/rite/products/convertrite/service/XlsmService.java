package com.rite.products.convertrite.service;

import java.util.List;

import javax.servlet.http.HttpServletResponse;

import com.rite.products.convertrite.model.XxrXlsmTempCols;
import com.rite.products.convertrite.po.FbdiColumnSequencePo;
import com.rite.products.convertrite.po.SaveTemplateHeaderResponsePo;
import com.rite.products.convertrite.po.SaveXlsmTemplateColumnsPo;
import com.rite.products.convertrite.po.SaveXlsmTemplateColumnsResPo;
import com.rite.products.convertrite.po.SaveXlsmTemplateHeaderPo;
import com.rite.products.convertrite.po.XxrXlsmTempHdrsResp;


public interface XlsmService {

	List<XxrXlsmTempHdrsResp> getXlsmTemplates() throws Exception;

	List<XxrXlsmTempCols> getXlsmTemplateColumns(Long templateId) throws Exception;

	String getXlsmTemplateName(String objectCode)throws Exception;

	String getSheetName(String objectCode)throws Exception;

	SaveTemplateHeaderResponsePo saveXlsmTemplateHdrs(SaveXlsmTemplateHeaderPo xlsmTemplateHeaderPo)throws Exception;

	List<FbdiColumnSequencePo> getXlsmColumnSequence(String fileName,String sheetName, String version, HttpServletResponse response)throws Exception;

	SaveXlsmTemplateColumnsResPo saveXlsmTemplateColumns(List<SaveXlsmTemplateColumnsPo> xlsmTemplateColumnsPo)throws Exception;

	void downloadXlsmColumnCsv(String fileName, String sheetName, String version, HttpServletResponse response)throws Exception;

	SaveTemplateHeaderResponsePo saveXlsmAsBlob(Long objectId, String version)throws Exception;


}
