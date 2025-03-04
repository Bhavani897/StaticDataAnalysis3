package com.rite.products.convertrite.service;

import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.web.multipart.MultipartFile;

import com.rite.products.convertrite.exception.BadRequestException;
import com.rite.products.convertrite.exception.ValidationException;
import com.rite.products.convertrite.model.FbdiTempHdrsView;
import com.rite.products.convertrite.model.XxrFbdiTempCols;
import com.rite.products.convertrite.po.FbdiColumnSequencePo;
import com.rite.products.convertrite.po.SaveFbdiTemplateColumnsPo;
import com.rite.products.convertrite.po.SaveFbdiTemplateColumnsResPo;
import com.rite.products.convertrite.po.SaveFbdiTemplateHeaderPo;
import com.rite.products.convertrite.po.SaveTemplateHeaderResponsePo;

public interface FbdiService {

	List<FbdiColumnSequencePo> getFbdiColumnSequence(String excelFileName,String version,HttpServletResponse resp) throws Exception;

	List<FbdiTempHdrsView> getFbdiTemplates(Long roleId) throws Exception;

	String getFbdiTemplateName(String objectCode) throws Exception;

	String getSheetName(Long objectId) throws Exception;

	List<XxrFbdiTempCols> getFbdiTemplateColumns(Long fbdiTemplateId)throws Exception;

	SaveTemplateHeaderResponsePo saveFbdiTemplateHdrs(List<SaveFbdiTemplateHeaderPo> saveFbdiTemplateHeaderPo,HttpServletRequest request)throws Exception;

	SaveFbdiTemplateColumnsResPo saveFbdiTemplateColumns(List<SaveFbdiTemplateColumnsPo> fbdiTemplateColumnsPo,HttpServletRequest request)throws Exception;

	SaveTemplateHeaderResponsePo saveCtlAsBlob(Long objectId, String version)throws Exception;

	List<FbdiColumnSequencePo> uploadCtlGetColumnSeq(MultipartFile file, String version, Long objectId) throws Exception;

	List<FbdiColumnSequencePo> generateSapFbdiSeq(String objectCode) throws Exception;

	SaveTemplateHeaderResponsePo saveFbdiTemplateHdrsJpa(SaveFbdiTemplateHeaderPo saveFbdiTemplateHeaderPo,
			HttpServletRequest request)throws ValidationException;

	SaveFbdiTemplateColumnsResPo saveFbdiTemplateColumnsWithJpa(List<SaveFbdiTemplateColumnsPo> fbdiTemplateColumnsPo,
			HttpServletRequest request) throws BadRequestException;

}
