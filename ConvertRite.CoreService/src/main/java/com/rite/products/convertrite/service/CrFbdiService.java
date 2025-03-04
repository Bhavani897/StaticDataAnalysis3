package com.rite.products.convertrite.service;

import com.rite.products.convertrite.model.CrFbdiCols;
import com.rite.products.convertrite.model.CrFbdiHdrs;
import com.rite.products.convertrite.model.CrFbdiView;
import com.rite.products.convertrite.po.*;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.List;


public interface CrFbdiService {
    List<FbdiColumnSequencePo> getFbdiColumnSequence(String excelFileName, String version, HttpServletResponse resp) throws Exception;

      CrSaveFbdiTempColsResPo saveFbdiTemplateColumns(List<SaveFbdiTemplateColumnsPo> fbdiTemplateColumnsPo,
                                                      HttpServletRequest request) throws Exception;

    SaveFbdiHdrsPo saveFbdiTemplateHdrs(SaveFbdiTempHdrsPo fbdiTemplateHeaderPo, HttpServletRequest request) throws Exception;

    public List<CrFbdiHdrs>  getFbdiTemplates() throws Exception;

    List<CrFbdiCols> getFbdiTemplateColumns(Long fbdiTemplateId) throws Exception;

    List<FbdiColumnSequencePo> uploadCtlGetColumnSeq(MultipartFile file, String version, Long objectId);
    
    public List<CrFbdiView> getAllFbdITemplates();
}
