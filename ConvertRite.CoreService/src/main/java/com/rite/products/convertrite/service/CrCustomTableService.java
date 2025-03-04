package com.rite.products.convertrite.service;

import com.rite.products.convertrite.exception.ValidationException;
import com.rite.products.convertrite.po.*;
import org.springframework.stereotype.Service;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;

@Service
public interface CrCustomTableService {

    BasicResponsePo createCustomTable(CrCreateCustomTableReqPo customTableReqPo) throws ValidationException;

    BasicResponsePo getCustomTableDtls();

    BasicResponsePo descCustomTable(String tableName);

    BasicResponsePo modifyCustomTblColumns(CrModifyCustomTblColumnsReqPo mdfyCustmTblReq, HttpServletRequest request);

    BasicResponsePo loadDataToCustomTable(LoadCustomDataReqPo loadCustomDataReqPo, HttpServletRequest request) throws Exception;

    void downloadCustmTblFailedRecLogFile(Long customTableId, String crBatchName, HttpServletResponse resp) throws Exception;

    void getCustmTblRecsByBatchName(GetCustomTableRecordsReqPo custmTableReqPo, HttpServletResponse response, PrintWriter writer, HttpServletRequest request) throws Exception;

    BasicResponsePo getBatchNamesByCustomTblId(Long customTableId);

    void downloadCustmTblFailedRecBadFile(Long customTableId, String crBatchName, HttpServletResponse resp)throws Exception;
}
