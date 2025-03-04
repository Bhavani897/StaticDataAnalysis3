package com.rite.products.convertrite.controller;


import com.rite.products.convertrite.exception.BadRequestException;
import com.rite.products.convertrite.exception.ValidationException;
import com.rite.products.convertrite.po.*;
import com.rite.products.convertrite.respository.CrFileDetailsRepo;
import com.rite.products.convertrite.service.CrCustomTableService;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.validation.Valid;
import java.io.IOException;
import java.io.PrintWriter;

@RestController
@RequestMapping("/api/convertritecore/customtables")
@Slf4j
public class CrCustomTablesController {

    @Autowired
    CrCustomTableService crCustomTableService;

    @ApiOperation(value = "Create custom table & load meta data")
    @ApiResponses(value = {@ApiResponse(code = 200, message = "Successful Response"),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 500, message = "Server Side Error")})
    @PostMapping("/createcustomtable")
    public ResponseEntity<BasicResponsePo> createCustomTable(@RequestBody @Valid CrCreateCustomTableReqPo customTableReqPo) throws ValidationException {
        return new ResponseEntity<BasicResponsePo>(crCustomTableService.createCustomTable(customTableReqPo), HttpStatus.OK);
    }

    @ApiOperation(value = "Get Custom table details")
    @ApiResponses(value = {@ApiResponse(code = 200, message = "Successful Response"),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 500, message = "Server Side Error")})
    @GetMapping("/getcustomtabledtls")
    public ResponseEntity<BasicResponsePo> getCustomTableDtls() {
        return new ResponseEntity<BasicResponsePo>(crCustomTableService.getCustomTableDtls(), HttpStatus.OK);
    }

    @ApiOperation(value = "Describe custom table")
    @ApiResponses(value = {@ApiResponse(code = 200, message = "Successful Response"),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 500, message = "Server Side Error")})
    @GetMapping("/describecustomtable")
    public ResponseEntity<BasicResponsePo> descCustomTable(@RequestParam("tableName") String tableName) {
        return new ResponseEntity<BasicResponsePo>(crCustomTableService.descCustomTable(tableName), HttpStatus.OK);
    }

    @ApiOperation(value = "Modify Custom Table Columns")
    @ApiResponses(value = {@ApiResponse(code = 200, message = "Successful Response"),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 500, message = "Server Side Error")})
    @PostMapping("/modifycutomtablecolumns")
    public ResponseEntity<BasicResponsePo> modifyCustomTblColumns(@RequestBody @Valid CrModifyCustomTblColumnsReqPo mdfyCustmTblReq, HttpServletRequest request) {
        return new ResponseEntity<BasicResponsePo>(crCustomTableService.modifyCustomTblColumns(mdfyCustmTblReq, request), HttpStatus.OK);
    }


    @ApiOperation(value = "This Api is for loading data to custom table")
    @ApiResponses(value = {@ApiResponse(code = 200, message = "Successful Response"),
            @ApiResponse(code = 500, message = "Server Side Error"),
            @ApiResponse(code = 400, message = "Bad Request")})
    @PostMapping("/loaddatatocustomtable")
    public ResponseEntity<BasicResponsePo> loadDataToCustomTable(@RequestBody LoadCustomDataReqPo loadCustomDataReqPo,
                                                                 HttpServletRequest request) throws Exception {
        return new ResponseEntity<BasicResponsePo>(crCustomTableService.loadDataToCustomTable(loadCustomDataReqPo, request), HttpStatus.OK);
    }

    @ApiOperation(value = "Api downloads log file of failed records during load custom table data")
    @ApiResponses(value = {@ApiResponse(code = 200, message = "Successful Response"),
            @ApiResponse(code = 500, message = "Server Side Error"),
            @ApiResponse(code = 400, message = "Bad Request")})
    @GetMapping("/downloadcustomtblfailedreclogfile")
    public void downloadCustmTblFailedRecLogFile(@RequestParam("customTableId") Long customTableId, @RequestParam("crBatchName") String crBatchName, HttpServletResponse resp) throws Exception {
        crCustomTableService.downloadCustmTblFailedRecLogFile(customTableId, crBatchName, resp);
    }

    @ApiOperation(value = "Api downloads bad file of failed records during load custom table data")
    @ApiResponses(value = {@ApiResponse(code = 200, message = "Successful Response"),
            @ApiResponse(code = 500, message = "Server Side Error"),
            @ApiResponse(code = 400, message = "Bad Request")})
    @GetMapping("/downloadcustomtblfailedrecbadfile")
    public void downloadCustmTblFailedRecBadFile(@RequestParam("customTableId") Long customTableId, @RequestParam("crBatchName") String crBatchName, HttpServletResponse resp) throws Exception {
        crCustomTableService.downloadCustmTblFailedRecBadFile(customTableId, crBatchName, resp);
    }

    @ApiOperation(value = "Api downloads custom table records of particular batch")
    @ApiResponses(value = {@ApiResponse(code = 200, message = "Successful Response"),
            @ApiResponse(code = 500, message = "Server Side Error"),
            @ApiResponse(code = 400, message = "Bad Request")})
    @PostMapping("/getcustmtblrecsbybatch")
    public void getCustmTblRecsByBatchName(@RequestBody @Valid GetCustomTableRecordsReqPo custmTableReqPo, HttpServletResponse response, HttpServletRequest request) throws Exception {
        PrintWriter writer = response.getWriter();
        try {

            crCustomTableService.getCustmTblRecsByBatchName(custmTableReqPo, response, writer, request);
        } catch (BadRequestException e) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            response.setContentType("text/plain");
            writer.println("Bad request: " + e.getMessage());
            writer.flush();
            log.error("Bad request: {}", e.getMessage(), e);
        } catch (Exception e) {
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            response.setContentType("text/plain");
            writer.println("Internal server error: " + e.getMessage());
            writer.flush();
            log.error("Internal server error: {}", e.getMessage(), e);
        }
    }

    @ApiOperation(value = "API provides distinct batch names by custom tableId")
    @ApiResponses(value = {@ApiResponse(code = 200, message = "Successful Response"),
            @ApiResponse(code = 500, message = "Server Side Error"),
            @ApiResponse(code = 400, message = "Bad Request")})
    @GetMapping("/getbatchnamesbycustomtblid")
    public ResponseEntity<BasicResponsePo> getBatchNamesByCustomTblId(@RequestParam("customTableId") Long customTableId) {
        return new ResponseEntity<BasicResponsePo>(crCustomTableService.getBatchNamesByCustomTblId(customTableId), HttpStatus.OK);
    }


}
