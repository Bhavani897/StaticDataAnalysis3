package com.rite.products.convertrite.controller;

import com.rite.products.convertrite.enums.Status;
import com.rite.products.convertrite.exception.BadRequestException;
import com.rite.products.convertrite.exception.ConvertRiteException;
import com.rite.products.convertrite.exception.ValidationException;
import com.rite.products.convertrite.model.CrFileDetails;
import com.rite.products.convertrite.model.XxrCloudDataProcess;
import com.rite.products.convertrite.po.CrCloudDataProcessResPo;
import com.rite.products.convertrite.po.CrHcmCloudImportStatusResPo;
import com.rite.products.convertrite.po.HcmLoadAndImportDataRes;
import com.rite.products.convertrite.po.HcmLoadandImportDataReqPo;
import com.rite.products.convertrite.respository.CrFileDetailsRepo;
import com.rite.products.convertrite.service.HcmDataImportService;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Objects;
import java.util.Optional;

@RestController
@RequestMapping(value = "/api/convertritecore/hcm")
@CrossOrigin
@Slf4j
public class CrHcmDataImportController {
    @Autowired
    HcmDataImportService hcmDataImportService;
    @Autowired
    CrFileDetailsRepo crFileDetailsRepo;
    @ApiOperation(value = "This Api is to load and import data")
    @ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
            @ApiResponse(code = 500, message = "Server Side Error"),
            @ApiResponse(code = 400, message = "Bad Request") })
    @PostMapping("/hcmloadandimportdata")
    public ResponseEntity<HcmLoadAndImportDataRes> hcmLoadAndImportData(
            @RequestBody HcmLoadandImportDataReqPo hcmLoadandImportDataReqPo, @RequestHeader("Authorization") String bearerToken, HttpServletResponse response) {
          HcmLoadAndImportDataRes loadandImportDataResPo = new HcmLoadAndImportDataRes();
        try {

           //  "cloudTemplateId,podId,projectId,parentObjectId,documentTitle,documentAccount,documentSecurityGroup,batchName and documentAuthor are Mandatory fields

            loadandImportDataResPo = hcmDataImportService.hcmLoadAndImportData(hcmLoadandImportDataReqPo,bearerToken,response);
        } catch (ValidationException e) {
            log.error(e.getMessage());
            loadandImportDataResPo.setMessage(e.getMessage());
            loadandImportDataResPo
                    .setError("Please contact System Administrator there is an error while processing the request");
            return new ResponseEntity<HcmLoadAndImportDataRes>(loadandImportDataResPo, HttpStatus.OK);
        } catch (BadRequestException e) {
            log.error(e.getMessage());
            loadandImportDataResPo.setMessage(e.getMessage());
            loadandImportDataResPo
                    .setError("Please contact System Administrator there is an error while processing the request");
            return new ResponseEntity<HcmLoadAndImportDataRes>(loadandImportDataResPo, HttpStatus.BAD_REQUEST);
        } catch (Exception e) {
            log.error(e.getMessage());
            loadandImportDataResPo
                    .setMessage("Please contact System Administrator there is an error while processing the request");
            loadandImportDataResPo.setError(e.getMessage());
            return new ResponseEntity<HcmLoadAndImportDataRes>(loadandImportDataResPo,
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return new ResponseEntity<HcmLoadAndImportDataRes>(loadandImportDataResPo, new HttpHeaders(), HttpStatus.OK);
    }
    @ApiOperation(value = "Api saves query for reconcile report")
    @ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
            @ApiResponse(code = 500, message = "Server Side Error"),
            @ApiResponse(code = 400, message = "Bad Request") })
    @PostMapping("/processhdlreconcile")
    public ResponseEntity<CrCloudDataProcessResPo> processHdlReconcile(@RequestParam("cldTemplateId") Long cldTemplateId,
                                                                       @RequestParam("batchName") String batchName,HttpServletRequest request) throws Exception {
        return new ResponseEntity<CrCloudDataProcessResPo>(hcmDataImportService.processHdlReconcile(cldTemplateId, batchName, request), HttpStatus.OK);
    }

    @ApiOperation(value = "Saves query of summary report")
    @ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
            @ApiResponse(code = 500, message = "Server Side Error"),
            @ApiResponse(code = 400, message = "Bad Request") })
    @PostMapping("/processhdlsummary")
    public ResponseEntity<CrCloudDataProcessResPo> processHdlSummary(@RequestParam("batchName") String batchName,@RequestParam("cldTemplateId") Long cldTemplateId, HttpServletRequest request) throws Exception {
        return new ResponseEntity<CrCloudDataProcessResPo>(hcmDataImportService.processHdlSummary(batchName,cldTemplateId, request), HttpStatus.OK);
    }

    @ApiOperation(value = "api generates report")
    @ApiResponses(value = {@ApiResponse(code = 200, message = "Successful Response"),
            @ApiResponse(code = 500, message = "Server Side Error")})
    @GetMapping("/hdlreport")
    public void hdlReport(@RequestParam("statusId") Long statusId, @RequestParam("id") Long id, @RequestParam("reportType") String reportType, HttpServletResponse response) throws ValidationException, Exception {
        hcmDataImportService.hdlReport(statusId, id,reportType, response);
    }

    @ApiOperation(value = "API hcm cloud import summary")
    @ApiResponses(value = {@ApiResponse(code = 200, message = "Successful Response"),
            @ApiResponse(code = 500, message = "Server Side Error")})
    @GetMapping("/hcmcldimportsummary")
    public ResponseEntity<CrHcmCloudImportStatusResPo> hcmCldImportSummary(@RequestParam("statusId") Long statusId, @RequestParam("id") Long id) throws ValidationException, Exception {
       return new ResponseEntity<CrHcmCloudImportStatusResPo>(hcmDataImportService.hcmCldImportSummary(statusId, id),HttpStatus.OK);
    }


}
