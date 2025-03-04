package com.rite.products.convertrite.controller;

import java.util.ArrayList;
import java.util.List;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Produces;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import com.rite.products.convertrite.Validations.Validations;
import com.rite.products.convertrite.exception.BadRequestException;
import com.rite.products.convertrite.exception.ConvertRiteException;
import com.rite.products.convertrite.exception.ValidationException;
import com.rite.products.convertrite.model.XxrErpIntegration;
import com.rite.products.convertrite.po.ErpIntegrationPageReqPo;
import com.rite.products.convertrite.po.LoadImportJobStatusResPo;
import com.rite.products.convertrite.po.LoadandImportDataReqPo;
import com.rite.products.convertrite.po.LoadandImportDataResPo;
import com.rite.products.convertrite.service.ErpIntegrationService;

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

@RestController
public class ErpIntegrationController {

    private static final Logger log = LoggerFactory.getLogger(ErpIntegrationController.class);

    @Autowired
    ErpIntegrationService erpIntegrationService;

    @ApiOperation(value = "This Api is to load and import data")
    @ApiResponses(value = {@ApiResponse(code = 200, message = "Successful Response"),
            @ApiResponse(code = 500, message = "Server Side Error"),
            @ApiResponse(code = 400, message = "Bad Request")})
    @PostMapping("/loadandimportdata")
    public ResponseEntity<LoadandImportDataResPo> loadAndImportData(
            @RequestBody LoadandImportDataReqPo loadandImportDataReqPo) {
        log.info("Start of loadAndImportData Method in Controller ###");

        LoadandImportDataResPo loadandImportDataResPo = new LoadandImportDataResPo();
        try {

            if (loadandImportDataReqPo.getCloudTemplateId() == null)
                throw new BadRequestException("cloudTemplateId is Mandatory field");

            loadandImportDataResPo = erpIntegrationService.loadAndImportData(loadandImportDataReqPo);
        } catch (ValidationException e) {
            log.error(e.getMessage());
            loadandImportDataResPo.setMessage(e.getMessage());
            loadandImportDataResPo.setError("Error while load or importing data");
            return new ResponseEntity<LoadandImportDataResPo>(loadandImportDataResPo, HttpStatus.OK);
        } catch (BadRequestException e) {
            log.error(e.getMessage());
            loadandImportDataResPo.setMessage(e.getMessage());
            loadandImportDataResPo.setError("Error while load or importing data");
            return new ResponseEntity<LoadandImportDataResPo>(loadandImportDataResPo, HttpStatus.BAD_REQUEST);
        } catch (Exception e) {
            log.error(e.getMessage());
            loadandImportDataResPo
                    .setMessage("Please contact System Administrator there is an error while processing the request");
            loadandImportDataResPo.setError(e.getMessage());
            return new ResponseEntity<LoadandImportDataResPo>(loadandImportDataResPo, HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return new ResponseEntity<LoadandImportDataResPo>(loadandImportDataResPo, new HttpHeaders(), HttpStatus.OK);
    }

    @ApiOperation(value = "This Api is to load and import data")
    @ApiResponses(value = {@ApiResponse(code = 200, message = "Successful Response"),
            @ApiResponse(code = 500, message = "Server Side Error"),
            @ApiResponse(code = 400, message = "Bad Request")})
    @PostMapping("/loadandimportdatav1")
    public synchronized ResponseEntity<LoadandImportDataResPo> loadAndImportDataV1(
            @RequestBody LoadandImportDataReqPo loadandImportDataReqPo, @RequestHeader("Authorization") String bearerToken) {
        log.info("Start of loadAndImportData Method in Controller ###");
        LoadandImportDataResPo loadandImportDataResPo = new LoadandImportDataResPo();
        try {

            if (loadandImportDataReqPo.getCloudTemplateId() == null
                    || Validations.isNullOrEmpty(loadandImportDataReqPo.getBatchName()))
                throw new BadRequestException("cloudTemplateId and batchName are Mandatory fields");

            //	loadandImportDataResPo = erpIntegrationService.loadAndImportDataV1(loadandImportDataReqPo,bearerToken);
            loadandImportDataResPo = erpIntegrationService.loadAndImportDataV2(loadandImportDataReqPo, bearerToken);

        } catch (Exception e) {
            log.error(e.getMessage());
            loadandImportDataResPo
                    .setMessage("Please contact System Administrator there is an error while processing the request");
            loadandImportDataResPo.setError(e.getMessage());
            return new ResponseEntity<LoadandImportDataResPo>(loadandImportDataResPo, HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return new ResponseEntity<LoadandImportDataResPo>(loadandImportDataResPo, new HttpHeaders(), HttpStatus.OK);
    }

    @ApiOperation(value = "This api is to know status of job submitted")
    @ApiResponses(value = {@ApiResponse(code = 200, message = "Successful Response"),
            @ApiResponse(code = 500, message = "Server Side Error"),
            @ApiResponse(code = 400, message = "Bad Request")})
    @GetMapping("/getjobstatus")
    public ResponseEntity<LoadImportJobStatusResPo> getJobStatus(@RequestParam("resultId") Long resultId,
                                                                 @RequestParam("cldTemplateId") Long cldTemplateId) throws ConvertRiteException {
        log.info("Start of getJobStatus Method in Controller ###");
        LoadImportJobStatusResPo loadImportJobStatusRes = new LoadImportJobStatusResPo();
        try {
            loadImportJobStatusRes = erpIntegrationService.getJobStatus(resultId, cldTemplateId);
        } catch (Exception e) {
            log.error(e.getMessage());
            loadImportJobStatusRes.setMessage("Error while Retriving job status");
            loadImportJobStatusRes.setError(e.getMessage());
            return new ResponseEntity<LoadImportJobStatusResPo>(loadImportJobStatusRes,
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return new ResponseEntity<LoadImportJobStatusResPo>(loadImportJobStatusRes, new HttpHeaders(), HttpStatus.OK);
    }

    @ApiOperation(value = "This api is to get erpintegration details")
    @ApiResponses(value = {@ApiResponse(code = 200, message = "Successful Response"),
            @ApiResponse(code = 500, message = "Server Side Error"),
            @ApiResponse(code = 400, message = "Bad Request")})
    @GetMapping("/geterpintegrationdetails")
    public ResponseEntity<List<XxrErpIntegration>> getErpIntegrationDetails(
            ErpIntegrationPageReqPo erpIntegrationPageReqPo) throws ConvertRiteException {
        log.info("Start of getErpIntegrationDetails Method in Controller ###");
        List<XxrErpIntegration> erpDetailsList = new ArrayList<>();
        HttpHeaders httpHeaders = new HttpHeaders();
        try {
            erpDetailsList = erpIntegrationService.getErpIntegrationDetails(erpIntegrationPageReqPo, httpHeaders);
        } catch (Exception e) {
            log.error(e.getMessage());
            throw new ConvertRiteException(
                    "Please contact System Administrator there is an error while processing the request",
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return new ResponseEntity<List<XxrErpIntegration>>(erpDetailsList, httpHeaders, HttpStatus.OK);
    }

    @ApiOperation(value = "This api is to download exported output in zip")
    @ApiResponses(value = {@ApiResponse(code = 200, message = "Successful Response"),
            @ApiResponse(code = 500, message = "Server Side Error"),
            @ApiResponse(code = 400, message = "Bad Request")})
    @GetMapping("/downloadexportoutput")
    @Produces("application/zip")
    public byte[] downloadExportOutput(@RequestParam("resultId") String resultId, @RequestParam("cldTemplateId") Long cldTemplateId, HttpServletResponse response)
            throws ConvertRiteException {
        log.info("Start of downloadExportOutput Method in Controller ###");
        byte[] resp = null;
        try {

            resp = erpIntegrationService.downloadExportOutput(resultId, cldTemplateId, response);
        } catch (Exception e) {
            log.error(e.getMessage());
            throw new ConvertRiteException(
                    "Please contact System Administrator there is an error while processing the request",
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return resp;
    }

}
