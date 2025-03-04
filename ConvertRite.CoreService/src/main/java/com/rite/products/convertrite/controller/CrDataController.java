package com.rite.products.convertrite.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rite.products.convertrite.exception.BadRequestException;
import com.rite.products.convertrite.exception.ConvertRiteException;
import com.rite.products.convertrite.model.CallBackReqPo;
import com.rite.products.convertrite.po.CrCloudRecordsReqPo;
import com.rite.products.convertrite.po.GetSourceRecords;
import com.rite.products.convertrite.service.CrDataService;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import java.io.IOException;
import javax.servlet.http.HttpServletResponse;
import javax.validation.Valid;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;
@Slf4j
@RequestMapping("/api/convertritecore/crdata")
@RestController
public class CrDataController {
    @Autowired
    CrDataService crDataService;

    @ApiOperation(value = "This api is for fetching distinct batch names")
    @ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
            @ApiResponse(code = 500, message = "Server Side Error"),
            @ApiResponse(code = 400, message = "Bad Request") })
    @GetMapping("/getbatchnames")
    public ResponseEntity<List<String>> getBatchNames(@RequestParam("cldTemplateId") Long templateId, HttpServletRequest request) throws Exception {
        log.info("Start of getBatchNames in controller #####");
        List<String> batchNamesLi = new ArrayList<>();
        try {
            batchNamesLi =  crDataService.getBatchNames(templateId,request);
        } catch (Exception e) {
            log.error(e.getMessage());
            throw new Exception(e.getMessage());
        }
        return new ResponseEntity<List<String>>(batchNamesLi, HttpStatus.OK);
    }

    @ApiOperation(value = "downloadRecords")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successful Response"),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 500, message = "Server Side Error")
    })
    @GetMapping("/downloadRecords")
    public ResponseEntity<InputStreamResource> downloadResults(
            @RequestParam(defaultValue = "1") int pageNo,@RequestParam(defaultValue = "150") int pageSize,
            @RequestParam Long srcTemplateId,@RequestParam String dataCriteria,
            @RequestParam String batchName,@RequestParam String returnType) {
        try {
            HttpHeaders httpHeaders = new HttpHeaders();
            Object result = crDataService.downloadResults(pageNo,pageSize, srcTemplateId, dataCriteria, batchName,returnType,httpHeaders);
            if (result instanceof File) {
                File file = (File) result;
                InputStreamResource resource = new InputStreamResource(new FileInputStream(file));

                return ResponseEntity.ok()
                        .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=" + file.getName())
                        .contentLength(file.length())
                        .contentType(MediaType.APPLICATION_OCTET_STREAM)
                        .headers(httpHeaders)
                        .body(resource);
            }
            else if (result instanceof String) {
                String jsonString = (String) result;
                return ResponseEntity.ok()
                        .contentLength(jsonString.getBytes().length)
                        .contentType(MediaType.APPLICATION_JSON)
                        .headers(httpHeaders)
                        .body(new InputStreamResource(new ByteArrayInputStream(jsonString.getBytes())));
            } else {
                throw new IllegalArgumentException("Invalid result type");
            }
        } catch (Exception e) {
            log.error(e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(null);
        }
    }

    @ApiOperation(value = "callback api post cloud load")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successful Response"),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 500, message = "Server Side Error")
    })
    @PostMapping("/callback")
    public void onLoadCallback(@RequestBody CallBackReqPo request, @RequestParam String tenantId) {
        log.info("request##########::::::::" + request);
        log.info("tenantId - "+tenantId);
        try {
            ObjectMapper Obj = new ObjectMapper();
            log.info("Recieved payload - "+ Obj.writeValueAsString(request));
            Long requestId = request.getHeader().getRelatesTo();
            log.info("requestId:::::::" + requestId);
            String jobStatus = request.getBody().getOnJobCompletion().getState();
            String resultMessage = request.getBody().getOnJobCompletion().getResultMessage();
            crDataService.onLoadCallback(requestId, jobStatus, resultMessage, tenantId);
        } catch (Exception e) {
            log.error(e.getMessage());
            e.printStackTrace();
        }
    }

    @PostMapping("/scheduledjob")
    public void onScheduledJob(HttpServletRequest request) {
        try {
            crDataService.onScheduledJob(request.getHeader("X-TENANT-ID"));
        } catch (Exception e) {
            log.error("Error in scheduledjob()---->"+e.getMessage());
        }
    }
    @ApiOperation(value = "This Api is for getting records post job execution,Status field value should be VF/VS/ALL,'type field value should be CSV/JSON")
    @ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
            @ApiResponse(code = 500, message = "Server Side Error") })
    @PostMapping("/getsourcerecords")
    public void getRecordsPostJobExecution(@RequestBody @Valid GetSourceRecords getSourceRecords,
                                           HttpServletResponse response, HttpServletRequest request) throws ConvertRiteException, IOException {
        log.info("Start of getRecordsPostJobExecution Method in Controller######");
        try {
            crDataService.getSourceRecords(getSourceRecords, response, request);
        } catch (BadRequestException e) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            response.setContentType("text/plain");
            response.getWriter().println("Bad request: " + e.getMessage());
            log.error("Bad request: {}", e.getMessage(), e);
        } catch (Exception e) {
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            response.setContentType("text/plain");
            response.getWriter().println("Internal server error: " + e.getMessage());
            log.error("Internal server error: {}", e.getMessage(), e);
        }
    }

    @ApiOperation(value = "This Api is for getting cloud staging records post job execution.responseType field value should be CSV/JSON")
    @ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
            @ApiResponse(code = 500, message = "Server Side Error") })
    @PostMapping("/getcloudstagingrecords")
    public void getCloudRecordsPostJobExecution(@RequestBody @Valid CrCloudRecordsReqPo crCloudRecordsReqPo,
                                                HttpServletResponse response, HttpServletRequest request) throws Exception {
        log.info("Start of getCloudRecordsPostJobExecution Method in Controller.");
        try {
            crDataService.getCloudStagingRecords(crCloudRecordsReqPo, response, request);
        } catch (BadRequestException e) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            response.setContentType("text/plain");
            response.getWriter().println("Bad request: " + e.getMessage());
            log.error("Bad request: {}", e.getMessage(), e);
        } catch (Exception e) {
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            response.setContentType("text/plain");
            response.getWriter().println("Internal server error: " + e.getMessage());
            log.error("Internal server error: {}", e.getMessage(), e);
        }
    }

    @PostMapping("/cloudjobstatuscheck")
    public void cloudJobStatusCheck(@RequestParam(name ="tenantid") String tenantId,
                                    @RequestParam(name ="requestid") Long requestId ,
                                    @RequestParam (name="jobid") Long jobID) {
        try {
            crDataService.cloudJobStatusCheck(tenantId,requestId,jobID);
        } catch (Exception e) {
            log.error("Error in scheduledjob()---->"+e.getMessage());
        }
    }

    @ApiOperation(value = "This api is for fetching paramlist")
    @ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
            @ApiResponse(code = 500, message = "Server Side Error"),
            @ApiResponse(code = 400, message = "Bad Request") })
    @GetMapping("/displayparamlist")
    public ResponseEntity<List<String>> getParamList(@RequestParam(name="objectid") Long objectid, HttpServletRequest request) throws Exception {
        log.info("Start of getparamlist in controller #####");
        List<String> paramLi = new ArrayList<>();
        try {
            paramLi =  crDataService.displayParamList(objectid,request);
        } catch (Exception e) {
            log.error(e.getMessage());
            throw new Exception(e.getMessage());
        }
        return new ResponseEntity<List<String>>(paramLi, HttpStatus.OK);
    }
}
