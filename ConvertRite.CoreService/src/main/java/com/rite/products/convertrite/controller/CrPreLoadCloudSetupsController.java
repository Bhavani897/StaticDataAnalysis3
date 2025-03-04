package com.rite.products.convertrite.controller;

import com.rite.products.convertrite.exception.BadRequestException;
import com.rite.products.convertrite.model.AsyncProcessStatus;
import com.rite.products.convertrite.model.CrPreLoadCloudSetUpsReqPo;
import com.rite.products.convertrite.po.BasicResponsePo;
import com.rite.products.convertrite.po.ProcessValidationObjectsReqPo;
import com.rite.products.convertrite.service.AsyncProcessStatusService;
import com.rite.products.convertrite.service.CrPreLoadCloudTemplatesService;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;

@RestController
@RequestMapping("/api/convertritecore/preloadcloudsetups")
@Slf4j
public class CrPreLoadCloudSetupsController {

    @Autowired
    CrPreLoadCloudTemplatesService crPreLoadCloudTemplatesService;
    @Autowired
    AsyncProcessStatusService asyncProcessStatusService;


    @ApiOperation(value = "Api PreLoads metadata,cldtemplates and creates cldstaging table")
    @ApiResponses(value = {@ApiResponse(code = 200, message = "Successful Response"),
            @ApiResponse(code = 500, message = "Server Side Error")})
    @PostMapping("/preloadcloudsetups")
    public void preLoadCloudSetUps(@RequestBody CrPreLoadCloudSetUpsReqPo preLoadCloudSetUpsReqPo, HttpServletRequest request) throws Exception {
        AsyncProcessStatus asyncProcessStatus = asyncProcessStatusService.startProcess("PreLoad-Cloud-Setups",null,null, request.getHeader("userId"));
        if (preLoadCloudSetUpsReqPo.getProjectId() == null || preLoadCloudSetUpsReqPo.getObjectIdLi() == null || preLoadCloudSetUpsReqPo.getObjectIdLi().isEmpty()) {
            asyncProcessStatusService
                    .endProcess(asyncProcessStatus.getAsyncProcessId(), null,null,HttpStatus.BAD_REQUEST.toString(), "ObjectIds and projectId must be provided", request.getHeader("userId"), null);
            throw new BadRequestException("ObjectId's and projectId must be provided");
        }
        crPreLoadCloudTemplatesService.preLoadCloudSetUps(preLoadCloudSetUpsReqPo, request, asyncProcessStatus.getAsyncProcessId());
    }

    @ApiOperation(value = " Api process validaton objects")
    @ApiResponses(value = {@ApiResponse(code = 200, message = "Successful Response"),
            @ApiResponse(code = 500, message = "Server Side Error")})
    @PostMapping("/processValidationObjects")
    public void processValidationObjects(@RequestBody ProcessValidationObjectsReqPo processValidationObjectReq,HttpServletRequest request) throws Exception {
        AsyncProcessStatus asyncProcessStatus = asyncProcessStatusService.startProcess("Process-Validation-API",null,null, request.getHeader("userId"));
        if (processValidationObjectReq.getProjectId() == null || processValidationObjectReq.getObjectIdsLi() == null || processValidationObjectReq.getObjectIdsLi().isEmpty()) {
            asyncProcessStatusService
                    .endProcess(asyncProcessStatus.getAsyncProcessId(), null,null,HttpStatus.BAD_REQUEST.toString(), "ObjectIds and projectId must be provided", request.getHeader("userId"),null);
            throw new BadRequestException("ObjectId's and projectId must be provided");
        }
        crPreLoadCloudTemplatesService.processValidationObjects(processValidationObjectReq,request,asyncProcessStatus.getAsyncProcessId());
    }

    @ApiOperation(value = "Get All Cloud Setup details")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successful Response"),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 500, message = "Server Side Error")
    })
    @GetMapping("/getPreloadSetupDtls")
    public ResponseEntity<?> getPreloadSetupDtls(@RequestParam("projectId") Long projectId) {
        try {
            BasicResponsePo basicResponsePo = crPreLoadCloudTemplatesService.getPreloadSetupDtls(projectId);
            return ResponseEntity.ok(basicResponsePo);
        } catch (Exception e) {
            log.error(e.getMessage());
            return new ResponseEntity<>(e.getCause(), HttpStatus.EXPECTATION_FAILED);
        }
    }

    @ApiOperation(value = "Get All Validation Setup details")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successful Response"),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 500, message = "Server Side Error")
    })
    @GetMapping("/getPreloadValidationSetupDtls")
    public ResponseEntity<?> getPreloadValidationSetupDtls(@RequestParam("projectId") Long projectId) {
        try {
            BasicResponsePo basicResponsePo = crPreLoadCloudTemplatesService.getPreloadValidationSetupDtls(projectId);
            return ResponseEntity.ok(basicResponsePo);
        } catch (Exception e) {
            log.error(e.getMessage());
            return new ResponseEntity<>(e.getCause(), HttpStatus.EXPECTATION_FAILED);
        }
    }
}
