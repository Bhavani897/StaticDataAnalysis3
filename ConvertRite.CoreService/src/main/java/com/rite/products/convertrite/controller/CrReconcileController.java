package com.rite.products.convertrite.controller;

import com.rite.products.convertrite.exception.ConvertRiteException;
import com.rite.products.convertrite.service.CrConversionService;
import com.rite.products.convertrite.service.CrReconcileService;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletResponse;
import java.io.PrintWriter;
import java.util.Objects;

@RestController
@Slf4j
@RequestMapping("/api/convertritecore/reconcile")
public class CrReconcileController {
    @Autowired
    CrReconcileService crReconcileService;


    @ApiOperation(value = "This Api is for downloading fbdi from ftp")
    @GetMapping("/downloadCloudImportSuccessRec")
    public void downloadCloudImportSuccessRec(@RequestParam("cloudTemplateId") Long cloudTemplateId, @RequestParam("batchName") String batchName,HttpServletResponse response)
            throws ConvertRiteException, Exception {
        Object res = null;
        try {
            if (batchName.toString().isEmpty() || cloudTemplateId == null || batchName == null || batchName.equals(null) || batchName.equals("null") ) {
                PrintWriter writer = response.getWriter();
                writer.write("CloudTemplateId ,BatchName and UserId Should Not be Null or Empty");
                response.setStatus(400);
            } else {
                res = crReconcileService.downloadCloudImportSuccessRec(cloudTemplateId, batchName,  response);
            }
        } catch (Exception e) {
            log.error(e.getMessage());
            throw new Exception(e.getMessage());
        }
        //return new ResponseEntity<>(res, HttpStatus.OK);
    }

    @ApiOperation(value = "This Api is for downloading fbdi from ftp")
    @GetMapping("/downloadCloudImportRejRec")
    public void downloadCloudImportRejRec(@RequestParam("cloudTemplateId") Long cloudTemplateId, @RequestParam("batchName") String batchName, HttpServletResponse response)
            throws Exception {
        Object res = null;
        try {
            if (batchName.toString().isEmpty() || cloudTemplateId == null || batchName == null || batchName.equals(null) || batchName.equals("null")) {
                PrintWriter writer = response.getWriter();
                writer.write("CloudTemplateId ,BatchName and UserId Should Not be Null or Empty");
                response.setStatus(400);
            } else {
                //   res = crReconcileService.downloadCloudImportRejRec(cloudTemplateId, batchName, userId, response);
                res = crReconcileService.downloadValFailRec(cloudTemplateId, batchName, response);
            }
        } catch (Exception e) {
            log.error(e.getMessage());
            // return new ResponseEntity<>(e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
            throw new Exception(e.getMessage());
        }
        // return new ResponseEntity<>(res, HttpStatus.OK);
    }

    @ApiOperation(value = "This Api is for downloading fbdi from ftp")
    @GetMapping("/downloadValFailRec")
    public void downloadValFailRec(@RequestParam("cloudTemplateId") Long cloudTemplateId, @RequestParam("batchName") String batchName, @RequestHeader("userId") String userId, HttpServletResponse response)
            throws Exception {
        Object res = null;
        try {
            log.debug(cloudTemplateId + "-" + batchName + "-" + userId);
            if (batchName.toString().isEmpty() || cloudTemplateId == null || batchName == null || batchName.equals(null) || batchName.equals("null")) {
                PrintWriter writer = response.getWriter();
                writer.write("CloudTemplateId ,BatchName and UserId Should Not be Null or Empty");
                response.setStatus(400);
            } else {
                res = crReconcileService.downloadCloudImportRejRec(cloudTemplateId, batchName, userId, response);
            }
        } catch (Exception e) {
            log.error(e.getMessage());
            throw new Exception(e.getMessage());
        }
        //  return new ResponseEntity<>(res, HttpStatus.OK);
    }
}
