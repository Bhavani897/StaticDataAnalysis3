package com.rite.products.convertrite.controller;

import com.rite.products.convertrite.service.CrDashBoardService;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/convertritecore/dashboard")
@Slf4j
public class CrDashBoardController {

    @Autowired
    CrDashBoardService crDashBoardService;
    @ApiOperation(value = "This Api returns template statistics")
    @GetMapping("/gettemplatestatistics")
    public ResponseEntity<?> getTemplatesStatistics() {
        try {
            return new ResponseEntity<>(crDashBoardService.getTemplateStatistics(), HttpStatus.OK);
        } catch (Exception e) {
            log.error(e.getMessage());
            return new ResponseEntity<>(e.getCause(), HttpStatus.EXPECTATION_FAILED);
        }
    }
    @ApiOperation(value = "This Api returns template statistics")
    @GetMapping("/getCrTransformStats")
    public ResponseEntity<?> getCrTransformStats(@RequestParam(name = "cloudTemplateName", required = false) String cloudTemplateName, @RequestParam(name = "batchName", required = false) String batchName) {
        try {
            return new ResponseEntity<>(crDashBoardService.getCrTransformStats(cloudTemplateName, batchName), HttpStatus.OK);
        } catch (Exception e) {
            log.error(e.getMessage());
            return new ResponseEntity<>(e.getCause(), HttpStatus.EXPECTATION_FAILED);
        }
    }
}
