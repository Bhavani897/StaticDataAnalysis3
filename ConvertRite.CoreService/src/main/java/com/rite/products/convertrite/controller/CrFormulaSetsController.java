package com.rite.products.convertrite.controller;

import java.util.List;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import com.rite.products.convertrite.model.CrFormulaSets;
import com.rite.products.convertrite.po.CrFormulaSetsCreateReqPo;
import com.rite.products.convertrite.service.CrFormulaSetsService;

import io.swagger.annotations.ApiOperation;

@RestController
@RequestMapping("/api/convertritecore")
@Slf4j
public class CrFormulaSetsController {
    @Autowired
    CrFormulaSetsService crFormulaSetsService;

    @ApiOperation(value = "API to get all Formula sets")
    @GetMapping("/getAllFormulaSets")
    public ResponseEntity<?> getAllFormulaSets() {
        try {
            return new ResponseEntity<>(crFormulaSetsService.getAllFormulaSets(), HttpStatus.OK);
        } catch (Exception e) {
            log.error(e.getMessage());
            return new ResponseEntity<>(e.getCause(), HttpStatus.EXPECTATION_FAILED);
        }
    }

    @ApiOperation(value = "API to get Formula sets by Id")
    @GetMapping("/getFormulaSetById/{formulaSetId}")
    public ResponseEntity<?> getFormulaSetById(@PathVariable Long formulaSetId) {
        try {
            return new ResponseEntity<>(crFormulaSetsService.getFormulaSetById(formulaSetId), HttpStatus.OK);
        } catch (Exception e) {
            log.error(e.getMessage());
            return new ResponseEntity<>(e.getCause(), HttpStatus.EXPECTATION_FAILED);
        }
    }

    @ApiOperation(value = "API to Save/Update Formula Set")
    @PostMapping("/saveFormulaSet")
    public ResponseEntity<?> saveFormulaSet(@RequestBody CrFormulaSetsCreateReqPo createReqPo) {
        try {
            return new ResponseEntity<>(crFormulaSetsService.saveFormulaSet(createReqPo), HttpStatus.OK);
        } catch (Exception e) {
            log.error(e.getMessage());
            return new ResponseEntity<>(e.getCause(), HttpStatus.EXPECTATION_FAILED);
        }
    }

    @ApiOperation(value = "API to Delete Formula Set")
    @DeleteMapping("/deleteFormulaSet")
    public ResponseEntity<?> deleteFormulaSet(@RequestParam String mappingType, @RequestParam Long formulaSetId) {
        return new ResponseEntity<>(crFormulaSetsService.deleteFormulaSetById(mappingType, formulaSetId), HttpStatus.OK);
    }
}