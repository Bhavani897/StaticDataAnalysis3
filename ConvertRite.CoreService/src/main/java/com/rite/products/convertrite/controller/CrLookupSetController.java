package com.rite.products.convertrite.controller;

import java.util.ArrayList;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import com.rite.products.convertrite.po.CrLookUpSetsCreatePo;
import com.rite.products.convertrite.po.CrLookupSetValueCreateReqPo;
import com.rite.products.convertrite.service.CrLookUpSetsService;
import com.rite.products.convertrite.service.CrLookUpValuesService;

import io.swagger.annotations.ApiOperation;
import org.springframework.web.multipart.MultipartFile;

@RestController
@RequestMapping("/api/convertritecore")
@Slf4j
public class CrLookupSetController {

    @Autowired
    CrLookUpSetsService crLookUpSetsService;

    @Autowired
    CrLookUpValuesService crLookUpValuesService;

    /*
     * Lookup Set Header APIs
     */
    @ApiOperation(value = "API to get LookupSet By Id")
    @GetMapping("/getLookupSetById/{lookUpSetId}")
    public ResponseEntity<?> findById(@PathVariable Long lookUpSetId) {
        try {
            return new ResponseEntity<>(crLookUpSetsService.getLookupSetById(lookUpSetId), HttpStatus.OK);
        } catch (Exception e) {
            log.error(e.getMessage());
            return new ResponseEntity<>(e.getCause(), HttpStatus.EXPECTATION_FAILED);
        }

    }

    @ApiOperation(value = "API to get All LookupSets")
    @GetMapping("/getAllLookUpSets")
    public ResponseEntity<?> getAllLookUpSets() {
        try {
            return new ResponseEntity<>(crLookUpSetsService.getAllLookupSets(), HttpStatus.OK);
        } catch (Exception e) {
            log.error(e.getMessage());
            return new ResponseEntity<>(e.getCause(), HttpStatus.EXPECTATION_FAILED);
        }

    }

    @ApiOperation(value = "API to get Save LookupSet")
    @PostMapping("/saveLookUpSet")
    public ResponseEntity<?> saveLookupSet(@RequestBody CrLookUpSetsCreatePo crLookUpSetsCreatePo) {
        try {
            return new ResponseEntity<>(crLookUpSetsService.saveLookupSet(crLookUpSetsCreatePo), HttpStatus.OK);
        } catch (Exception e) {
            log.error(e.getMessage());
            return new ResponseEntity<>(e.getCause(), HttpStatus.EXPECTATION_FAILED);
        }

    }

    /*
     * Lookup Set Values API
     */
    @ApiOperation(value = "API to get LookUp Set Values by Set ID")
    @GetMapping("/getLookUpValuesBySetId/{LookUpSetId}")
    public ResponseEntity<?> getMappingSetValuesById(@PathVariable Long LookUpSetId) {
        try {
            return new ResponseEntity<>(crLookUpValuesService.getAllLookupSetValues(LookUpSetId), HttpStatus.OK);
        } catch (Exception e) {
            log.error(e.getMessage());
            return new ResponseEntity<>(e.getCause(), HttpStatus.EXPECTATION_FAILED);
        }
    }

    @ApiOperation(value = "API to delete LookUp Set by LookupSet ID")
    @DeleteMapping("/deleteLookupSet")
    public ResponseEntity<?> deleteLookupSet(@RequestParam Long lookUpSetId) {
        return new ResponseEntity<>(crLookUpValuesService.deleteLookupSet(lookUpSetId), HttpStatus.OK);
    }

    @ApiOperation(value = "API to delete LookUp Value by Value ID")
    @DeleteMapping("/deleteLookupValue")
    public ResponseEntity<?> deleteLookupValue(@RequestParam Long lookUpValueId) {
        return new ResponseEntity<>(crLookUpValuesService.deleteLookupValueById(lookUpValueId), HttpStatus.OK);
    }

    @ApiOperation(value = "API to save all LookUp Values in a given Mapping Set")
    @PostMapping("/saveallLookUpvalues/{LookUpSetId}")
    public ResponseEntity<?> saveAllMappingValues(@RequestBody ArrayList<CrLookupSetValueCreateReqPo> valueList,
                                                  @PathVariable Long LookUpSetId) {
        try {
            return new ResponseEntity<>(crLookUpValuesService.saveAllLookupSet(valueList, LookUpSetId),
                    HttpStatus.OK);
        } catch (Exception e) {
            log.error(e.getMessage());
            return new ResponseEntity<>(e.getCause(), HttpStatus.EXPECTATION_FAILED);
        }
    }

    @ApiOperation(value = "API to save all LookUp Values in a given LookUp Set")
    @PostMapping("/saveSingleLookUpSetValue")
    public ResponseEntity<?> saveSingleLookUpValue(@RequestBody CrLookupSetValueCreateReqPo LookUpValueReqPo) {
        try {
            return new ResponseEntity<>(crLookUpValuesService.SaveLookupValue(LookUpValueReqPo),
                    HttpStatus.OK);
        } catch (Exception e) {
            log.error(e.getMessage());
            return new ResponseEntity<>(e.getCause(), HttpStatus.EXPECTATION_FAILED);
        }
    }

    @PostMapping("/loadLookUpfromfile")
    public ResponseEntity<?> loadLookupFromFile(@RequestParam("file") MultipartFile file,
                                                @RequestParam("lookupSetId") Long lookupSetId) {
        try {
            return new ResponseEntity<>(crLookUpValuesService.loadLookupFromFile(file, lookupSetId), HttpStatus.OK);
        } catch (Exception e) {
            log.error(e.getMessage());
            return new ResponseEntity<>(e.getCause(), HttpStatus.EXPECTATION_FAILED);
        }
    }

}
