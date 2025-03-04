package com.rite.products.convertrite.controller;

import java.util.ArrayList;
import java.util.List;

import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import com.rite.products.convertrite.model.CrMappingSetHdr;
import com.rite.products.convertrite.po.CrMappingSetHdrCreateReqPo;
import com.rite.products.convertrite.po.CrMappingSetValuesCreateReqPo;
//import com.rite.products.convertrite.po.CrMappingSetValuesCreateReqPoWrapper;
import com.rite.products.convertrite.service.CrMappingSetHdrService;
import com.rite.products.convertrite.service.CrMappingSetValuesService;

import io.swagger.annotations.ApiOperation;
import org.springframework.web.multipart.MultipartFile;

@RestController
@RequestMapping("/api/convertritecore")
@Slf4j
public class CrMappingSetController {

    @Autowired
    CrMappingSetHdrService crMappingSetHdrService;

    @Autowired
    CrMappingSetValuesService crMappingSetValuesService;

    /*
     * Mapping Header APIs
     */

    @ApiOperation(value = "API to get all Mapping sets")
    @GetMapping("/getAllMappingSets")
    public ResponseEntity<?> getAllMappingsets() {
        try {
            return new ResponseEntity<>(crMappingSetHdrService.getAllMappingsets(), HttpStatus.OK);
        } catch (Exception e) {
            log.error(e.getMessage());
            return new ResponseEntity<>(e.getCause(), HttpStatus.EXPECTATION_FAILED);
        }
    }


    @ApiOperation(value = "API to Save Mapping Set Header")
    @PostMapping("/SaveMappingSetHeader")
    public ResponseEntity<?> saveMappingSetHdr(@RequestBody CrMappingSetHdrCreateReqPo crMSetHdrCreateReqPo) {
        try {
            return new ResponseEntity<>(crMappingSetHdrService.saveMappingSetHdr(crMSetHdrCreateReqPo), HttpStatus.OK);
        } catch (Exception e) {
            log.error(e.getMessage());
            return new ResponseEntity<>(e.getCause(), HttpStatus.EXPECTATION_FAILED);
        }

    }

    @ApiOperation(value = "API to get Mapping Set by ID")
    @GetMapping("/getMappingSetById/{mapSetId}")
    public ResponseEntity<?> getMappingSetByID(@PathVariable int mapSetId) {
        try {
            return new ResponseEntity<>(crMappingSetHdrService.findById(mapSetId), HttpStatus.OK);
        } catch (Exception e) {
            log.error(e.getMessage());
            return new ResponseEntity<>(e.getCause(), HttpStatus.EXPECTATION_FAILED);
        }
    }

    @ApiOperation(value = "API to get Mapping Set by Code")
    @GetMapping("/getMappingSetByCode/{mapSetCode}")
    public ResponseEntity<?> getMappingSetByID(@PathVariable String mapSetCode) {
        try {
            return new ResponseEntity<>(crMappingSetHdrService.findByMapCode(mapSetCode), HttpStatus.OK);
        } catch (Exception e) {
            log.error(e.getMessage());
            return new ResponseEntity<>(e.getCause(), HttpStatus.EXPECTATION_FAILED);
        }
    }
    @ApiOperation(value = "API to delete Mapping Set ")
    @DeleteMapping("/deleteMappingSet")
    public ResponseEntity<?> deleteMappingSetById(@RequestParam String mappingType, @RequestParam Long mapSetId) {
        return new ResponseEntity<>(crMappingSetValuesService.deleteAllMappingValuesBySetId(mappingType, mapSetId), HttpStatus.OK);
    }

    /*
     * Mapping Values APIs
     */

    @ApiOperation(value = "API to get Mapping Set Values by Set ID")
    @GetMapping("/getMappingValuesBySetId/{mapSetId}")
    public ResponseEntity<?> getMappingSetValuesById(@PathVariable Long mapSetId) {
        try {
            return new ResponseEntity<>(crMappingSetValuesService.getValuesBySetId(mapSetId), HttpStatus.OK);
        } catch (Exception e) {
            log.error(e.getMessage());
            return new ResponseEntity<>(e.getCause(), HttpStatus.EXPECTATION_FAILED);
        }
    }

    @ApiOperation(value = "API to delete Mapping Set Value by Value ID")
    @DeleteMapping("/deleteMappingValuesByValueId/{valueId}")
    public ResponseEntity<?> deleteByMappingValueId(@PathVariable int valueId) {
        try {
            return new ResponseEntity<>(crMappingSetValuesService.deleteByValueId(valueId), HttpStatus.OK);
        } catch (Exception e) {
            log.error(e.getMessage());
            return new ResponseEntity<>(e.getCause(), HttpStatus.EXPECTATION_FAILED);
        }
    }

    @ApiOperation(value = "API to save all Mapping Values in a given Mapping Set")
    @PostMapping("/saveallmappingvalues/{mapSetId}")
    public ResponseEntity<?> saveAllMappingValues(@RequestBody ArrayList<CrMappingSetValuesCreateReqPo> valueList,
                                                  @PathVariable Long mapSetId) {
        try {
            return new ResponseEntity<>(crMappingSetValuesService.saveAllMappingValues(valueList, mapSetId),
                    HttpStatus.OK);
        } catch (Exception e) {
            log.error(e.getMessage());
            return new ResponseEntity<>(e.getCause(), HttpStatus.EXPECTATION_FAILED);
        }
    }

    @ApiOperation(value = "API to save all Mapping Values in a given Mapping Set")
    @PostMapping("/saveSingleMapingSetValue")
    public ResponseEntity<?> saveSingleMappingValue(@RequestBody CrMappingSetValuesCreateReqPo mappingValueReqPo) {
        try {
            return new ResponseEntity<>(crMappingSetValuesService.saveMappingSetValues(mappingValueReqPo),
                    HttpStatus.OK);
        } catch (Exception e) {
            log.error(e.getMessage());
            return new ResponseEntity<>(e.getCause(), HttpStatus.EXPECTATION_FAILED);
        }
    }


	@ApiOperation(value = "This Api is to load mapping values from a csv file")
    @ApiResponses(value = {@ApiResponse(code = 200, message = "Successful Response"),
            @ApiResponse(code = 500, message = "Server Side Error")})
    @PostMapping("/loadmappingvalues")
    public ResponseEntity<?> loadMappingValues(@RequestParam("file") MultipartFile file,
                                               @RequestParam("mappingSetId") Long mappingSetId) {
        try {
            return new ResponseEntity<>(crMappingSetValuesService.loadMappingValues(file, mappingSetId), HttpStatus.OK);
        } catch (Exception e) {
            log.error(e.getMessage());
            return new ResponseEntity<>(e.getCause(), HttpStatus.EXPECTATION_FAILED);
        }
    }
}
