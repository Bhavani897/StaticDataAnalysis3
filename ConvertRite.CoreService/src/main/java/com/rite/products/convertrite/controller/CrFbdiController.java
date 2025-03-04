package com.rite.products.convertrite.controller;

import com.rite.products.convertrite.Validations.Validations;
import com.rite.products.convertrite.exception.BadRequestException;
import com.rite.products.convertrite.exception.ConvertRiteException;
import com.rite.products.convertrite.model.CrFbdiCols;
import com.rite.products.convertrite.model.CrFbdiHdrs;
import com.rite.products.convertrite.po.*;
import com.rite.products.convertrite.service.CrFbdiService;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.ArrayList;
import java.util.List;

@RestController
@Slf4j
@RequestMapping("/api/convertritecore/fbdi")
public class CrFbdiController {
    @Autowired
    CrFbdiService crFbdiService;

    @ApiOperation(value = "This Api to save fbdi template header")
    @ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
            @ApiResponse(code = 500, message = "Server Side Error") })
    @PostMapping("/savefbditemphdrs")
    public ResponseEntity<SaveFbdiHdrsPo> saveFbdiTemplateHdrs(
            @RequestBody SaveFbdiTempHdrsPo fbdiTemplateHeaderPo, HttpServletRequest request) {
        log.info("Start of saveFbdiTemplateHdrs Method in Controller ###");
        SaveFbdiHdrsPo saveFbdiHdrsPo = new SaveFbdiHdrsPo();
        try {

            if ( fbdiTemplateHeaderPo.getObjectId() == null
                    || Validations.isNullOrEmpty(fbdiTemplateHeaderPo.getFbdiTemplateName())
                    || fbdiTemplateHeaderPo.getParentObjectId() == null
                    || fbdiTemplateHeaderPo.getProjectId() == null)
                throw new BadRequestException(
                        "objectId,projectId,parentOjectId and fbdiTemplateName are Mandatory fields");

            saveFbdiHdrsPo = crFbdiService.saveFbdiTemplateHdrs(fbdiTemplateHeaderPo, request);

        }  catch (BadRequestException e) {
            log.error(e.getMessage() + "BadRequestException");
            saveFbdiHdrsPo.setMessage("Error while saving Fbdi template");
            saveFbdiHdrsPo.setError(e.getMessage());
            return new ResponseEntity<SaveFbdiHdrsPo>(saveFbdiHdrsPo,
                    HttpStatus.BAD_REQUEST);
        } catch (Exception e) {
            log.error(e.getMessage() + "Exception");
            saveFbdiHdrsPo.setMessage("Error while saving Fbdi template");
            saveFbdiHdrsPo.setError(e.getMessage());
            return new ResponseEntity<SaveFbdiHdrsPo>(saveFbdiHdrsPo,
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }

        return new ResponseEntity<SaveFbdiHdrsPo>(saveFbdiHdrsPo, new HttpHeaders(),
                HttpStatus.OK);

    }
    @ApiOperation(value = "This Api returns fbdi column sequence")
    @ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
            @ApiResponse(code = 500, message = "Server Side Error") })
    @GetMapping("/getfbdicolsequence")
    public ResponseEntity<?> getFbdiColumnSequence(@RequestParam("fileName") String fileName,
                                                                            @RequestParam("version") String version, HttpServletResponse response) throws ConvertRiteException {
        log.info("Start of getFbdiColumnSequence Method in Controller ###");
        List<FbdiColumnSequencePo> columnSequence = new ArrayList<>();
        try {
            columnSequence = crFbdiService.getFbdiColumnSequence(fileName, version, response);
        } catch (Exception e) {
            log.error(e.getMessage());
            return new ResponseEntity<>(e.getCause(), HttpStatus.EXPECTATION_FAILED);
        }
        return new ResponseEntity<List<FbdiColumnSequencePo>>(columnSequence, new HttpHeaders(), HttpStatus.OK);

    }

    @ApiOperation(value = "This Api is to save fbditemplate columns")
    @ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
            @ApiResponse(code = 500, message = "Server Side Error"),
            @ApiResponse(code = 400, message = "Bad Request") })
    @PostMapping("/savefbditempcols")
    public ResponseEntity<?> saveFbdiTemplateColumns(
            @RequestBody List<SaveFbdiTemplateColumnsPo> fbdiTemplateColumnsPo, HttpServletRequest request) {
        log.info("Start of saveFbdiTemplateColumns Method in Controller ###");
        CrSaveFbdiTempColsResPo response = new CrSaveFbdiTempColsResPo();
        try {
            response = crFbdiService.saveFbdiTemplateColumns(fbdiTemplateColumnsPo, request);

        }  catch (Exception e) {
            log.error(e.getMessage());
            return new ResponseEntity<>(e.getMessage(),
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return new ResponseEntity<CrSaveFbdiTempColsResPo>(response, new HttpHeaders(),
                HttpStatus.OK);

    }

    @ApiOperation(value = "This Api returns all fbdi templates")
    @ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
            @ApiResponse(code = 500, message = "Server Side Error") })
    @GetMapping("/getfbditemplates")
    public ResponseEntity<List<CrFbdiHdrs>> getFbdiTemplates()
            throws ConvertRiteException {
        log.info("Start of getFbdiTemplates Method in Controller ###");
        List<CrFbdiHdrs> fbdiTempHdrsResp = new ArrayList<>();
        try {
            fbdiTempHdrsResp = crFbdiService.getFbdiTemplates();
        } catch (Exception e) {
            log.error(e.getMessage());
            throw new ConvertRiteException(
                   e.getMessage(),
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return new ResponseEntity<List<CrFbdiHdrs>>(fbdiTempHdrsResp, new HttpHeaders(), HttpStatus.OK);

    }
    
    @ApiOperation(value = "This Api returns all fbdi templates With Names")
    @ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
            @ApiResponse(code = 500, message = "Server Side Error") })
    @GetMapping("/getAllfbditemplates")
    public ResponseEntity<?> getAllFbdiTemplates() {
    	try {
    		return new ResponseEntity<>(crFbdiService.getAllFbdITemplates(),HttpStatus.OK);
    	} catch(Exception e) {
    		log.error(e.getMessage());
    		return new ResponseEntity<>(e.getCause(),HttpStatus.EXPECTATION_FAILED);
    	}
        
    }

    @ApiOperation(value = "This Api returns fbdi columns based on FbdiTemplateId")
    @ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
            @ApiResponse(code = 500, message = "Server Side Error") })
    @GetMapping("/getfbditempcols")
    public ResponseEntity<List<CrFbdiCols>> getFbdiTemplateColumns(
            @RequestParam("fbdiTemplateId") Long fbdiTemplateId) throws ConvertRiteException {
        log.info("Start of getFbdiTemplateColumns Method in Controller ###");
        List<CrFbdiCols> fbdiTempColsResp = new ArrayList<>();
        try {
            fbdiTempColsResp = crFbdiService.getFbdiTemplateColumns(fbdiTemplateId);
        } catch (Exception e) {
            log.error(e.getMessage());
            throw new ConvertRiteException(
                    e.getMessage(),
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return new ResponseEntity<List<CrFbdiCols>>(fbdiTempColsResp, new HttpHeaders(), HttpStatus.OK);

    }
    @PostMapping("/uploadctlgetcolumnseq")
    public ResponseEntity<?> uploadCtlGetColumnSeq(@RequestParam("file") MultipartFile file,
                                                                            @RequestParam("version") String version, @RequestParam("objectId") Long objectId)
            throws ConvertRiteException {
        log.info("Start of uploadCtlGetColumnSeq Method in Controller ###");
        List<FbdiColumnSequencePo> columnSequence = new ArrayList<>();
        try {

            columnSequence = crFbdiService.uploadCtlGetColumnSeq(file, version, objectId);

        } catch (Exception e) {
            log.error(e.getMessage());
            return new ResponseEntity<>(e.getCause(), HttpStatus.BAD_REQUEST);
        }
        return new ResponseEntity<List<FbdiColumnSequencePo>>(columnSequence, new HttpHeaders(), HttpStatus.OK);
    }
}
