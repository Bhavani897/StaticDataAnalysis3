package com.rite.products.convertrite.controller;


import com.rite.products.convertrite.po.LoadMetaDataFromCloudRes;
import com.rite.products.convertrite.po.RoleObjectLinkResPo;
import com.rite.products.convertrite.po.SaveCloudAtomationRes;
import com.rite.products.convertrite.po.SaveTempResPo;
import com.rite.products.convertrite.service.ErpService;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletRequest;

@RestController
public class ErpController {

    private static final Logger log = LoggerFactory.getLogger(ErpController.class);
    @Autowired
    ErpService erpService;

	/*
	 * @ApiOperation(value =
	 * "This Api is to save Cloud config,lookup values,fbdi seq generation")
	 * 
	 * @ApiResponses(value = {@ApiResponse(code = 200, message =
	 * "Successful Response"),
	 * 
	 * @ApiResponse(code = 500, message = "Server Side Error")})
	 * 
	 * @PostMapping("/uptofbdisequence") public
	 * ResponseEntity<SaveCloudAtomationRes> saveCloudConfig(@RequestParam("file")
	 * MultipartFile file,
	 * 
	 * @RequestParam("podId") Long podId,
	 * 
	 * @RequestParam("projectId") Long projectId,
	 * 
	 * @RequestParam("version") String version, HttpServletRequest request) {
	 * log.info("Start of saveCloudConfig Method in Controller ###");
	 * SaveCloudAtomationRes saveCloudAtomationRes = new SaveCloudAtomationRes();
	 * 
	 * try { saveCloudAtomationRes = erpService.save(file, podId, projectId,
	 * version, request); } catch (Exception e) { log.error(e.getMessage());
	 * saveCloudAtomationRes.setErrMessage(e.getMessage()); return new
	 * ResponseEntity<SaveCloudAtomationRes>(saveCloudAtomationRes,
	 * HttpStatus.INTERNAL_SERVER_ERROR); } return new
	 * ResponseEntity<SaveCloudAtomationRes>(saveCloudAtomationRes, new
	 * HttpHeaders(), HttpStatus.OK); }
	 */

    @ApiOperation(value = "This Api is to save Cloud Temp")
    @ApiResponses(value = {@ApiResponse(code = 200, message = "Successful Response"),
            @ApiResponse(code = 500, message = "Server Side Error")})
    @PostMapping("/savecloudtemplates")
    public ResponseEntity<SaveTempResPo> saveCloudTemp(@RequestParam("file") MultipartFile file,
                                                       @RequestParam("podId") Long podId,
                                                       @RequestParam("projectId") Long projectId,
                                                       @RequestParam("version") String version,
                                                       @RequestParam("module") String module,
                                                       HttpServletRequest request) {
        log.info("Start of saveCloudTemp Method in Controller ###");
        SaveTempResPo saveTempResPo = new SaveTempResPo();

        try {
            saveTempResPo = erpService.savingCloudTemps(file, podId, projectId, version,module, request);
        } catch (Exception e) {
            log.error(e.getMessage());
            saveTempResPo.setErrorMessage(e.getMessage());
            return new ResponseEntity<SaveTempResPo>(saveTempResPo,
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return new ResponseEntity<SaveTempResPo>(saveTempResPo,
                new HttpHeaders(), HttpStatus.OK);
    }

    @ApiOperation(value = "This Api is to load into cloud")
    @ApiResponses(value = {@ApiResponse(code = 200, message = "Successful Response"),
            @ApiResponse(code = 500, message = "Server Side Error")})
    @PostMapping("/savingcloudloadmetadata")
    public ResponseEntity<LoadMetaDataFromCloudRes> saveCloudMetaData(@RequestParam("file") MultipartFile file,
                                                                      @RequestParam("podId") Long podId,
                                                                      @RequestParam("projectId") Long projectId,
                                                                      @RequestParam("version") String version,
                                                                      HttpServletRequest request) {
        log.info("Start of saveCloudMetaData Method in Controller ###");

        LoadMetaDataFromCloudRes loadMetaDataFromCloudRes = new LoadMetaDataFromCloudRes();
        try {
            loadMetaDataFromCloudRes = erpService.saveCloudMetadata(file, podId, projectId, version, request);
        } catch (Exception e) {
            log.error(e.getMessage());
            loadMetaDataFromCloudRes.setMessage("Please contact System Administrator there is an error while processing the request");
            loadMetaDataFromCloudRes.setError(e.getMessage());
            return new ResponseEntity<LoadMetaDataFromCloudRes>(loadMetaDataFromCloudRes,
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return new ResponseEntity<LoadMetaDataFromCloudRes>(loadMetaDataFromCloudRes,
                new HttpHeaders(), HttpStatus.OK);
    }

    @ApiOperation(value = "This Api is to load into cloud")
    @ApiResponses(value = {@ApiResponse(code = 200, message = "Successful Response"),
            @ApiResponse(code = 500, message = "Server Side Error")})
    @PostMapping("/saveroleobjects")
    public ResponseEntity<RoleObjectLinkResPo> saveRoleObject(@RequestParam("file") MultipartFile file,
                                                              @RequestParam("podId") Long podId,
                                                              @RequestParam("projectId") Long projectId,
                                                              @RequestParam("role") String role) {
        log.info("Start of saveRoleObject Method in Controller ###");

        RoleObjectLinkResPo roleObjectLinkResPo = new RoleObjectLinkResPo();
        try {
            roleObjectLinkResPo = erpService.savingRoleObject(file, podId, projectId, role);
        } catch (Exception e) {
            log.error(e.getMessage());
            roleObjectLinkResPo.setMessage("Please contact System Administrator there is an error while processing the request");
            roleObjectLinkResPo.setError(e.getMessage());
            return new ResponseEntity<RoleObjectLinkResPo>(roleObjectLinkResPo,
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return new ResponseEntity<RoleObjectLinkResPo>(roleObjectLinkResPo,
                new HttpHeaders(), HttpStatus.OK);
    }
	/*
	 * @ApiOperation(value = "This Api Automation of cloud")
	 * 
	 * @ApiResponses(value = {@ApiResponse(code = 200, message =
	 * "Successful Response"),
	 * 
	 * @ApiResponse(code = 500, message = "Server Side Error")})
	 * 
	 * @PostMapping("/automationofcloud") public
	 * ResponseEntity<SaveCloudAtomationRes> automationOfcloud(@RequestParam("file")
	 * MultipartFile file,
	 * 
	 * @RequestParam("podId") Long podId,
	 * 
	 * @RequestParam("projectId") Long projectId,
	 * 
	 * @RequestParam("version") String version,
	 * 
	 * @RequestParam("module") String module, HttpServletRequest request) {
	 * log.info("Start of saveCloudConfig Method in Controller ###");
	 * SaveCloudAtomationRes saveCloudAtomationRes = new SaveCloudAtomationRes();
	 * 
	 * try { saveCloudAtomationRes = erpService.automationOfcloud(file, podId,
	 * projectId, version,module, request); } catch (Exception e) {
	 * log.error(e.getMessage());
	 * saveCloudAtomationRes.setErrMessage(e.getMessage()); return new
	 * ResponseEntity<SaveCloudAtomationRes>(saveCloudAtomationRes,
	 * HttpStatus.INTERNAL_SERVER_ERROR); } return new
	 * ResponseEntity<SaveCloudAtomationRes>(saveCloudAtomationRes, new
	 * HttpHeaders(), HttpStatus.OK); }
	 */

}
