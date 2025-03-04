package com.rite.products.convertrite.controller;

import java.util.ArrayList;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import com.rite.products.convertrite.model.CrProjectActivities;
import com.rite.products.convertrite.model.CrProjects;
import com.rite.products.convertrite.po.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;

import com.rite.products.convertrite.Validations.Validations;
import com.rite.products.convertrite.exception.BadRequestException;
import com.rite.products.convertrite.exception.ConvertRiteException;
import com.rite.products.convertrite.exception.ValidationException;
import com.rite.products.convertrite.service.CrProjectService;

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

@RestController
@RequestMapping(value = "/api/convertritecore")
public class CrProjectEntryController {

    @Autowired
    CrProjectService crProjectService;

    private static final Logger log = LoggerFactory.getLogger(CrProjectEntryController.class);



    @ApiOperation(value = "This Api will save project headers")
    @ApiResponses(value = {@ApiResponse(code = 200, message = "Successful Response"),
            @ApiResponse(code = 500, message = "Server Side Error"),
            @ApiResponse(code = 400, message = "Bad Request")})
    @PostMapping("/saveprojectheaders")
    public ResponseEntity<SaveProjectHeaderResponsePo> saveProjectHeaders(
            @RequestBody CrProjects projectHeadersPo, HttpServletRequest request) {
        log.info("Start of saveProjectHeaders Method in Controller ###");
        SaveProjectHeaderResponsePo saveProjectHeaderResponsePo = new SaveProjectHeaderResponsePo();
        try {
            if (Validations.isNullOrEmpty(projectHeadersPo.getProjectName())
                    || Validations.isNullOrEmpty(projectHeadersPo.getProjecManager())
            )
                throw new BadRequestException(
                        "projectName,projectManager,programNumber,clientManager,kpiAggregartionLevel,startDate,projectStatus and accessLevel are Mandatory fields");
            saveProjectHeaderResponsePo = crProjectService.saveProjectHeaders(projectHeadersPo, request);
        } catch (BadRequestException e) {
            log.error(e.getMessage());
            saveProjectHeaderResponsePo.setMessage("Error while saving project headers");
            saveProjectHeaderResponsePo.setError(e.getMessage());
            return new ResponseEntity<>(saveProjectHeaderResponsePo, HttpStatus.BAD_REQUEST);
        } catch (Exception e) {
            log.error(e.getMessage());
            saveProjectHeaderResponsePo.setMessage("Error while saving project headers");
            saveProjectHeaderResponsePo.setError(e.getMessage());
            return new ResponseEntity<SaveProjectHeaderResponsePo>(saveProjectHeaderResponsePo,
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return new ResponseEntity<SaveProjectHeaderResponsePo>(saveProjectHeaderResponsePo, new HttpHeaders(),
                HttpStatus.OK);
    }



    @ApiOperation(value = "This Api return all project headers")
    @ApiResponses(value = {@ApiResponse(code = 200, message = "Successful Response"),
            @ApiResponse(code = 500, message = "Server Side Error"),
            @ApiResponse(code = 400, message = "Bad Request")})
    @GetMapping("/getallprojectheaders")
    public ResponseEntity<Object> getAllProjectHeaders() throws ConvertRiteException {
        log.info("Start of getAllProjectHeaders Method in Controller ###");
        List<CrProjects> xxrProjectsList = new ArrayList<>();
        try {
            xxrProjectsList = crProjectService.getAllProjectHeaders();
        } catch (Exception e) {
            log.error(e.getMessage());
            throw new ConvertRiteException(
                    "Please contact System Administrator there is an error while processing the request",
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return new ResponseEntity<Object>(xxrProjectsList, new HttpHeaders(), HttpStatus.OK);
    }

    @ApiOperation(value = "This Api return all project lines based on projectId")
    @ApiResponses(value = {@ApiResponse(code = 200, message = "Successful Response"),
            @ApiResponse(code = 500, message = "Server Side Error"),
            @ApiResponse(code = 400, message = "Bad Request")})
    @GetMapping("/getprojectlinesbyid")
    public ResponseEntity<List<CrProjectActivities>> getProjectLinesById(ProjectLinesReqPo projectLinesReqPo) throws ConvertRiteException {
        log.info("Start of getProjectLinesById Method in Controller ###");
        List<CrProjectActivities> xxrActivitiesList = new ArrayList<>();
        HttpHeaders httpHeaders = new HttpHeaders();
        try {
            xxrActivitiesList = crProjectService.getProjectLinesById(projectLinesReqPo, httpHeaders);
        } catch (Exception e) {
            log.error(e.getMessage());
            throw new ConvertRiteException(
                    "Please contact System Administrator there is an error while processing the request",
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return new ResponseEntity<List<CrProjectActivities>>(xxrActivitiesList, httpHeaders, HttpStatus.OK);
    }

    @ApiOperation(value = "This Api is to save or update Activity lines")
    @ApiResponses(value = {@ApiResponse(code = 200, message = "Successful Response"),
            @ApiResponse(code = 500, message = "Server Side Error"),
            @ApiResponse(code = 400, message = "Bad Request")})
    @PostMapping("/upsertprojectactivities")
    public ResponseEntity<ActivitiesResPo> upsertProjectActivities(@RequestBody List<CrProjectActivities> activitiesPo,
                                                                   HttpServletRequest request) {
        log.info("Start of upsertProjectActivities Method in Controller ###");
        // List<XxrActivities> xxrActivities = new ArrayList<>();
        ActivitiesResPo activitiesResPo = new ActivitiesResPo();
        //if (activitiesPo.get(0).getProjectId() != null) {
        try {
            activitiesResPo = crProjectService.upsertProjectActivities(activitiesPo, request);
        } catch (BadRequestException e) {
            log.error(e.getMessage());
            activitiesResPo.setMessage("Error while saving project activities");
            activitiesResPo.setError(e.getMessage());
            return new ResponseEntity<ActivitiesResPo>(activitiesResPo, HttpStatus.BAD_REQUEST);
        } catch (Exception e) {
            log.error(e.getMessage());
            activitiesResPo.setMessage("Error while saving project activities");
            activitiesResPo.setError(e.getMessage());
            return new ResponseEntity<ActivitiesResPo>(activitiesResPo, HttpStatus.INTERNAL_SERVER_ERROR);
        }
//		} else {
//			activitiesResPo.setMessage("Error while saving project activities");
//			activitiesResPo.setError("Missing projectId in the Request");
//			return new ResponseEntity<ActivitiesResPo>(activitiesResPo, HttpStatus.BAD_REQUEST);
//		}

        return new ResponseEntity<ActivitiesResPo>(activitiesResPo, new HttpHeaders(), HttpStatus.OK);
    }






    @GetMapping("/getProjectsAndObjects")
    public ResponseEntity<Object> getProjectsAndObjects(@RequestParam(name = "clientId") Long clientId, @RequestParam(name = "podId") Long podId, @RequestParam(name = "projectCode") String projectCode, HttpServletRequest request) {
        Object response = null;
        try {
            response = crProjectService.getProjectsAndObjects(clientId, podId, projectCode, request);
        } catch (Exception e) {
            log.error(e.getMessage());
            throw new RuntimeException(e);
        }
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    @GetMapping("/updateProjectObjects")
    public ResponseEntity<Object> updateProjectObjects(@RequestParam(name = "clientId") Long clientId, @RequestParam(name = "podId") Long podId, @RequestParam(name = "projectId") Long projectId, HttpServletRequest request) {
        Object response = null;
        try {
            response = crProjectService.updateProjectObjects(clientId, podId, projectId, request);
        } catch (Exception e) {
            log.error("Error in updateProjectObjects--->"+e);
            throw new RuntimeException(e.getMessage());
        }
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    @GetMapping("/getParentObjectsByProjectId/{projectId}")
    public ResponseEntity<Object> getParentObjects(@PathVariable Long projectId,@RequestHeader("Conversiontype") String conversionType) {
        Object response = null;
        try {

            response = crProjectService.getParentObjects(projectId,conversionType);
        } catch (Exception e) {
            log.error(e.getMessage());
            throw new RuntimeException(e);
        }
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    @GetMapping("/getObjectsByObjectCode/{projectId}/{objectCode}")
    public ResponseEntity<Object> getObjectsByObjectCode(@PathVariable Long projectId, @PathVariable String objectCode) {
        Object response = null;
        try {
            response = crProjectService.getObjectsByObjectCode(projectId, objectCode);
        } catch (Exception e) {
            log.error(e.getMessage());
            throw new RuntimeException(e);
        }
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    @GetMapping("/insertProjectActivities")
    public ResponseEntity<Object> insertProjectActivities(@RequestParam(name = "projectId") Long projectId) {
        Object response = null;
        try {
            response = crProjectService.insertProjectActivities(projectId);
        } catch (Exception e) {
            log.error(e.getMessage());
            throw new RuntimeException(e);
        }
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    @GetMapping("/getObjectsByUserId/{userId}")
    public ResponseEntity<Object> getObjectsByUserId(@PathVariable Long userId, @RequestHeader("Authorization") String bearerToken) {
        Object response = crProjectService.getObjectsByUserId(userId, bearerToken);
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    @ApiOperation(value = "This Api will give preload cloud setup status enableFlag")
    @ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
            @ApiResponse(code = 500, message = "Server Side Error") })
    @GetMapping("/getPreloadCloudSetupStatusEnable")
    public ResponseEntity<Boolean> getPreloadCloudSetupStatusEnable() {
        return new ResponseEntity<>(crProjectService.getPreCloudSetupStatusEnable(), HttpStatus.OK);
    }
}