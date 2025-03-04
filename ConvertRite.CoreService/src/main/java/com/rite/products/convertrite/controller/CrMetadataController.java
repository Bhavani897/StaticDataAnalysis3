package com.rite.products.convertrite.controller;

import com.rite.products.convertrite.Validations.Validations;
import com.rite.products.convertrite.exception.ConvertRiteException;
import com.rite.products.convertrite.exception.ValidationException;
import com.rite.products.convertrite.po.*;
import com.rite.products.convertrite.service.CrMetadataService;
import com.rite.products.convertrite.service.LoadMetaDataService;
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
@RequestMapping("/api/convertritecore/metadata")
public class CrMetadataController {
    @Autowired
    LoadMetaDataService loadMetaDataService;
    @Autowired
    CrMetadataService crMetadataService;
    @ApiOperation(value = "This Api loads metadata from cloud")
    @ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
            @ApiResponse(code = 500, message = "Server Side Error"),
            @ApiResponse(code = 400, message = "Bad Request") })
    @PostMapping("/loadmetadatafromcloud")
    public ResponseEntity<LoadMetaDataFromCloudRes> loadMetaDataFromCloud(@RequestBody CrCloudMetadataPo crCloudMetadataPo, HttpServletRequest request) throws ConvertRiteException {
        log.info("Start of loadMetaDataFromCloud Method in Controller ###");

        LoadMetaDataFromCloudRes loadMetaDataFromCloudRes = new LoadMetaDataFromCloudRes();
        try {
            loadMetaDataFromCloudRes = crMetadataService.loadMetaDataFromCloud(crCloudMetadataPo, request);
        } catch (ValidationException e) {
            log.error(e.getMessage() + "ValidationException");
            loadMetaDataFromCloudRes.setMessage(e.getMessage());
            loadMetaDataFromCloudRes
                    .setError("Please contact System Administrator there is an error while processing the request");
            return new ResponseEntity<LoadMetaDataFromCloudRes>(loadMetaDataFromCloudRes, HttpStatus.OK);
        } catch (Exception e) {
            log.error(e.getMessage() + "Exception");
            loadMetaDataFromCloudRes
                    .setMessage(e.getMessage());
            loadMetaDataFromCloudRes.setError(e.getMessage());
            return new ResponseEntity<LoadMetaDataFromCloudRes>(loadMetaDataFromCloudRes, new HttpHeaders(),
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return new ResponseEntity<LoadMetaDataFromCloudRes>(loadMetaDataFromCloudRes, new HttpHeaders(), HttpStatus.OK);
    }
    @ApiOperation(value = "This Api is to save cloudtemplatecolumns sequence based on ctl file")
    @ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
            @ApiResponse(code = 500, message = "Server Side Error"),
            @ApiResponse(code = 400, message = "Bad Request") })
    @PostMapping("/generatesequence")
    public ResponseEntity<?> generateSequence(@RequestParam("templateId") Long templateId,
                                              @RequestParam("objectId") Long objectId, @RequestParam("version") String version,
                                              HttpServletRequest request) {
        log.info("Start of generateSequence Method in Controller ###");
        List<CrCloudTemplateColumnsResPo> crCloudTemplateColumnsResPos = new ArrayList<>();
        try {
            crCloudTemplateColumnsResPos = crMetadataService.generateSequence(templateId, objectId, version, request);

        }  catch (Exception e) {
            log.error(e.getMessage());
            return new ResponseEntity<>(e.getMessage(),
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }

        return new ResponseEntity<>(crCloudTemplateColumnsResPos, new HttpHeaders(),
                HttpStatus.OK);

    }
    @ApiOperation(value = "This Api is for uploading data file to FTP")
    @ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
            @ApiResponse(code = 500, message = "Server Side Error") })
    @PostMapping("/uploadfiletoftp")
    public ResponseEntity<Object> uploadFileToFtp(@RequestParam("file") MultipartFile file)
            throws ConvertRiteException {
        log.info("Start of uploadFile Method in Controller######");
        String fileName = "";
        UploadFileResponse obj=null;
        try {
            fileName = loadMetaDataService.uploadFileToFtp(file);
            obj = new UploadFileResponse(fileName, file.getContentType(), file.getSize());
        } catch (Exception e) {
            log.error(e.getMessage());
            return new ResponseEntity<Object>(e.getMessage(),HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return new ResponseEntity<Object>(
                obj, new HttpHeaders(),
                HttpStatus.OK);
    }
    @ApiOperation(value = "This Api loads hdl metadata from cloud")
    @ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
            @ApiResponse(code = 500, message = "Server Side Error"),
            @ApiResponse(code = 400, message = "Bad Request") })
    @PostMapping("/loadhdlcloudmetadata")
    public ResponseEntity<LoadMetaDataFromCloudRes> loadHdlCloudMetaData(@RequestParam("objectId") Long objectId,@RequestParam("objectCode") String objectCode,
                                                                         @RequestParam("metaDataTableName") String metaDataTableName, @RequestParam("file") MultipartFile file)
            throws ConvertRiteException {
        log.info("Start of loadHdlCloudMetaData Method in Controller ###");

        LoadMetaDataFromCloudRes loadMetaDataFromCloudRes = new LoadMetaDataFromCloudRes();
        try {
            loadMetaDataFromCloudRes = crMetadataService.loadHdlCloudMetaData(objectId, metaDataTableName, file,objectCode);
        } catch (ValidationException e) {
            log.error(e.getMessage() + "ValidationException");
            loadMetaDataFromCloudRes.setMessage("Something went wrong while loading hdl metadata");
            loadMetaDataFromCloudRes.setError(e.getMessage());
            return new ResponseEntity<LoadMetaDataFromCloudRes>(loadMetaDataFromCloudRes, HttpStatus.OK);
        } catch (Exception e) {
            log.error(e.getMessage() + "Exception");
            loadMetaDataFromCloudRes.setMessage("Something went wrong while loading hdl metadata");
            loadMetaDataFromCloudRes.setError(e.getMessage());
            return new ResponseEntity<LoadMetaDataFromCloudRes>(loadMetaDataFromCloudRes, new HttpHeaders(),
                    HttpStatus.INTERNAL_SERVER_ERROR);

        }
        return new ResponseEntity<LoadMetaDataFromCloudRes>(loadMetaDataFromCloudRes, new HttpHeaders(), HttpStatus.OK);
    }

    @PostMapping("/uploadfile")
    public ResponseEntity<UploadFileResponse> uploadFile(@RequestParam("file") MultipartFile file,
                                                         @RequestParam("type") String type) throws ConvertRiteException {
        log.info("Start of uploadFile Method in Controller######");
        String fileName = "";
        try {
            fileName = loadMetaDataService.uploadFile(file);
        } catch (Exception e) {
            e.printStackTrace();
            throw new ConvertRiteException(
                    "Please contact System Administrator there is an error while processing the request",
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return new ResponseEntity<UploadFileResponse>(
                new UploadFileResponse(fileName, file.getContentType(), file.getSize()), new HttpHeaders(),
                HttpStatus.OK);
    }

    @GetMapping("/downloadsourcetemplate")
    public void downloadSourceFile(@RequestParam(name = "objectCode", required = false) String objectCode,
                                   @RequestParam("metaDataFileType") String metaDataFileType, @RequestParam("environment") String environment, HttpServletResponse response)
            throws ConvertRiteException {
        log.info("Start of downloadSourceFile Method in Controller######");
        response.setContentType("text/csv");

        if ("TABLE".equalsIgnoreCase(metaDataFileType) || "COLUMN".equalsIgnoreCase(metaDataFileType)) {
            if ("TABLE".equalsIgnoreCase(metaDataFileType)) {
                response.setHeader(HttpHeaders.CONTENT_DISPOSITION,
                        "attachment; filename=Rite_Source_Table_Template.csv");
                if (Validations.isNullOrEmpty(objectCode)) {
                    //throw new ConvertRiteException("ObjectCode is Mandatory in Request", HttpStatus.BAD_REQUEST);
                }} else if ("COLUMN".equalsIgnoreCase(metaDataFileType))
                response.setHeader(HttpHeaders.CONTENT_DISPOSITION,
                        "attachment; filename=Rite_Source_MetaData_Template.csv");
            try {
                loadMetaDataService.downloadSourceFile(response.getWriter(), objectCode, metaDataFileType,environment);
            } catch (Exception e) {
                log.error(e.getMessage());
                throw new ConvertRiteException(
                        "Please contact System Administrator there is an error while processing the request",
                        HttpStatus.INTERNAL_SERVER_ERROR);
            }

        } else
            throw new ConvertRiteException("metaDataFileType Value Should be TABLE OR COLUMN", HttpStatus.BAD_REQUEST);
    }
}
