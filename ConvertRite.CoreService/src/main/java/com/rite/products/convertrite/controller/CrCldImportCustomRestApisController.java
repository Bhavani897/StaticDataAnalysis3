package com.rite.products.convertrite.controller;

import com.rite.products.convertrite.exception.ConvertRiteException;
import com.rite.products.convertrite.po.CustomRestApiReqPo;
import com.rite.products.convertrite.service.BankAccountErrorService;
import com.rite.products.convertrite.service.CrCldImportCustomRestApisServiceImpl;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.*;

import javax.validation.Valid;
import java.io.IOException;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/convertritecore/cloudimport")
public class CrCldImportCustomRestApisController {

    @Autowired
    CrCldImportCustomRestApisServiceImpl cldImportCustomRestApisServiceImpl;
    @Autowired
    private BankAccountErrorService bankAccountErrorService;
    @ApiOperation(value = "This api is for creating bank & branches")
    @ApiResponses(value = {@ApiResponse(code = 200, message = "Successful Response"), @ApiResponse(code = 500, message = "Server Side Error"), @ApiResponse(code = 400, message = "Bad Request")})
    @PostMapping("/createbankandbranches")
    public ResponseEntity<?> createBankAndBranches(@RequestBody CustomRestApiReqPo customRestApiReqPo) throws Exception {
        cldImportCustomRestApisServiceImpl.createBankAndBranches(customRestApiReqPo);
        return new ResponseEntity<>("successful", HttpStatus.OK);
    }
    @ApiOperation(value = "This api is for creating banks")
    @ApiResponses(value = {@ApiResponse(code = 200, message = "Successful Response"), @ApiResponse(code = 500, message = "Server Side Error"), @ApiResponse(code = 400, message = "Bad Request")})
    @PostMapping("/createbank")
    public ResponseEntity<?> createOrUpdateBank(@RequestBody CustomRestApiReqPo customRestApiReqPo) throws Exception {
        cldImportCustomRestApisServiceImpl.createOrUpdateBank(customRestApiReqPo);
        return new ResponseEntity<>("successful", HttpStatus.OK);
    }
    @ApiOperation(value = "This api is for creating branches")
    @ApiResponses(value = {@ApiResponse(code = 200, message = "Successful Response"), @ApiResponse(code = 500, message = "Server Side Error"), @ApiResponse(code = 400, message = "Bad Request")})
    @PostMapping("/createbranch")
    public ResponseEntity<?> createOrUpdateBranch(@RequestBody CustomRestApiReqPo customRestApiReqPo) throws Exception {
        cldImportCustomRestApisServiceImpl.createOrUpdateBranch(customRestApiReqPo);
        return new ResponseEntity<>("successful", HttpStatus.OK);
    }

    @ApiOperation(value = "This api is for creating bank account")
    @ApiResponses(value = {@ApiResponse(code = 200, message = "Successful Response"), @ApiResponse(code = 500, message = "Server Side Error"), @ApiResponse(code = 400, message = "Bad Request")})
    @PostMapping("/createbankaccount")
    public ResponseEntity<?> createOrUpdateBankAccount(@RequestBody CustomRestApiReqPo customRestApiReqPo) throws Exception {
        cldImportCustomRestApisServiceImpl.createOrUpdateBankAccount(customRestApiReqPo);
        return new ResponseEntity<>("successful", HttpStatus.OK);
    }

   /* @ApiOperation(value = "This api is for updating project DFF fields")
    @ApiResponses(value = {@ApiResponse(code = 200, message = "Successful Response"), @ApiResponse(code = 500, message = "Server Side Error"), @ApiResponse(code = 400, message = "Bad Request")})
    @PostMapping("/updateprojectdff")
    public ResponseEntity<?> updateProjectDff(@RequestBody CustomRestApiReqPo customRestApiReqPo) throws Exception {
        cldImportCustomRestApisServiceImpl.updateProjectDff(customRestApiReqPo);
        return new ResponseEntity<>("successful", HttpStatus.OK);
    }

    */

    @ApiOperation(value = "This API is for updating project DFF fields")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successful Response"),
            @ApiResponse(code = 500, message = "Server Side Error"),
            @ApiResponse(code = 400, message = "Bad Request")
    })
    @PostMapping("/updateprojectdff")
    public ResponseEntity<?> updateProjectDff(@Valid @RequestBody CustomRestApiReqPo customRestApiReqPo) throws Exception {
        try {
            if (!isValidBatchName(customRestApiReqPo.getBatchName())) {
                return new ResponseEntity<>("Invalid Batch Name", HttpStatus.BAD_REQUEST);
            }
            cldImportCustomRestApisServiceImpl.updateProjectDff(customRestApiReqPo);
            return new ResponseEntity<>("Successful", HttpStatus.OK);
        } catch (Exception e) {
            return new ResponseEntity<>("Server Side Error", HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
    private boolean isValidBatchName(String batchName) {
        return batchName != null && batchName.matches("[a-zA-Z0-9 ]+");
    }


   /* @ApiOperation(value = "This api is for tax registrations")
    @ApiResponses(value = {@ApiResponse(code = 200, message = "Successful Response"), @ApiResponse(code = 500, message = "Server Side Error"), @ApiResponse(code = 400, message = "Bad Request")})
    @PostMapping("/supplierTaxProfileUpdate")
    public ResponseEntity<?> supplierTaxProfileUpdate(@RequestBody CustomRestApiReqPo customRestApiReqPo) throws Exception {
        cldImportCustomRestApisServiceImpl.supplierTaxProfileUpdate(customRestApiReqPo);
        return new ResponseEntity<>("successful", HttpStatus.OK);
    }
*/

    @ApiOperation(value = "This API is for tax registrations")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successful Response"),
            @ApiResponse(code = 500, message = "Server Side Error"),
            @ApiResponse(code = 400, message = "Bad Request")
    })
    @PostMapping("/supplierTaxProfileUpdate")
    public ResponseEntity<?> supplierTaxProfileUpdate(
            @Valid @RequestBody CustomRestApiReqPo customRestApiReqPo,
            BindingResult bindingResult) throws Exception {
        if (bindingResult.hasErrors()) {
            String errorMessages = bindingResult.getAllErrors().stream()
                    .map(objectError -> objectError.getDefaultMessage())
                    .collect(Collectors.joining(", "));

            return new ResponseEntity<>("validation Failed: " + errorMessages, HttpStatus.BAD_REQUEST);
        }
        cldImportCustomRestApisServiceImpl.supplierTaxProfileUpdate(customRestApiReqPo);

        return new ResponseEntity<>("Successful", HttpStatus.OK);
    }

   /* @ApiOperation("This Api is for validate ccid")
    @ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
            @ApiResponse(code = 500, message = "Server Side Error") })
    @PostMapping("/validateccid")
    public ResponseEntity<?> validateCcid(@RequestBody CustomRestApiReqPo customRestApiReqPo)
            throws Exception {
            cldImportCustomRestApisServiceImpl.validateCcid(customRestApiReqPo);
        return new ResponseEntity<String>("successful", HttpStatus.OK);
    }

    */

    @ApiOperation(value = "This API is for validating CCID")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successful Response"),
            @ApiResponse(code = 500, message = "Server Side Error"),
            @ApiResponse(code = 400, message = "Bad Request: Validation failed")
    })
    @PostMapping("/validateccid")
    public ResponseEntity<?> validateCcid(
            @Valid @RequestBody CustomRestApiReqPo customRestApiReqPo,
            BindingResult bindingResult) throws Exception {
        if (bindingResult.hasErrors()) {
            String errorMessages = bindingResult.getAllErrors().stream()
                    .map(objectError -> objectError.getDefaultMessage())
                    .collect(Collectors.joining(", "));

            return new ResponseEntity<>("Validation Failed: " + errorMessages, HttpStatus.BAD_REQUEST);
        }
        cldImportCustomRestApisServiceImpl.validateCcid(customRestApiReqPo);
        return new ResponseEntity<>("Successful", HttpStatus.OK);
    }


   /* @ApiOperation("This Api updates Bank Number,Branch Number,Bank Account Id")
    @ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
            @ApiResponse(code = 500, message = "Server Side Error") })
    @PostMapping("/update/{updateType}")
    public ResponseEntity<?> updateCldStagingTable(@PathVariable String updateType, @RequestBody CustomRestApiReqPo customRestApiReqPo)
            throws Exception {
        cldImportCustomRestApisServiceImpl.updateCldStagingTable(updateType,customRestApiReqPo);
        return new ResponseEntity<String>("successful", HttpStatus.OK);
    }
    */

    @ApiOperation(value = "This API updates Bank Number, Branch Number, Bank Account Id")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successful Response"),
            @ApiResponse(code = 500, message = "Server Side Error"),
            @ApiResponse(code = 400, message = "Bad Request: Validation failed")
    })
    @PostMapping("/update/{updateType}")
    public ResponseEntity<?> updateCldStagingTable(
            @PathVariable String updateType,
            @Valid @RequestBody CustomRestApiReqPo customRestApiReqPo,
            BindingResult bindingResult) throws Exception {
        if (bindingResult.hasErrors()) {
            String errorMessages = bindingResult.getAllErrors().stream()
                    .map(objectError -> objectError.getDefaultMessage())
                    .collect(Collectors.joining(", "));
            return new ResponseEntity<>("validation Failed: " + errorMessages, HttpStatus.BAD_REQUEST);
        }
        cldImportCustomRestApisServiceImpl.updateCldStagingTable(updateType, customRestApiReqPo);
        return new ResponseEntity<>("Successful", HttpStatus.OK);
    }


    @GetMapping("/download-bank-account-error-records")
    public ResponseEntity<byte[]> downloadBankAccountErrorRecords(@RequestParam Long cldTempId,@RequestParam String batchName) {
        try {
            byte[] csvData = bankAccountErrorService.downloadBankAccountErrorRecords(cldTempId,batchName);

            HttpHeaders headers = new HttpHeaders();
            headers.add(HttpHeaders.CONTENT_TYPE, "text/csv");
            headers.add(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=bank_account_errors.csv");
            headers.setContentLength(csvData.length);

            return new ResponseEntity<>(csvData, headers, HttpStatus.OK);
        } catch (IOException e) {
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
    @GetMapping("/download-bank-or-branches-error-records")
    public ResponseEntity<byte[]> downloadBankOrBranchesErrorRecords(@RequestParam Long cldTempId,@RequestParam String batchName) {
        try {
            byte[] csvData = bankAccountErrorService.downloadBankOrBranchesErrorRecords(cldTempId,batchName);

            HttpHeaders headers = new HttpHeaders();
            headers.add(HttpHeaders.CONTENT_TYPE, "text/csv");
            headers.add(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=bank_errors.csv");
            headers.setContentLength(csvData.length);

            return new ResponseEntity<>(csvData, headers, HttpStatus.OK);
        } catch (IOException e) {
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
}
