package com.rite.products.convertrite.controller;

import javax.servlet.http.HttpServletRequest;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import com.rite.products.convertrite.exception.ValidationException;
import com.rite.products.convertrite.po.BasicResponsePo;
import com.rite.products.convertrite.po.CrLoadDataFromEbsReqPo;
import com.rite.products.convertrite.po.CrLoadMetaDataFromEbsReqPo;
import com.rite.products.convertrite.po.CrSaveEbsConnectionDetailsReqPo;
import com.rite.products.convertrite.service.CrEbsConnectionServiceImpl;

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

@RestController
@RequestMapping("/api/convertritecore/ebsconnection")
@Validated
public class CrEbsConnectionController {

	@Autowired
	CrEbsConnectionServiceImpl crEbsConnectionServiceImpl;

	@ApiOperation(value = "Save Ebs Connection details")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 400, message = "Bad Request"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@PostMapping("/saveebsconnectiondtls")
	public ResponseEntity<BasicResponsePo> saveEbsConnectionDtls(
			@RequestBody @Valid CrSaveEbsConnectionDetailsReqPo ebsConnectnDtlsReqPo, HttpServletRequest request)
			throws ValidationException, Exception {
		BasicResponsePo responsePo = crEbsConnectionServiceImpl.saveEbsConnectionDtls(ebsConnectnDtlsReqPo, request);
		return new ResponseEntity<>(responsePo, HttpStatus.CREATED);
	}

	@ApiOperation(value = "Get Source Connection details")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 400, message = "Bad Request"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/getebsconnectiondtls")
	public ResponseEntity<BasicResponsePo> getEbsConnectionDtls() {
		return new ResponseEntity<>(crEbsConnectionServiceImpl.getEbsConnectionDtls(), HttpStatus.OK);
	}

	@ApiOperation(value = "Api loads source metadata through ebs adaptor")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error"),
			@ApiResponse(code = 400, message = "Bad Request") })
	@PostMapping("/loadsrcmetadatafromebs")
	public ResponseEntity<BasicResponsePo> loadSrcMetaDataFromEbs(
			@RequestBody @Valid CrLoadMetaDataFromEbsReqPo crLoadMetaDataFromEbsReqPo, HttpServletRequest request)
			throws ValidationException, Exception {
		return new ResponseEntity<>(
				crEbsConnectionServiceImpl.loadSrcMetaDataFromEbs(crLoadMetaDataFromEbsReqPo, request), HttpStatus.OK);
	}


	@ApiOperation(value = "Api loads data from EBS")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error"),
			@ApiResponse(code = 400, message = "Bad Request") })
	@PostMapping("/loadsrcdatafromebs")
	public ResponseEntity<BasicResponsePo> loadSrcDataFromEbs(@RequestBody @Valid CrLoadDataFromEbsReqPo crLoadDataFromEbsReqPo, HttpServletRequest request)throws Exception{
		String tenantId=request.getHeader("X-TENANT-ID");
		String userId=request.getHeader("userId");
		return new ResponseEntity<>(crEbsConnectionServiceImpl.loadSrcDataFromEbs(crLoadDataFromEbsReqPo,tenantId,userId), HttpStatus.OK);
	}

	@ApiOperation(value="Api return EBS adaptor enable flag")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error"),
			@ApiResponse(code = 400, message = "Bad Request") })
	@GetMapping("/getebsadaptorenableflag")
	public ResponseEntity<BasicResponsePo> getEbsAdaptorEnableFlag(@RequestParam("objectId") @NotNull(message="objectId cannot be null") Long objectId
	,@RequestParam("connectionType") String connectionType) throws Exception{
		return new ResponseEntity<>(crEbsConnectionServiceImpl.getEbsAdaptorEnableFlag(objectId,connectionType), HttpStatus.OK);
	}

	@ApiOperation(value = "Get Ebs Connection details")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 400, message = "Bad Request"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@DeleteMapping("/deleteebsconnectiondtls")
	public ResponseEntity<BasicResponsePo> deleteEbsConnectionDtls(@RequestParam("connectionId") Long connectionId,HttpServletRequest request) throws ValidationException,Exception {
		return  new ResponseEntity<>(crEbsConnectionServiceImpl.deleteEbsConnectionDtls(connectionId,request), HttpStatus.OK);
	}
}
