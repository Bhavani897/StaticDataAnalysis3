package com.rite.products.convertrite.controller;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.rite.products.convertrite.Validations.Validations;
import com.rite.products.convertrite.exception.BadRequestException;
import com.rite.products.convertrite.exception.ConvertRiteException;
import com.rite.products.convertrite.exception.ValidationException;
import com.rite.products.convertrite.po.ErpIntegrationMetaDataReqPo;
import com.rite.products.convertrite.po.SaveErpIntegrationMetaDataResPo;
import com.rite.products.convertrite.po.XxrErpIntegrationMetaDataResPo;
import com.rite.products.convertrite.service.ErpIntegartionMetadataService;

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

@RestController
public class ErpIntegrationMetaDataController {

	@Autowired
	ErpIntegartionMetadataService erpIntegartionMetadataService;

	private static final Logger log = LoggerFactory.getLogger(ErpIntegrationMetaDataController.class);

	@ApiOperation(value = "This Api is to save erp integration metadata details ")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error"),
			@ApiResponse(code = 400, message = "Bad Request") })
	@PostMapping("/saveerpintegrationmetadata")
	public ResponseEntity<SaveErpIntegrationMetaDataResPo> saveErpIntegrationMetaData(
			@RequestBody ErpIntegrationMetaDataReqPo erpIntegrationMetaDataReqPo) {
		log.info("Start of saveErpIntegrationMetaData Method in Controller ###");

		SaveErpIntegrationMetaDataResPo erpIntegrationMetaDataResPo = new SaveErpIntegrationMetaDataResPo();
		try {
			if (//erpIntegrationMetaDataReqPo.getPodId() == null || erpIntegrationMetaDataReqPo.getProjectId() == null
				erpIntegrationMetaDataReqPo.getObjectId() == null
					|| erpIntegrationMetaDataReqPo.getParentObjectId() == null
					|| Validations.isNullOrEmpty(erpIntegrationMetaDataReqPo.getInterfaceDetails())
					|| Validations.isNullOrEmpty(erpIntegrationMetaDataReqPo.getDocumentAccount())
					|| Validations.isNullOrEmpty(erpIntegrationMetaDataReqPo.getJobName())
					|| Validations.isNullOrEmpty(erpIntegrationMetaDataReqPo.getDocumentSecurityGroup()))
				throw new BadRequestException(
						"parentObjectId,objectId,interfaceDetails,documentAccount,documentSecurityGroup and jobName are Mandatory fields");
			erpIntegrationMetaDataResPo = erpIntegartionMetadataService
					.saveErpIntegrationMetaData(erpIntegrationMetaDataReqPo);
		} catch (BadRequestException e) {
			log.error(e.getMessage());
			erpIntegrationMetaDataResPo.setMessage(e.getMessage());
			erpIntegrationMetaDataResPo.setError("Error while saving erp integartion metadata");
			return new ResponseEntity<SaveErpIntegrationMetaDataResPo>(erpIntegrationMetaDataResPo,
					HttpStatus.BAD_REQUEST);
		} catch (ValidationException e) {
			log.error(e.getMessage());
			erpIntegrationMetaDataResPo.setMessage(e.getMessage());
			erpIntegrationMetaDataResPo.setError("Please contact System Administrator there is an error while processing the request");
			return new ResponseEntity<SaveErpIntegrationMetaDataResPo>(erpIntegrationMetaDataResPo,
					HttpStatus.OK);
		}catch (Exception e) {
			log.error(e.getMessage());
			erpIntegrationMetaDataResPo.setMessage("Please contact System Administrator there is an error while processing the request");
			erpIntegrationMetaDataResPo.setError(e.getMessage());
			return new ResponseEntity<SaveErpIntegrationMetaDataResPo>(erpIntegrationMetaDataResPo,
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<SaveErpIntegrationMetaDataResPo>(erpIntegrationMetaDataResPo, new HttpHeaders(),
				HttpStatus.OK);
	}

	@ApiOperation(value = "This Api is to get all erp integration metadata details")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error"),
			@ApiResponse(code = 400, message = "Bad Request") })
	@GetMapping("/geterpintegrationmetadata")
	public ResponseEntity<List<XxrErpIntegrationMetaDataResPo>> getErpIntegrationMetaData() throws ConvertRiteException {
		log.info("Start of getErpIntegrationMetaData Method in Controller ###");

		List<XxrErpIntegrationMetaDataResPo> erpIntegrationMetaDataLi = new ArrayList<>();
		try {
			erpIntegrationMetaDataLi = erpIntegartionMetadataService.getErpIntegrationMetaData();
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<List<XxrErpIntegrationMetaDataResPo>>(erpIntegrationMetaDataLi, new HttpHeaders(),
				HttpStatus.OK);
	}

	/*
	 * @ApiOperation(value =
	 * "This Api is to get  erp integration metadata details filtered with podId,projectId,objectId,parentObjectId"
	 * )
	 * 
	 * @ApiResponses(value = { @ApiResponse(code = 200, message =
	 * "Successful Response"),
	 * 
	 * @ApiResponse(code = 500, message = "Server Side Error"),
	 * 
	 * @ApiResponse(code = 400, message = "Bad Request") })
	 * 
	 * @GetMapping("/geterpintegrationmetadatabyids") public
	 * ResponseEntity<XxrErpIntegrationMetaDataResPo>
	 * getErpIntegrationMetaDataByIds(@RequestParam("podId") Long podId,
	 * 
	 * @RequestParam("projectId") Long projectId, @RequestParam("parentObjectId")
	 * Long parentObjectId,
	 * 
	 * @RequestParam("objectId") Long objectId) throws ConvertRiteException {
	 * log.info("Start of getErpIntegrationMetaDataByIds Method in Controller ###");
	 * 
	 * XxrErpIntegrationMetaDataResPo erpIntegrationMetaData = new
	 * XxrErpIntegrationMetaDataResPo(); try { erpIntegrationMetaData =
	 * erpIntegartionMetadataService.getErpIntegrationMetaDataByIds(podId,
	 * projectId, parentObjectId, objectId); } catch (Exception e) {
	 * log.error(e.getMessage()); throw new ConvertRiteException(
	 * "Please contact System Administrator there is an error while processing the request"
	 * , HttpStatus.INTERNAL_SERVER_ERROR); } return new
	 * ResponseEntity<XxrErpIntegrationMetaDataResPo>(erpIntegrationMetaData, new
	 * HttpHeaders(), HttpStatus.OK); }
	 */
	
	@ApiOperation(value = "This Api is to get  erp integration metadata details filtered with objectId and parentObjectId")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error"),
			@ApiResponse(code = 400, message = "Bad Request") })
	@GetMapping("/geterpintegrationmetadatabyids")
	public ResponseEntity<XxrErpIntegrationMetaDataResPo> getErpIntegrationMetaDataByIds(@RequestParam("parentObjectId") Long parentObjectId,
			@RequestParam("objectId") Long objectId) throws ConvertRiteException {
		log.info("Start of getErpIntegrationMetaDataByIds Method in Controller ###");

		XxrErpIntegrationMetaDataResPo erpIntegrationMetaData = new XxrErpIntegrationMetaDataResPo();
		try {
			erpIntegrationMetaData = erpIntegartionMetadataService.getErpIntegrationMetaDataByIds(parentObjectId, objectId);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<XxrErpIntegrationMetaDataResPo>(erpIntegrationMetaData, new HttpHeaders(), HttpStatus.OK);
	}

}
