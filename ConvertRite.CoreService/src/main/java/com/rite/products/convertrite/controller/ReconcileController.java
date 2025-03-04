package com.rite.products.convertrite.controller;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.rite.products.convertrite.exception.ConvertRiteException;
import com.rite.products.convertrite.exception.ValidationException;
import com.rite.products.convertrite.model.XxrReconcileConfig;
import com.rite.products.convertrite.model.XxrReconcileStatistics;
import com.rite.products.convertrite.po.ReconcileDetailsReqPo;
import com.rite.products.convertrite.po.ReconcileDetailsResPo;
import com.rite.products.convertrite.po.ReconcileErrorReqPo;
import com.rite.products.convertrite.po.ReconcileErrorResPo;
import com.rite.products.convertrite.po.SaveReconcileConfigReqPo;
import com.rite.products.convertrite.po.SaveReconcileConfigResPo;
import com.rite.products.convertrite.service.ReconcileService;

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

@RestController
public class ReconcileController {

	private static final Logger log = LoggerFactory.getLogger(ReconcileController.class);

	@Autowired
	ReconcileService reconcileService;

	@ApiOperation("This is to fetch reconcile statistics details")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error"),
			@ApiResponse(code = 400, message = "Bad Request") })
	@GetMapping("/getreconcilestatistics")
	public ResponseEntity<XxrReconcileStatistics> getReconcileStatistics(
			@RequestParam("cldTemplateName") String cldTemplateName, @RequestParam("batchName") String batchName)
			throws ConvertRiteException {
		XxrReconcileStatistics xxrReconcileStatistics = new XxrReconcileStatistics();
		log.info("Start of getReconcileStatistics #########");
		try {
			xxrReconcileStatistics = reconcileService.getReconcileStatistics(cldTemplateName, batchName);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<XxrReconcileStatistics>(xxrReconcileStatistics, HttpStatus.OK);
	}

	@ApiOperation(value = "This Api is for generating reconcile details")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@PostMapping("/getreconciledetails")
	public ResponseEntity<ReconcileDetailsResPo> getReconcileDetails(
			@RequestBody ReconcileDetailsReqPo reconcileDetailsReqPo) throws ConvertRiteException {
		log.info("Start of getReconcileDetails Method in Controller######");
		ReconcileDetailsResPo reconcileDetailsResPo = new ReconcileDetailsResPo();
		try {
			reconcileDetailsResPo = reconcileService.getReconcileDetails(reconcileDetailsReqPo);
		} catch (Exception e) {
			e.printStackTrace();
			log.error(e.getMessage());
			reconcileDetailsResPo
					.setMessage("Please contact System Administrator there is an error while processing the request");
			reconcileDetailsResPo.setError(e.getMessage());
			return new ResponseEntity<ReconcileDetailsResPo>(reconcileDetailsResPo, HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<ReconcileDetailsResPo>(reconcileDetailsResPo, HttpStatus.OK);
	}

	@ApiOperation(value = "This Api is for generating reconcile error details")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@PostMapping("/getreconcileerrordetails")
	public ResponseEntity<ReconcileErrorResPo> getReconcileErrors(
			@RequestBody ReconcileErrorReqPo reconcileErrorReqPo) {
		log.info("Start of getReconcileErrors Method in Controller######");
		ReconcileErrorResPo reconcileErrorResPo = new ReconcileErrorResPo();
		try {

			reconcileErrorResPo = reconcileService.getReconcileErrors(reconcileErrorReqPo);
		} catch (Exception e) {
			e.printStackTrace();
			log.error(e.getMessage());
			reconcileErrorResPo
					.setMessage("Please contact System Administrator there is an error while processing the request");
			reconcileErrorResPo.setError(e.getMessage());
			return new ResponseEntity<ReconcileErrorResPo>(reconcileErrorResPo, HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<ReconcileErrorResPo>(reconcileErrorResPo, HttpStatus.OK);

	}

	@ApiOperation(value = "This Api is for saving reconcile config details")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),

			@ApiResponse(code = 500, message = "Server Side Error") })
	@PostMapping("savereconcileconfig")
	public ResponseEntity<SaveReconcileConfigResPo> saveReconcileConfig(
			@RequestBody SaveReconcileConfigReqPo saveReconcileConfigReqPo) {
		log.info("Start of saveReconcileConfig in controller ###");
		SaveReconcileConfigResPo saveReconcileConfigResPo = new SaveReconcileConfigResPo();
		try {
			saveReconcileConfigResPo = reconcileService.saveReconcileConfig(saveReconcileConfigReqPo);
		} catch (ValidationException e) {
			log.error(e.getMessage());
			saveReconcileConfigResPo.setMessage(e.getMessage());
			saveReconcileConfigResPo
					.setError("Please contact System Administrator there is an error while processing the request");
			return new ResponseEntity<SaveReconcileConfigResPo>(saveReconcileConfigResPo,
					HttpStatus.OK);
		} catch (Exception e) {
			log.error(e.getMessage());
			saveReconcileConfigResPo
					.setMessage("Please contact System Administrator there is an error while processing the request");
			saveReconcileConfigResPo.setError(e.getMessage());
			return new ResponseEntity<SaveReconcileConfigResPo>(saveReconcileConfigResPo,
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<SaveReconcileConfigResPo>(saveReconcileConfigResPo, HttpStatus.OK);

	}
	
	@ApiOperation(value="This Api is for gettting Reconcile Config Details")
	@ApiResponses(value={ @ApiResponse(code = 200, message = "Successful Response"),

			@ApiResponse(code = 500, message = "Server Side Error")})
	@GetMapping("/getreconcileconfig")
	public ResponseEntity<List<XxrReconcileConfig>> getReconcileConfig() throws ConvertRiteException{
		List<XxrReconcileConfig> reconcileConfigLi=new ArrayList<>();
		try {
		reconcileConfigLi=reconcileService.getReconcileConfig();
		}catch(Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<List<XxrReconcileConfig>>(reconcileConfigLi,HttpStatus.OK);
	}
}
