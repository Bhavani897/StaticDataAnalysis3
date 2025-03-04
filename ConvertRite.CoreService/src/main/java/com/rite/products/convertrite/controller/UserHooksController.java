package com.rite.products.convertrite.controller;

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
import com.rite.products.convertrite.model.XxrUserHooksDetailsView;
import com.rite.products.convertrite.po.SaveUserHooksResPo;
import com.rite.products.convertrite.po.UserHooksDetailsReqPo;
import com.rite.products.convertrite.service.UserHookService;

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

@RestController
public class UserHooksController {

	private static final Logger log = LoggerFactory.getLogger(UserHooksController.class);

	@Autowired
	UserHookService userHookService;

	@ApiOperation(value = "This Api is for save/update of userhooks")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@PostMapping("/saveuserhooks")
	public ResponseEntity<SaveUserHooksResPo> saveUserHooksDetails(
			@RequestBody UserHooksDetailsReqPo userHooksDetailsReqPo) {
		log.info("Start of saveUserHooksDetails Method ##########");
		SaveUserHooksResPo saveUserHooksResPo = new SaveUserHooksResPo();
		try {
			saveUserHooksResPo = userHookService.saveUserHooksDetails(userHooksDetailsReqPo);
		} catch (ValidationException e) {
			saveUserHooksResPo.setMessage(e.getMessage());
			saveUserHooksResPo
					.setError("Please contact System Administrator there is an error while processing the request");
			return new ResponseEntity<SaveUserHooksResPo>(saveUserHooksResPo, HttpStatus.OK);
		} catch (Exception e) {
			saveUserHooksResPo
					.setMessage("Please contact System Administrator there is an error while processing the request");
			saveUserHooksResPo.setError(e.getMessage());
			return new ResponseEntity<SaveUserHooksResPo>(saveUserHooksResPo, HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<SaveUserHooksResPo>(saveUserHooksResPo, HttpStatus.OK);
	}

	@ApiOperation(value = "This Api is for fetching userhooks details")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/getuserhooksdtls")
	public ResponseEntity<XxrUserHooksDetailsView> getUserHooksDtls(@RequestParam("cldTemplateId") Long cldTemplateId)
			throws ConvertRiteException {
		log.info("Start of getUserHooksDtls Method ########");
		XxrUserHooksDetailsView xxrUserHooksDetailsView = new XxrUserHooksDetailsView();
		try {
			xxrUserHooksDetailsView = userHookService.getUserHooksDtls(cldTemplateId);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<XxrUserHooksDetailsView>(xxrUserHooksDetailsView, HttpStatus.OK);
	}

}
