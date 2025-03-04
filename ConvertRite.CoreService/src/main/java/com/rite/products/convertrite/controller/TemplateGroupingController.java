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
import org.springframework.web.bind.annotation.RestController;

import com.rite.products.convertrite.Validations.Validations;
import com.rite.products.convertrite.exception.BadRequestException;
import com.rite.products.convertrite.exception.ConvertRiteException;
import com.rite.products.convertrite.exception.ValidationException;
import com.rite.products.convertrite.po.SaveTemplateGroupingReqPo;
import com.rite.products.convertrite.po.SaveTemplateGroupingResPo;
import com.rite.products.convertrite.po.XxrTemplateRelationResPo;
import com.rite.products.convertrite.service.TemplateGroupingService;

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

@RestController
public class TemplateGroupingController {
	
	private static final Logger log = LoggerFactory.getLogger(TemplateGroupingController.class);
	
	@Autowired
	TemplateGroupingService templateGroupingService;
	
	@ApiOperation(value = "This api is to save templategrouping")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error"),
			@ApiResponse(code = 400, message = "Bad Request") })
	@PostMapping("/savetemplategrouping")
	public ResponseEntity<SaveTemplateGroupingResPo> saveTemplateGrouping(
			@RequestBody SaveTemplateGroupingReqPo saveTemplateGroupingReqPo) {
		log.info("Start of saveTemplateGrouping Method in Controller ###");

		SaveTemplateGroupingResPo saveTemplateGroupingResPo = new SaveTemplateGroupingResPo();
		try {
			if (Validations.isNullOrEmpty(saveTemplateGroupingReqPo.getTemplateIds())
					|| Validations.isNullOrEmpty(saveTemplateGroupingReqPo.getVersion())
					||saveTemplateGroupingReqPo.getGroupId()==null )
				throw new BadRequestException(
						"templateIds,groupId and version are Mandatory fields");

			saveTemplateGroupingResPo = templateGroupingService.saveTemplateGrouping(saveTemplateGroupingReqPo);

		} catch (ValidationException e) {
			log.error(e.getMessage() + "ValidationException");
			saveTemplateGroupingResPo.setMessage("Error while saving templategrouping");
			saveTemplateGroupingResPo.setError(e.getMessage());
			return new ResponseEntity<SaveTemplateGroupingResPo>(saveTemplateGroupingResPo, HttpStatus.OK);
		} catch (BadRequestException e) {
			log.error(e.getMessage() + "BadRequestException");
			saveTemplateGroupingResPo.setMessage("Error while saving templategrouping");
			saveTemplateGroupingResPo.setError(e.getMessage());
			return new ResponseEntity<SaveTemplateGroupingResPo>(saveTemplateGroupingResPo,
					HttpStatus.BAD_REQUEST);
		} catch (Exception e) {
			log.error(e.getMessage() + "Exception");
			saveTemplateGroupingResPo.setMessage("Error while saving templategrouping");
			saveTemplateGroupingResPo.setError(e.getMessage());
			return new ResponseEntity<SaveTemplateGroupingResPo>(saveTemplateGroupingResPo,
					HttpStatus.INTERNAL_SERVER_ERROR);
		}

		return new ResponseEntity<SaveTemplateGroupingResPo>(saveTemplateGroupingResPo, new HttpHeaders(),
				HttpStatus.OK);

	}
	
	
	@ApiOperation(value = "Gets all template groupings")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/gettemplategroupings")
	public ResponseEntity<List<XxrTemplateRelationResPo>> getTemplateGroupings() throws ConvertRiteException {
		log.info("Start of getTemplateGroupings Method in Controller :::");
		List<XxrTemplateRelationResPo> xxrTemplateRelationResPo=new ArrayList<>();
		try {
			xxrTemplateRelationResPo = templateGroupingService.getTemplateGroupings();

		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<List<XxrTemplateRelationResPo>>(xxrTemplateRelationResPo, new HttpHeaders(), HttpStatus.OK);
	}

}
