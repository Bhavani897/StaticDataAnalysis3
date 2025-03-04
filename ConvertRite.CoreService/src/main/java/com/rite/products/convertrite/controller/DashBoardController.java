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
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.rite.products.convertrite.Validations.Validations;
import com.rite.products.convertrite.exception.BadRequestException;
import com.rite.products.convertrite.exception.ConvertRiteException;
import com.rite.products.convertrite.po.DashBoardResPo;
import com.rite.products.convertrite.po.GantChatResPo;
import com.rite.products.convertrite.service.DashBoardService;

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

@RestController
public class DashBoardController {
	private static final Logger log = LoggerFactory.getLogger(DashBoardController.class);
	@Autowired
	private DashBoardService dashboardService;

	@ApiOperation(value = "This Api will give Task information based on user ")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/gettasks")
	public ResponseEntity<List<DashBoardResPo>> getTaskByUser(@RequestParam(value = "user") String user)
			throws BadRequestException, ConvertRiteException {
		log.info("getTaskByUser method in DashBoardController##");
		List<DashBoardResPo> resPoLi = new ArrayList<>();
		try {
			if (!Validations.isNullOrEmptyorWhiteSpace(user))
				resPoLi = dashboardService.getTask(user);
			else
				throw new BadRequestException("userName is Mandatory field");
		}catch(BadRequestException be) {
			 throw new ConvertRiteException(
					be.getMessage(),
					HttpStatus.OK);
		
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}

		return new ResponseEntity<List<DashBoardResPo>>(resPoLi, new HttpHeaders(), HttpStatus.OK);

	}
	@ApiOperation(value = "This Api will give Task information on GanttChat")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/getchat")
	public ResponseEntity<List<GantChatResPo>> getChatByUser(@RequestParam("user") String user) throws ConvertRiteException{
		log.info("getChatByUser method in DashBoardController###");
		List<GantChatResPo> resPo = new ArrayList<>();
		try {
			if(!Validations.isNullOrEmptyorWhiteSpace(user))
				resPo = dashboardService.getGanttChat(user);
			else
				throw new BadRequestException("UserName is Mandatory field");
		}catch(BadRequestException be) {
			 throw new ConvertRiteException(
					be.getMessage(),
					HttpStatus.OK);
		
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<List<GantChatResPo>>(resPo, HttpStatus.OK);
	}
	
	

}
