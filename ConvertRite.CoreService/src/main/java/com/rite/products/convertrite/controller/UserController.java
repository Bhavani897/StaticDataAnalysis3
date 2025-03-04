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
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.rite.products.convertrite.Validations.Validations;
import com.rite.products.convertrite.exception.BadRequestException;
import com.rite.products.convertrite.exception.ConvertRiteException;
import com.rite.products.convertrite.exception.UnAuthorizedException;
import com.rite.products.convertrite.model.XxrUsers;
import com.rite.products.convertrite.po.UserRegistrationStatusRes;
import com.rite.products.convertrite.po.XxrUserResPo;
import com.rite.products.convertrite.service.UserService;

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

@RestController
public class UserController {

	private static final Logger log = LoggerFactory.getLogger(UserController.class);

	@Autowired
	UserService userService;

	@PostMapping("/registeruser")
	public ResponseEntity<XxrUserResPo> registerUser(@RequestBody XxrUsers xxrUsers,
			@RequestHeader("role") String role) {
		log.info("Start of registerUser ######");
		XxrUserResPo xxrUserResPo = new XxrUserResPo();
		try {
			if (role.equalsIgnoreCase("SuperUser")) {
				if (!Validations.isNullOrEmptyorWhiteSpace(xxrUsers.getName())
						&& !Validations.isNullOrEmptyorWhiteSpace(xxrUsers.getEmailId()))
					xxrUserResPo = userService.registerUser(xxrUsers);
				else
					throw new BadRequestException("name & emailId are Mandatory fields");
			} else {
				throw new UnAuthorizedException("Only SuperUser has permissions to Register user");
			}
		} catch (UnAuthorizedException e) {
			log.error(e.getMessage());
			xxrUserResPo
					.setMessage("Please contact System Administrator there is an error while processing the request");
			xxrUserResPo.setError(e.getMessage());
			return new ResponseEntity<XxrUserResPo>(xxrUserResPo, new HttpHeaders(), HttpStatus.FORBIDDEN);
		} catch (BadRequestException e) {
			log.error(e.getMessage());
			xxrUserResPo
					.setMessage("Please contact System Administrator there is an error while processing the request");
			xxrUserResPo.setError(e.getMessage());
			return new ResponseEntity<XxrUserResPo>(xxrUserResPo, new HttpHeaders(), HttpStatus.BAD_REQUEST);
		} catch (Exception e) {
			log.error(e.getMessage());
			xxrUserResPo
					.setMessage("Please contact System Administrator there is an error while processing the request");
			xxrUserResPo.setError(e.getMessage());
			return new ResponseEntity<XxrUserResPo>(xxrUserResPo, new HttpHeaders(), HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<XxrUserResPo>(xxrUserResPo, new HttpHeaders(), HttpStatus.OK);
	}
	
	@ApiOperation(value = "Check User Registration Status")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/userregistrationStatus")
	public ResponseEntity<UserRegistrationStatusRes> userRegistrationStatus(@RequestParam("userName") String userName)
			throws ConvertRiteException {
		log.info("Start of userRegistrationStatus ######");
		UserRegistrationStatusRes userRegistrationStatusRes = new UserRegistrationStatusRes();
		try {
			userRegistrationStatusRes = userService.userRegistrationStatus(userName);
		}catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<UserRegistrationStatusRes>(userRegistrationStatusRes, new HttpHeaders(),
				HttpStatus.OK);
	}
	
	
	@ApiOperation(value = "Check User Registration Status by emailId")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/userregistrationstatusbyemail")
	public ResponseEntity<UserRegistrationStatusRes> userRegistrationStatusByEmail(@RequestParam("email") String email)
			throws ConvertRiteException {
		log.info("Start of userRegistrationStatus ######");
		UserRegistrationStatusRes userRegistrationStatusRes = new UserRegistrationStatusRes();
		try {
			userRegistrationStatusRes = userService.userRegistrationStatusByEmail(email);
		}catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<UserRegistrationStatusRes>(userRegistrationStatusRes, new HttpHeaders(),
				HttpStatus.OK);
	}
	
	@ApiOperation(value = "Get users info")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/getusers")
	public ResponseEntity<List<XxrUsers>> getUsers()
			throws ConvertRiteException {
		log.info("Start of getUsers ######");
		List<XxrUsers> usersLi=new ArrayList<>();
		try {
			usersLi = userService.getUsers();
		}catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<List<XxrUsers>>(usersLi, new HttpHeaders(),
				HttpStatus.OK);
	}

}
