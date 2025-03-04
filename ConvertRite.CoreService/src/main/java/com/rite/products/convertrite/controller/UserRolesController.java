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

import com.rite.products.convertrite.exception.BadRequestException;
import com.rite.products.convertrite.exception.ConvertRiteException;
import com.rite.products.convertrite.exception.UnAuthorizedException;
import com.rite.products.convertrite.po.RolesResPo;
import com.rite.products.convertrite.po.SaveUserRolesResPo;
import com.rite.products.convertrite.po.UsersRolesReqPo;
import com.rite.products.convertrite.po.XxrUserRolesRes;
import com.rite.products.convertrite.service.UserRoleService;

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

@RestController
public class UserRolesController {

	private static final Logger log = LoggerFactory.getLogger(UserRolesController.class);

	@Autowired
	UserRoleService userRoleService;

	@ApiOperation(value = "This Api will save userroles")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error"),
			@ApiResponse(code = 400, message = "Bad Request") })
	@PostMapping("/saveuserroles")
	public ResponseEntity<SaveUserRolesResPo> saveUserRoles(@RequestBody List<UsersRolesReqPo> userRolesReqPo,
			@RequestHeader("role") String role) {
		log.info("Start of saveUserRoles#####");
		SaveUserRolesResPo saveUserRolesResPo = new SaveUserRolesResPo();
		try {
			if (role.equalsIgnoreCase("SuperUser")) {
				saveUserRolesResPo = userRoleService.saveUserRoles(userRolesReqPo);
			} else
				throw new UnAuthorizedException("Only SuperUser has permissions");
		} catch (BadRequestException e) {
			log.error(e.getMessage());
			saveUserRolesResPo
					.setMessage("Please contact System Administrator there is an error while processing the request");
			saveUserRolesResPo.setError(e.getMessage());
			return new ResponseEntity<SaveUserRolesResPo>(saveUserRolesResPo, new HttpHeaders(), HttpStatus.FORBIDDEN);
		} catch (UnAuthorizedException e) {
			log.error(e.getMessage());
			saveUserRolesResPo
					.setMessage("Please contact System Administrator there is an error while processing the request");
			saveUserRolesResPo.setError(e.getMessage());
			return new ResponseEntity<SaveUserRolesResPo>(saveUserRolesResPo, new HttpHeaders(), HttpStatus.FORBIDDEN);
		} catch (Exception e) {
			log.error(e.getMessage());
			saveUserRolesResPo
					.setMessage("Please contact System Administrator there is an error while processing the request");
			saveUserRolesResPo.setError(e.getMessage());
			return new ResponseEntity<SaveUserRolesResPo>(saveUserRolesResPo, new HttpHeaders(),
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<SaveUserRolesResPo>(saveUserRolesResPo, new HttpHeaders(), HttpStatus.OK);
	}

	@ApiOperation(value = "This Api will return userroles")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error"),
			@ApiResponse(code = 400, message = "Bad Request") })
	@GetMapping("/getuserroles")
	public List<XxrUserRolesRes> getUserRoles(@RequestHeader("role") String role) throws ConvertRiteException {
		log.info("Start of getUserRoles ##########");
		List<XxrUserRolesRes> userRolesLi = new ArrayList<>();
		try {
			if (role.equalsIgnoreCase("SuperUser"))
				userRolesLi = userRoleService.getUserRoles();
			else
				throw new UnAuthorizedException("Only SuperUser to get Roles");
		} catch (UnAuthorizedException e) {
			throw new ConvertRiteException(e.getMessage(), HttpStatus.FORBIDDEN);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return userRolesLi;
	}

	@ApiOperation(value = "This Api will return roles based on userName")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error"),
			@ApiResponse(code = 400, message = "Bad Request") })
	@GetMapping("/getrolesbyusername")
	public ResponseEntity<List<RolesResPo>> getRolesByUserName(@RequestParam("userName") String userName)
			throws ConvertRiteException {
		log.info("Start of getRolesByUserName ##########");
		List<RolesResPo> rolesResLi = new ArrayList<>();
		try {
			rolesResLi = userRoleService.getRolesByUserName(userName);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<List<RolesResPo>>(rolesResLi, HttpStatus.OK);
	}
	
	@ApiOperation(value = "This Api will return roles based on userName")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error"),
			@ApiResponse(code = 400, message = "Bad Request") })
	@GetMapping("/getrolesbyemail")
	public ResponseEntity<List<RolesResPo>> getRolesByEmailId(@RequestParam("emailId") String emailId)
			throws ConvertRiteException {
		log.info("Start of getRolesByEmailId ##########");
		List<RolesResPo> rolesResLi = new ArrayList<>();
		try {
			rolesResLi = userRoleService.getRolesByEmailId(emailId);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<List<RolesResPo>>(rolesResLi, HttpStatus.OK);
	}

}
