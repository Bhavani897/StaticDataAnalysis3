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
import org.springframework.web.bind.annotation.RestController;

import com.rite.products.convertrite.exception.ConvertRiteException;
import com.rite.products.convertrite.exception.UnAuthorizedException;
import com.rite.products.convertrite.po.RoleObjectLinkResPo;
import com.rite.products.convertrite.po.RoleObjectReqPo;
import com.rite.products.convertrite.po.RoleObjectRes;
import com.rite.products.convertrite.service.RoleObjectService;

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

@RestController
public class RoleObjectController {

	private static final Logger log = LoggerFactory.getLogger(RoleObjectController.class);

	@Autowired
	RoleObjectService roleObjectService;

	@ApiOperation(value = "This Api will save roleobjects")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error"),
			@ApiResponse(code = 400, message = "Bad Request") })
	@PostMapping("/saveroleobject")
	public ResponseEntity<RoleObjectLinkResPo> saveRoleObject(@RequestBody List<RoleObjectReqPo> roleObjectReqPo,
			@RequestHeader("role") String role) throws UnAuthorizedException {
		log.info("Start of saveRoleObject####");
		RoleObjectLinkResPo roleObjectLinkResPo = new RoleObjectLinkResPo();
		if (!role.equalsIgnoreCase("SuperUser"))
			throw new UnAuthorizedException("Only SuperUser has permissions");
		try {
			roleObjectLinkResPo = roleObjectService.saveRoleObject(roleObjectReqPo);
		} catch (UnAuthorizedException e) {
			log.error(e.getMessage());
			roleObjectLinkResPo
					.setMessage("Please contact System Administrator there is an error while processing the request");
			roleObjectLinkResPo.setError(e.getMessage());
			return new ResponseEntity<RoleObjectLinkResPo>(roleObjectLinkResPo, new HttpHeaders(),
					HttpStatus.FORBIDDEN);
		} catch (Exception e) {
			log.error(e.getMessage());
			roleObjectLinkResPo
					.setMessage("Please contact System Administrator there is an error while processing the request");
			roleObjectLinkResPo.setError(e.getMessage());
			return new ResponseEntity<RoleObjectLinkResPo>(roleObjectLinkResPo, new HttpHeaders(),
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<RoleObjectLinkResPo>(roleObjectLinkResPo, new HttpHeaders(), HttpStatus.OK);
	}

	@ApiOperation(value = "This Api will return roleobjects")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error"),
			@ApiResponse(code = 400, message = "Bad Request") })
	@GetMapping("/getroleobjects")
	public ResponseEntity<List<RoleObjectRes>> getRoleObjects(@RequestHeader("role") String role)
			throws ConvertRiteException {
		log.info("Start of getRoleObjects#####");
		List<RoleObjectRes> roleObjectResLi = new ArrayList<>();
		try {
			if (!role.equalsIgnoreCase("SuperUser"))
				throw new UnAuthorizedException("Only SuperUser has permissions");
			roleObjectResLi = roleObjectService.getRoleObjects();
		} catch (UnAuthorizedException e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(e.getMessage(), HttpStatus.FORBIDDEN);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<List<RoleObjectRes>>(roleObjectResLi, new HttpHeaders(), HttpStatus.OK);
	}

}
