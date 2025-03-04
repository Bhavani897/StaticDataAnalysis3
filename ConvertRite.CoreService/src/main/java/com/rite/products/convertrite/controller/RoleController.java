package com.rite.products.convertrite.controller;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

import com.rite.products.convertrite.Validations.Validations;
import com.rite.products.convertrite.exception.BadRequestException;
import com.rite.products.convertrite.exception.ConvertRiteException;
import com.rite.products.convertrite.exception.UnAuthorizedException;
import com.rite.products.convertrite.model.XxrRoles;
import com.rite.products.convertrite.po.RolesReqPo;
import com.rite.products.convertrite.po.XxrRolesResPo;
import com.rite.products.convertrite.service.RoleService;

@RestController
public class RoleController {

	private static final Logger log = org.slf4j.LoggerFactory.getLogger(RoleController.class);

	@Autowired
	RoleService roleService;

	@PostMapping("/saveroles")
	public ResponseEntity<XxrRolesResPo> saveRoles(@RequestBody RolesReqPo rolesReqPo,
			@RequestHeader("role") String role) {
		log.info("Start of saveRoles#####");
		XxrRolesResPo xxrRolesResPo = new XxrRolesResPo();
		try {
			if (role.equalsIgnoreCase("SuperUser")) {
				if (!Validations.isNullOrEmptyorWhiteSpace(rolesReqPo.getRoleName()))
					xxrRolesResPo = roleService.saveRoles(rolesReqPo);
				else
					throw new BadRequestException("roleName is Mandatory field");
			} else {
				throw new UnAuthorizedException("Only SuperUser has permissions to Register user");
			}
		} catch (UnAuthorizedException e) {
			log.error(e.getMessage());
			xxrRolesResPo
					.setMessage("Please contact System Administrator there is an error while processing the request");
			xxrRolesResPo.setError(e.getMessage());
			return new ResponseEntity<XxrRolesResPo>(xxrRolesResPo, new HttpHeaders(), HttpStatus.FORBIDDEN);
		} catch (BadRequestException e) {
			log.error(e.getMessage());
			xxrRolesResPo
					.setMessage("Please contact System Administrator there is an error while processing the request");
			xxrRolesResPo.setError(e.getMessage());
			return new ResponseEntity<XxrRolesResPo>(xxrRolesResPo, new HttpHeaders(), HttpStatus.BAD_REQUEST);
		} catch (Exception e) {
			log.error(e.getMessage());
			xxrRolesResPo
					.setMessage("Please contact System Administrator there is an error while processing the request");
			xxrRolesResPo.setError(e.getMessage());
			return new ResponseEntity<XxrRolesResPo>(xxrRolesResPo, new HttpHeaders(),
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<XxrRolesResPo>(xxrRolesResPo, new HttpHeaders(), HttpStatus.OK);
	}
	
	@GetMapping("/getroles")
	public ResponseEntity<List<XxrRoles>> getRoles(@RequestHeader("role") String role) throws Exception{
		log.info("Start of getRoles#####");
		List<XxrRoles> rolesLi=new ArrayList<>();
		try {
			if (role.equalsIgnoreCase("SuperUser")) 
			rolesLi=roleService.getRoles();
			else
				throw new UnAuthorizedException("Only SuperUser to get Roles");
		}catch(UnAuthorizedException e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(e.getMessage(), HttpStatus.FORBIDDEN);
		}catch(Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException("Please contact System Administrator there is an error while processing the request", HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<List<XxrRoles>>(rolesLi,new HttpHeaders(),HttpStatus.OK);
	}

}
