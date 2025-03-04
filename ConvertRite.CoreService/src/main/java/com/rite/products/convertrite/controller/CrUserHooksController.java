package com.rite.products.convertrite.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.rite.products.convertrite.po.CrUserHooksCreateReqPo;
import com.rite.products.convertrite.service.CrUserHooksService;

import io.swagger.annotations.ApiOperation;


@RestController
@RequestMapping("/api/convertritecore")
@Slf4j
public class CrUserHooksController {

	@Autowired
	CrUserHooksService crUserHooksService;
	
	@ApiOperation(value = "API to return all User Hooks")
	@GetMapping("/getAllUserHooks")
	public ResponseEntity<?> getAllUserHooks() {
		try {
		return new ResponseEntity<>(crUserHooksService.getAllUserHooks(),HttpStatus.OK);
		}catch(Exception e) {
			log.error(e.getMessage());
			return new ResponseEntity<>(e.getCause(),HttpStatus.EXPECTATION_FAILED);
		}
	}
	
	@ApiOperation(value = "API to return User Hook By Id")
	@GetMapping("/getUserHookById/{hookId}")
	public ResponseEntity<?> getUserHokkById(@PathVariable Long hookId){
		try {
			return new ResponseEntity<>(crUserHooksService.getUserHookById(hookId),HttpStatus.OK);
			}catch(Exception e) {
			    log.error(e.getMessage());
				return new ResponseEntity<>(e.getCause(),HttpStatus.EXPECTATION_FAILED);
			}
	}
	
	@ApiOperation(value = "API to Save User Hook By Id")
	@PostMapping("/saveUserHook")
	public ResponseEntity<?> deleteUserHokkById(@RequestBody CrUserHooksCreateReqPo createReqPo){
		try {
			return new ResponseEntity<>(crUserHooksService.saveUserHook(createReqPo),HttpStatus.OK);
			}catch(Exception e) {
			    log.error(e.getMessage());
				return new ResponseEntity<>(e.getCause(),HttpStatus.EXPECTATION_FAILED);
			}
	}
	
	@ApiOperation(value = "API to delete User Hook By Id")
	@DeleteMapping("/delteUserHookById/{hookId}")
	public ResponseEntity<?> deleteUserHookById(@PathVariable Long hookId){
		try {
			return new ResponseEntity<>(crUserHooksService.deleteUserHook(hookId),HttpStatus.OK);
			}catch(Exception e) {
			    log.error(e.getMessage());
				return new ResponseEntity<>(e.getCause(),HttpStatus.EXPECTATION_FAILED);
			}
	}
}
