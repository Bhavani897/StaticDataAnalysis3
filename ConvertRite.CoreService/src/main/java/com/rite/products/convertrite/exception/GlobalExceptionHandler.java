package com.rite.products.convertrite.exception;

import java.util.stream.Collectors;
import javax.validation.ConstraintViolationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingRequestHeaderException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.server.ResponseStatusException;
import com.rite.products.convertrite.po.BasicResponsePo;

@ControllerAdvice
public class GlobalExceptionHandler {
	@Autowired
	ConvertException convertException;
	private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

	@ExceptionHandler(value = ConvertRiteException.class)
	public ResponseEntity<ConvertException> handleProcessException(ConvertRiteException e) {
		log.info("entering GlobalExceptionHandler#########");
		convertException.setErrorMessage(e.getErrorMessage());
		convertException.setHttpStatus(e.getErrorCode());
		return new ResponseEntity<ConvertException>(convertException, e.getErrorCode());
	}

	@ExceptionHandler(MethodArgumentNotValidException.class)
	public ResponseEntity<BasicResponsePo> handleMethodArgumentNotValidExceptions(MethodArgumentNotValidException ex) {
		BasicResponsePo responsePo = new BasicResponsePo();
		String errorMessage = ex.getBindingResult().getFieldErrors().stream().map(error -> {
			return String.format("%s=%s", error.getField(), error.getDefaultMessage());
		}).collect(Collectors.joining(", "));
		responsePo.setError("Bad Request");
		responsePo.setMessage(errorMessage);
		return new ResponseEntity<BasicResponsePo>(responsePo, HttpStatus.BAD_REQUEST);
	}

	@ExceptionHandler(MissingServletRequestParameterException.class)
	public ResponseEntity<BasicResponsePo> handleMissingParams(MissingServletRequestParameterException ex) {
		BasicResponsePo responsePo = new BasicResponsePo();
		responsePo.setError("Bad Request");
		responsePo.setMessage(ex.getParameterName() + " is required");
		return new ResponseEntity<BasicResponsePo>(responsePo, HttpStatus.BAD_REQUEST);
	}

	@ExceptionHandler(ResponseStatusException.class)
	public ResponseEntity<BasicResponsePo> handleResponseStatusException(ResponseStatusException ex) {
		BasicResponsePo responsePo = new BasicResponsePo();
		responsePo.setError(ex.getReason());
		responsePo.setMessage("Resource not found");
		return new ResponseEntity<BasicResponsePo>(responsePo, HttpStatus.NOT_FOUND);
	}

	@ExceptionHandler(Exception.class)
	public ResponseEntity<BasicResponsePo> handleAllExceptions(Exception ex) {
		BasicResponsePo responsePo = new BasicResponsePo();
		responsePo.setError(ex.getMessage());
		responsePo.setMessage("Please contact System Administrator, there is an error while processing the request");
		return new ResponseEntity<BasicResponsePo>(responsePo, HttpStatus.INTERNAL_SERVER_ERROR);
	}

	@ExceptionHandler(ConstraintViolationException.class)
	public ResponseEntity<BasicResponsePo> handleConstraintViolationException(ConstraintViolationException e) {
		// Handle ConstraintViolationException and return a custom response
		BasicResponsePo responsePo = new BasicResponsePo();
		StringBuilder errorMessage = new StringBuilder("Validation error(s): ");
		e.getConstraintViolations().forEach(violation -> {
			errorMessage.append(violation.getPropertyPath() + "=" + violation.getMessage()).append("; ");
		});
		responsePo.setError("Bad Request");
		responsePo.setMessage(errorMessage.toString());
		return new ResponseEntity<BasicResponsePo>(responsePo, HttpStatus.BAD_REQUEST);
	}
	@ExceptionHandler(ValidationException.class)
	public ResponseEntity<BasicResponsePo> handleValidationException(ValidationException e) {
		// Handle ConstraintViolationException and return a custom response
		BasicResponsePo responsePo = new BasicResponsePo();
		responsePo.setError("Validation Error");
		responsePo.setMessage(e.getMessage());
		return new ResponseEntity<BasicResponsePo>(responsePo, HttpStatus.OK);
	}
	@ExceptionHandler(MissingRequestHeaderException.class)
	public ResponseEntity<BasicResponsePo> handleMissingRequestHeaderException(MissingRequestHeaderException e) {
		// Handle MissingRequestHeaderException and return a custom response
		BasicResponsePo responsePo = new BasicResponsePo();
		responsePo.setError("Missing Request Header");
		responsePo.setMessage("Required header is missing: " + e.getHeaderName());
		return new ResponseEntity<BasicResponsePo>(responsePo, HttpStatus.BAD_REQUEST);
	}
}