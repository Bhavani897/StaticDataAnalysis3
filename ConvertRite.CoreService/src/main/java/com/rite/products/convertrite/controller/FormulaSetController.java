package com.rite.products.convertrite.controller;

import java.util.ArrayList;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

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
import com.rite.products.convertrite.exception.ValidationException;
import com.rite.products.convertrite.model.XxrFormulaSetsView;
import com.rite.products.convertrite.po.CloudColumnsPo;
import com.rite.products.convertrite.po.SaveFormulaColumnsPo;
import com.rite.products.convertrite.po.SaveFormulaColumnsResPo;
import com.rite.products.convertrite.po.SaveFormulaSetHeaderPo;
import com.rite.products.convertrite.po.SaveFormulaSetHeadersResPo;
import com.rite.products.convertrite.po.SaveFormulaSetTablesPo;
import com.rite.products.convertrite.po.SaveFormulaSetTablesResPo;
import com.rite.products.convertrite.po.SourceColumnsPo;
import com.rite.products.convertrite.po.TestDataResPo;
import com.rite.products.convertrite.po.TestSqlSyntaxResPo;
import com.rite.products.convertrite.po.XxrFormulaColumnsResPo;
import com.rite.products.convertrite.po.XxrFormulaSetTablesResPo;
import com.rite.products.convertrite.po.XxrFormulaSetsResPo;
import com.rite.products.convertrite.service.FormulaSetService;

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

@RestController
public class FormulaSetController {

	private static final Logger log = LoggerFactory.getLogger(FormulaSetController.class);

	
	@Autowired
	FormulaSetService formulaSetService ;

	@ApiOperation(value = "Get Formula Sets based on formula setId")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/getformulasetbyid")
	public ResponseEntity<List<XxrFormulaSetsResPo>> getFormulaSetsById(@RequestParam("formulaSetId") Long formulaSetId)
			throws ConvertRiteException {
		log.info("Start of getFormulaSetsById Method in FormulaSetController :::");
		List<XxrFormulaSetsResPo> fromulaSetList = new ArrayList<>();
		try {
			fromulaSetList = formulaSetService.getFormulaSetsById(formulaSetId);

		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<List<XxrFormulaSetsResPo>>(fromulaSetList, new HttpHeaders(), HttpStatus.OK);
	}

	/*
	 * @ApiOperation(value = "Get  All Formula Sets")
	 * 
	 * @ApiResponses(value = { @ApiResponse(code = 200, message =
	 * "Successful Response"),
	 * 
	 * @ApiResponse(code = 500, message = "Server Side Error") })
	 * 
	 * @GetMapping("/getformulasets") public
	 * ResponseEntity<List<XxrFormulaSetsResPo>> getFormulaSets() throws
	 * ConvertRiteException {
	 * log.info("Start of getFormulaSets Method in FormulaSetController :::");
	 * List<XxrFormulaSetsResPo> fromulaSetList = new ArrayList<>(); try {
	 * fromulaSetList = formulaSetService.getFormulaSets();
	 * 
	 * } catch (Exception e) { log.error(e.getMessage()); throw new
	 * ConvertRiteException(
	 * "Please contact System Administrator there is an error while processing the request"
	 * , HttpStatus.INTERNAL_SERVER_ERROR); } return new
	 * ResponseEntity<List<XxrFormulaSetsResPo>>(fromulaSetList, new HttpHeaders(),
	 * HttpStatus.OK); }
	 */
	
	@ApiOperation(value = "Get  All Formula Sets")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/getformulasets")
	public ResponseEntity<List<XxrFormulaSetsView>> getFormulaSets(@RequestHeader("roleid") Long roleId) throws ConvertRiteException {
		log.info("Start of getFormulaSets Method in FormulaSetController :::");
		List<XxrFormulaSetsView> fromulaSetList = new ArrayList<>();
		try {
			fromulaSetList = formulaSetService.getFormulaSets(roleId);

		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<List<XxrFormulaSetsView>>(fromulaSetList, new HttpHeaders(), HttpStatus.OK);
	}
	
	@ApiOperation(value = "Get Formula Sets based on objectcode ")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/getformulasetsbyobjectid")
	public ResponseEntity<List<XxrFormulaSetsResPo>> getFormulaSetsByobjectId(@RequestParam("objectId") Long objectId) throws ConvertRiteException {
		log.info("Start of getFormulaSets Method in FormulaSetController :::");
		List<XxrFormulaSetsResPo> fromulaSetList = new ArrayList<>();
		try {
			fromulaSetList = formulaSetService.getFormulaSetsByobjectId(objectId);

		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<List<XxrFormulaSetsResPo>>(fromulaSetList, new HttpHeaders(), HttpStatus.OK);
	}
	
	
	@ApiOperation(value = "This Api test Syntax of sql query valid or not")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/testsqlsyntax")
	public ResponseEntity<TestSqlSyntaxResPo> testSqlSyntax(@RequestParam("sqlQuery") String sqlQuery) throws ConvertRiteException {
		log.info("Start of testSqlSyntax Method in FormulaSetController :::");
		TestSqlSyntaxResPo testSqlSyntaxResPo=new TestSqlSyntaxResPo();
		try {
			testSqlSyntaxResPo = formulaSetService.testSqlSyntax(sqlQuery);

		} catch (Exception e) {
			log.error(e.getMessage());
			testSqlSyntaxResPo.setMessage("Please contact System Administrator there is an error while processing the request");
			testSqlSyntaxResPo.setError(e.getMessage());
			return new ResponseEntity<TestSqlSyntaxResPo>(testSqlSyntaxResPo,
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<TestSqlSyntaxResPo>(testSqlSyntaxResPo, new HttpHeaders(), HttpStatus.OK);
	}

	

	@ApiOperation(value = "This Api test's data is valid or not")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/testsqldata")
	public ResponseEntity<TestDataResPo> testSqlData(@RequestParam("sqlQuery") String sqlQuery) throws ConvertRiteException {
		log.info("Start of testSqlData Method in FormulaSetController :::");
		TestDataResPo testDataResPo=new TestDataResPo();
		try {
			testDataResPo = formulaSetService.testSqlData(sqlQuery);

		} catch (Exception e) {
			log.error(e.getMessage());
			testDataResPo.setMessage("Please contact System Administrator there is an error while processing the request");
			testDataResPo.setError(e.getMessage());
			return new ResponseEntity<TestDataResPo>(testDataResPo,
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<TestDataResPo>(testDataResPo, new HttpHeaders(), HttpStatus.OK);
	}

	@ApiOperation(value = "This Api is to save formulaset header")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error"),
			@ApiResponse(code = 400, message = "Bad Request") })
	@PostMapping("/saveformulasetheader")
	public ResponseEntity<SaveFormulaSetHeadersResPo> saveFormulaSetHeaders(
			@RequestBody List<SaveFormulaSetHeaderPo> saveFormulaSetHeaderPo,HttpServletRequest request) {
		log.info("Start of saveFormulaSetHeaders Method in Controller ###");

		SaveFormulaSetHeadersResPo saveFormulaSetHeadersResPo = new SaveFormulaSetHeadersResPo();
		try {

			saveFormulaSetHeadersResPo = formulaSetService.saveFormulaSetHeaders(saveFormulaSetHeaderPo,request);

		} catch (ValidationException e) {
			log.error(e.getMessage());
			saveFormulaSetHeadersResPo.setMessage(e.getMessage());
			saveFormulaSetHeadersResPo.setError("Error while saving  FormulaSet Headers");
			return new ResponseEntity<SaveFormulaSetHeadersResPo>(saveFormulaSetHeadersResPo, HttpStatus.OK);
		} catch (BadRequestException e) {
			log.error(e.getMessage());
			saveFormulaSetHeadersResPo.setMessage(e.getMessage());
			saveFormulaSetHeadersResPo.setError("Error while saving  FormulaSet Headers");	
			return new ResponseEntity<SaveFormulaSetHeadersResPo>(saveFormulaSetHeadersResPo, HttpStatus.BAD_REQUEST);
		} catch (Exception e) {
			log.error(e.getMessage());
			saveFormulaSetHeadersResPo.setMessage("Please contact System Administrator there is an error while processing the request");
			saveFormulaSetHeadersResPo.setError(e.getMessage());
			return new ResponseEntity<SaveFormulaSetHeadersResPo>(saveFormulaSetHeadersResPo,
					HttpStatus.INTERNAL_SERVER_ERROR);
		}

		return new ResponseEntity<SaveFormulaSetHeadersResPo>(saveFormulaSetHeadersResPo, new HttpHeaders(),
				HttpStatus.OK);

	}

	@ApiOperation(value = "Get  All FormulaSet tables")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/getformulasettables")
	public ResponseEntity<List<XxrFormulaSetTablesResPo>> getFormulaSetTables(
			@RequestParam("formulaSetId") Long formulaSetId) throws ConvertRiteException {
		log.info("Start of getFormulaSetTables Method in FormulaSetController :::");
		List<XxrFormulaSetTablesResPo> fromulaSetTablesList = new ArrayList<>();
		try {
			fromulaSetTablesList = formulaSetService.getFormulaSetTables(formulaSetId);

		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<List<XxrFormulaSetTablesResPo>>(fromulaSetTablesList, new HttpHeaders(),
				HttpStatus.OK);
	}

	@ApiOperation(value = "This Api is to save formulaset tables")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error"),
			@ApiResponse(code = 400, message = "Bad Request") })
	@PostMapping("/saveformulasettables")
	public ResponseEntity<SaveFormulaSetTablesResPo> saveFormulaSetTables(
			@RequestBody List<SaveFormulaSetTablesPo> saveFormulaSetTablesPo,HttpServletRequest request) {
		log.info("Start of saveFormulaSetTables Method in Controller ###");

		SaveFormulaSetTablesResPo saveFormulaSetTablesResPo = new SaveFormulaSetTablesResPo();
		try {

			saveFormulaSetTablesResPo = formulaSetService.saveFormulaSetTables(saveFormulaSetTablesPo,request);

		} catch (ValidationException e) {
			log.error(e.getMessage());
			saveFormulaSetTablesResPo.setMessage(e.getMessage());
			saveFormulaSetTablesResPo.setError("Error while saving  FormulaSet Tables");
			return new ResponseEntity<SaveFormulaSetTablesResPo>(saveFormulaSetTablesResPo, HttpStatus.OK);
		}catch (BadRequestException e) {
			log.error(e.getMessage());
			saveFormulaSetTablesResPo.setMessage(e.getMessage());
			saveFormulaSetTablesResPo.setError("Error while saving  FormulaSet Tables");
			return new ResponseEntity<SaveFormulaSetTablesResPo>(saveFormulaSetTablesResPo, HttpStatus.BAD_REQUEST);
		}catch (Exception e) {
			log.error(e.getMessage());
			saveFormulaSetTablesResPo.setMessage("Please contact System Administrator there is an error while processing the request");
			saveFormulaSetTablesResPo.setError(e.getMessage());
			return new ResponseEntity<SaveFormulaSetTablesResPo>(saveFormulaSetTablesResPo, HttpStatus.INTERNAL_SERVER_ERROR);
		}

		return new ResponseEntity<SaveFormulaSetTablesResPo>(saveFormulaSetTablesResPo, new HttpHeaders(),
				HttpStatus.OK);

	}

	@ApiOperation(value = "Get  All Formula Columns")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/getformulacolumns")
	public ResponseEntity<List<XxrFormulaColumnsResPo>> getFormulaColumns(
			@RequestParam("formulaSetId") Long formulaSetId, @RequestParam("formulaTableId") Long formulaTableId)
			throws ConvertRiteException {
		log.info("Start of getFormulaColumns Method in FormulaSetController :::");
		List<XxrFormulaColumnsResPo> formulaColumnsList = new ArrayList<>();
		try {
			formulaColumnsList = formulaSetService.getFormulaColumns(formulaSetId, formulaTableId);

		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<List<XxrFormulaColumnsResPo>>(formulaColumnsList, new HttpHeaders(), HttpStatus.OK);
	}

	@ApiOperation(value = "This Api is to save formula columns")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error"),
			@ApiResponse(code = 400, message = "Bad Request") })
	@PostMapping("/saveformulacolumns")
	public ResponseEntity<SaveFormulaColumnsResPo> saveFormulaColumns(
			@RequestBody List<SaveFormulaColumnsPo> saveFormulaColumnsPo,HttpServletRequest request) {
		log.info("Start of saveFormulaColumns Method in Controller ###");

		SaveFormulaColumnsResPo saveFormulaColumnsResPo = new SaveFormulaColumnsResPo();
		try {

			saveFormulaColumnsResPo = formulaSetService.saveFormulaColumns(saveFormulaColumnsPo,request);

		} catch (ValidationException e) {
			log.error(e.getMessage());
			saveFormulaColumnsResPo.setMessage(e.getMessage());
			saveFormulaColumnsResPo.setError("Error while saving  Formula Columns");			
			return new ResponseEntity<SaveFormulaColumnsResPo>(saveFormulaColumnsResPo, HttpStatus.OK);
		}catch (BadRequestException e) {
			log.error(e.getMessage());
			saveFormulaColumnsResPo.setMessage(e.getMessage());
			saveFormulaColumnsResPo.setError("Error while saving  Formula Columns");		
			return new ResponseEntity<SaveFormulaColumnsResPo>(saveFormulaColumnsResPo, HttpStatus.BAD_REQUEST);
		}catch (Exception e) {
			log.error(e.getMessage());
			saveFormulaColumnsResPo.setMessage("Please contact System Administrator there is an error while processing the request");
			saveFormulaColumnsResPo.setError(e.getMessage());
			return new ResponseEntity<SaveFormulaColumnsResPo>(saveFormulaColumnsResPo, HttpStatus.INTERNAL_SERVER_ERROR);
		}

		return new ResponseEntity<SaveFormulaColumnsResPo>(saveFormulaColumnsResPo, new HttpHeaders(), HttpStatus.OK);

	}

	@ApiOperation(value = "Gets all the SourceObjects without any filteration")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/getallsourceobjects")
	public ResponseEntity<String[]> getAllSourceObjects() throws ConvertRiteException {
		log.info("Start of getAllSourceObjects Method in Controller :::");
		String[] viewNames = {};
		try {
			viewNames = formulaSetService.getAllSourceObjects();

		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<String[]>(viewNames, new HttpHeaders(), HttpStatus.OK);
	}

	@ApiOperation(value = "Gets all the SourceColumns based on the viewname")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/getsourcecolumnsbyview")
	public ResponseEntity<List<SourceColumnsPo>> getSourceColumns(@RequestParam("viewName") String viewName)
			throws ConvertRiteException {
		log.info("Start of getSourceColumns Method in FormulaSetController :::");
		List<SourceColumnsPo> sourceTemplateColumns = new ArrayList<>();
		try {
			if (!Validations.isNullOrEmpty(viewName))
				sourceTemplateColumns = formulaSetService.getSourceColumns(viewName);
			/*
			 * else throw new ConvertRiteException("Missing viewName in Request",
			 * HttpStatus.BAD_REQUEST);
			 */
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<List<SourceColumnsPo>>(sourceTemplateColumns, new HttpHeaders(), HttpStatus.OK);
	}
	
	@ApiOperation(value = "This Api is for copy formulaset")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error"),
			@ApiResponse(code = 400, message = "Bad Request") })
	@PostMapping("/copyformulaset")
	public ResponseEntity<SaveFormulaSetHeadersResPo> copyFormulaSetHeaders(@RequestParam("newFormulaSetName") String newFormulaSetName, @RequestParam("formulaSetId") Long formulaSetId,
			@RequestParam("podId") Long podId,HttpServletRequest request) {
		log.info("Start of copyFormulaSetHeaders Method in Controller ###");

		SaveFormulaSetHeadersResPo saveFormulaSetHeadersResPo = new SaveFormulaSetHeadersResPo();
		try {

			saveFormulaSetHeadersResPo = formulaSetService.copyFormulaSet(newFormulaSetName,formulaSetId,podId,request);

		} catch (Exception e) {
			log.error(e.getMessage());
			saveFormulaSetHeadersResPo.setMessage("Please contact System Administrator there is an error while processing the request");
			saveFormulaSetHeadersResPo.setError(e.getMessage());
			return new ResponseEntity<SaveFormulaSetHeadersResPo>(saveFormulaSetHeadersResPo,
					HttpStatus.INTERNAL_SERVER_ERROR);
		}

		return new ResponseEntity<SaveFormulaSetHeadersResPo>(saveFormulaSetHeadersResPo, new HttpHeaders(),
				HttpStatus.OK);

	}

	/*
	 * @ApiOperation(value = "This Api return all SourceTableNames and TableId's")
	 * 
	 * @ApiResponses(value = { @ApiResponse(code = 200, message =
	 * "Successful Response"),
	 * 
	 * @ApiResponse(code = 500, message = "Server Side Error") })
	 * 
	 * @GetMapping("/getsourcetablenames") public
	 * ResponseEntity<List<SourceTablesPo>>
	 * getSourceTableNames(@RequestParam("objectId") Long objectId) throws
	 * ConvertRiteException {
	 * log.info("Start of getSourceTableNames Method in Controller ###");
	 * 
	 * List<SourceTablesPo> sourceTablesList = new ArrayList<>(); try { if
	 * (objectId!=null) sourceTablesList =
	 * formulaSetService.getSourceTableNames(objectId);
	 * 
	 * else throw new ConvertRiteException("objectCode is Mandatory in the Request",
	 * HttpStatus.BAD_REQUEST);
	 * 
	 * } catch (Exception e) { log.error(e.getMessage()); throw new
	 * ConvertRiteException(
	 * "Please contact System Administrator there is an error while processing the request"
	 * , HttpStatus.INTERNAL_SERVER_ERROR); }
	 * 
	 * return new ResponseEntity<List<SourceTablesPo>>(sourceTablesList,
	 * HttpStatus.OK); }
	 */

	/*
	 * @ApiOperation(value = "This Api return SourceColumns based on SourceTableId")
	 * 
	 * @ApiResponses(value = { @ApiResponse(code = 200, message =
	 * "Successful Response"),
	 * 
	 * @ApiResponse(code = 500, message = "Server Side Error"),
	 * 
	 * @ApiResponse(code = 400, message = "Bad Request") })
	 * 
	 * @GetMapping("/getsourcecolumns") public ResponseEntity<List<SourceColumnsPo>>
	 * getSourceColumns(@RequestParam("sourceTableId") Long sourceTableId) throws
	 * ConvertRiteException {
	 * log.info("Start of getSourceColumnsById Method in FormulaSetController ###");
	 * log.info("sourceTableId::::::::" + sourceTableId);
	 * 
	 * List<SourceColumnsPo> sourceColumnsList = new ArrayList<>(); try { if
	 * (sourceTableId != null) sourceColumnsList =
	 * formulaSetService.getSourceColumns(sourceTableId); else throw new
	 * ConvertRiteException("Missing sourceTableId in Request",
	 * HttpStatus.BAD_REQUEST); } catch (Exception e) { log.error(e.getMessage());
	 * throw new ConvertRiteException(
	 * "Please contact System Administrator there is an error while processing the request"
	 * , HttpStatus.INTERNAL_SERVER_ERROR); }
	 * 
	 * return new ResponseEntity<List<SourceColumnsPo>>(sourceColumnsList,
	 * HttpStatus.OK); }
	 */

	@ApiOperation(value = "Gets all the CloudColumns based on ObjectId")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/getcloudcolumns")
	public ResponseEntity<List<CloudColumnsPo>> getCloudColumns(@RequestParam("objectId") Long objectId)
			throws ConvertRiteException {
		log.info("Start of getCloudColumns Method in FormulaSetController :::");
		List<CloudColumnsPo> cloudColumnsList = new ArrayList<>();
		try {
			if (objectId != null)
				cloudColumnsList = formulaSetService.getCloudColumns(objectId);
			/*
			 * else throw new ConvertRiteException("Missing ObjectId in Request",
			 * HttpStatus.BAD_REQUEST);
			 */
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
		return new ResponseEntity<List<CloudColumnsPo>>(cloudColumnsList, new HttpHeaders(), HttpStatus.OK);
	}

}
