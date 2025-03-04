package com.rite.products.convertrite.service;

import java.util.List;

import javax.servlet.http.HttpServletRequest;

import com.rite.products.convertrite.model.XxrFormulaSetsView;
import com.rite.products.convertrite.po.CloudColumnsPo;
import com.rite.products.convertrite.po.SaveFormulaColumnsPo;
import com.rite.products.convertrite.po.SaveFormulaColumnsResPo;
import com.rite.products.convertrite.po.SaveFormulaSetHeaderPo;
import com.rite.products.convertrite.po.SaveFormulaSetHeadersResPo;
import com.rite.products.convertrite.po.SaveFormulaSetTablesPo;
import com.rite.products.convertrite.po.SaveFormulaSetTablesResPo;
import com.rite.products.convertrite.po.SourceColumnsPo;
import com.rite.products.convertrite.po.SourceTablesPo;
import com.rite.products.convertrite.po.TestDataResPo;
import com.rite.products.convertrite.po.TestSqlSyntaxResPo;
import com.rite.products.convertrite.po.XxrFormulaColumnsResPo;
import com.rite.products.convertrite.po.XxrFormulaSetTablesResPo;
import com.rite.products.convertrite.po.XxrFormulaSetsResPo;

public interface FormulaSetService {

	List<XxrFormulaSetsResPo> getFormulaSetsById(Long formulaSetId) throws Exception;

	SaveFormulaSetHeadersResPo saveFormulaSetHeaders(List<SaveFormulaSetHeaderPo> saveFormulaSetHeaderPo,HttpServletRequest request) throws Exception;

	List<XxrFormulaSetsView> getFormulaSets(Long roleId) throws Exception;

	List<XxrFormulaSetTablesResPo> getFormulaSetTables(Long formulaSetId)throws Exception ;

	SaveFormulaSetTablesResPo saveFormulaSetTables(List<SaveFormulaSetTablesPo> saveFormulaSetTablesPo,HttpServletRequest request)throws Exception;

	List<XxrFormulaColumnsResPo> getFormulaColumns(Long formulaSetId, Long formulaTableId) throws Exception;

	SaveFormulaColumnsResPo saveFormulaColumns(List<SaveFormulaColumnsPo> saveFormulaColumnsPo,HttpServletRequest request) throws Exception;

	List<SourceTablesPo> getSourceTableNames(Long objectId)throws Exception;

	List<SourceColumnsPo> getSourceColumns(Long sourceTableId)throws Exception;

	List<CloudColumnsPo> getCloudColumns(Long objectId)throws Exception;
	
	String[] getAllSourceObjects() throws Exception;

	List<SourceColumnsPo> getSourceColumns(String viewName)throws Exception;

	SaveFormulaSetHeadersResPo copyFormulaSet(String formulaSetName, Long formulaSetId, Long podId,HttpServletRequest request)throws Exception;

	TestSqlSyntaxResPo testSqlSyntax(String sqlQuery)throws Exception;

	TestDataResPo testSqlData(String sqlQuery)throws Exception;

	List<XxrFormulaSetsResPo> getFormulaSetsByobjectId(Long objectId)throws Exception;

}
