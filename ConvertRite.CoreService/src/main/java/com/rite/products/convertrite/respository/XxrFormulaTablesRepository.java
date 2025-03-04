package com.rite.products.convertrite.respository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import com.rite.products.convertrite.model.XxrFormulaTables;
import com.rite.products.convertrite.model.XxrFormulaTablesId;
import com.rite.products.convertrite.po.XxrFormulaSetTablesResPo;

public interface XxrFormulaTablesRepository extends JpaRepository<XxrFormulaTables,XxrFormulaTablesId>{
	@Query("select new com.rite.products.convertrite.po.XxrFormulaSetTablesResPo(f.formulaSetId,f.formulaTableId,f.seq,s.sourceTableId.tableId,s.sourceTableId.tableName,f.description,f.attribute1,f.attribute2,f.attribute3,f.attribute4,f.attribute5) from XxrFormulaTables f,XxrSourceTables s where f.sourceTableId=s.sourceTableId.tableId and f.formulaSetId=:formulaSetId")
	List<XxrFormulaSetTablesResPo> findByformulaSetId(Long formulaSetId);
	
	@Query("select f.formulaTableId from XxrFormulaTables f where f.formulaSetId=:formulaSetId and f.sourceTableId=:sourceTableId")
	public Long getFormulaTableId(@Param("formulaSetId") Long formulaSetId,@Param("sourceTableId") Long sourceTableId);
	
	@Query("select f.formulaTableId from XxrFormulaTables f where f.formulaSetId=:formulaSetId and f.seq=:seq")
	public Long getFormulaTableIdBySeq(@Param("formulaSetId") Long formulaSetId,@Param("seq") Long seq);
 
}
