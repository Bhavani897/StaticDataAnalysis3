package com.rite.products.convertrite.respository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import com.rite.products.convertrite.model.XxrFormulaColumns;
import com.rite.products.convertrite.model.XxrFormulaColumnsId;
import com.rite.products.convertrite.po.XxrFormulaColumnsResPo;

public interface XxrFormulaColumnsRepository extends JpaRepository<XxrFormulaColumns,XxrFormulaColumnsId>{

	@Query("select new com.rite.products.convertrite.po.XxrFormulaColumnsResPo(f.formulaSetId,f.formulaTableId,f.formulaColumnId,f.seq,s.columnId,s.columnName,f.description,f.attribute1,f.attribute2,f.attribute3,f.attribute4,f.attribute5) from XxrFormulaColumns f,XxrSourceColumns s where f.sourceColumnId=s.columnId and f.formulaSetId=:formulaSetId and f.formulaTableId=:formulaTableId")
	public List<XxrFormulaColumnsResPo> getFormulaColumns(@Param("formulaSetId") Long formulaSetId,@Param("formulaTableId") Long formulaTableId);
	
	@Query("select  f.formulaColumnId from XxrFormulaColumns f where f.formulaSetId=:formulaSetId and f.formulaTableId=:formulaTableId and f.sourceColumnId=:sourceColumnId")
	public Long getFormulaColumnId(@Param("formulaSetId") Long formulaSetId,@Param("formulaTableId") Long formulaTableId,@Param("sourceColumnId") Long sourceColumnId);
	
	@Query("select  f.formulaColumnId from XxrFormulaColumns f where f.formulaSetId=:formulaSetId and f.formulaTableId=:formulaTableId and f.seq=:seq")
	public Long getFormulaColumnIdByseq(@Param("formulaSetId") Long formulaSetId,@Param("formulaTableId") Long formulaTableId,@Param("seq") Long seq);

}
