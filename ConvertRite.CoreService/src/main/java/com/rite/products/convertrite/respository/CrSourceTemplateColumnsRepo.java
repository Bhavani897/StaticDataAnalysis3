package com.rite.products.convertrite.respository;

import com.rite.products.convertrite.model.CrSourceTemplateColumns;
import com.rite.products.convertrite.po.SourceColumnsPo;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.query.Procedure;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface CrSourceTemplateColumnsRepo extends JpaRepository<CrSourceTemplateColumns,Long> {
    List<CrSourceTemplateColumns> findAllByTemplateId(long templateId);
    @Procedure
    String CR_SRC_COLS_MODIFY_PROC(Long templateId, String columnName, String columnType,  String operationType,Long displaySeq, String userId);
   
    @Query("select s.columnName from CrSourceTemplateColumns s " + "where s.columnId =:columnId")
    public String getSourceColumnName(@Param("columnId") long columnId);

    @Query("select new com.rite.products.convertrite.po.SourceColumnsPo(s.columnId,s.columnName) from CrSourceTemplateColumns s " + "where s.templateId =:templateId and s.selected IN ('M','Y')")
    public List<SourceColumnsPo> getColumnNames(@Param("templateId") long templateId);

}
