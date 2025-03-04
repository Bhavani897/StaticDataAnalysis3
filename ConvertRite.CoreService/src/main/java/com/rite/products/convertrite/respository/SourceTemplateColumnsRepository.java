package com.rite.products.convertrite.respository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.rite.products.convertrite.model.SourceTemplateColumns;
import com.rite.products.convertrite.po.SourceColumnsPo;

@Repository
public interface SourceTemplateColumnsRepository extends JpaRepository<SourceTemplateColumns, Long> {

	@Query("select new com.rite.products.convertrite.po.SourceColumnsPo(s.columnId,s.columnName) from SourceTemplateColumns s " + "where s.templateId =:templateId and s.selected IN ('M','Y')")
	public List<SourceColumnsPo> getColumnNames(@Param("templateId") long templateId);
	
	@Query("select s.columnName from SourceTemplateColumns s " + "where s.columnId =:columnId")
	public String getSourceColumnName(@Param("columnId") long columnId);

	@Query("select new com.rite.products.convertrite.po.SourceColumnsPo(s.columnId,s.columnName) from SourceTemplateColumns s "
			+ "where s.templateId =:templateId and s.selected IN ('M','Y')")
	public List<SourceColumnsPo> getSourceColumns(@Param("templateId") long templateId);

	List<SourceTemplateColumns> findByTemplateId(@Param("templateId") long templateId);

}