package com.rite.products.convertrite.respository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import com.rite.products.convertrite.model.XxrSourceColumns;
import com.rite.products.convertrite.po.ColumnPo;
import com.rite.products.convertrite.po.SourceColumnsPo;

public interface XxrSourceColumnsRepository extends JpaRepository<XxrSourceColumns, Long>{
	
	@Query("select new com.rite.products.convertrite.po.SourceColumnsPo(x.columnId,x.columnName) from XxrSourceColumns x where x.tableId=:tableId ")
	public List<SourceColumnsPo> getSourceColumns(@Param("tableId") long tableId);
	
	@Query("select new com.rite.products.convertrite.po.ColumnPo(x.columnId,x.columnName,x.nullAllowedFlag,x.columnType,x.width) from XxrSourceColumns x where x.tableId=:tableId Order by nullAllowedFlag asc")
	public List<ColumnPo> getSourceColumnsById(@Param("tableId") long tableId);
	

	public List<XxrSourceColumns> findByTableIdOrderByColumnId(Long metaDataTableId);

}
