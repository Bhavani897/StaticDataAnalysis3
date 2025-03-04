package com.rite.products.convertrite.respository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.rite.products.convertrite.model.SourceTableId;
import com.rite.products.convertrite.model.XxrSourceTables;
import com.rite.products.convertrite.po.SourceTablesPo;

@Repository
public interface XxrSourceTablesRepository extends JpaRepository<XxrSourceTables, SourceTableId> {

	@Query(value = "SELECT c.sourceTableId.tableId FROM XxrSourceTables c "
			+ "WHERE  lower(c.sourceTableId.tableName) =lower(:tableName)")
	public Long getTableId(@Param("tableName") String tableName);

	@Query(value = "SELECT c.sourceTableId.tableName FROM XxrSourceTables c "
			+ "WHERE c.sourceTableId.tableId =:tableId")
	public String getMetaDataTableName(@Param("tableId") Long tableId);

	@Query(value = "select new com.rite.products.convertrite.po.SourceTablesPo(c.sourceTableId.tableId,c.sourceTableId.tableName) from XxrSourceTables c where c.objectId=:objectId")
	public List<SourceTablesPo> getTableIdNames(@Param("objectId") Long objectId);

}
