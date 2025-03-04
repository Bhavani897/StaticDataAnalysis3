package com.rite.products.convertrite.respository;

import com.rite.products.convertrite.model.CrCloudColumns;
import com.rite.products.convertrite.po.CloudColumnsPo;
import com.rite.products.convertrite.po.ColumnPo;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface CrCloudColumnsRepository extends JpaRepository<CrCloudColumns, Long> {

	@Query("select x.columnName from CrCloudColumns x " + "where x.tableId =:tableId")
	public String[] getColumnName(@Param("tableId") long tableId);

	@Query("select new com.rite.products.convertrite.po.CloudColumnsPo(a.columnId,a.columnName) from CrCloudColumns a, CrCloudTables b where a.tableId=b.tableId and a.objectId=:objectId")
	public List<CloudColumnsPo> getCloudColumns(@Param("objectId") Long objectId);

	@Query("select new com.rite.products.convertrite.po.ColumnPo(a.columnId,a.columnName,a.nullAllowedFlag,a.columnType,a.description,a.width)  from CrCloudColumns a where a.tableId =:tableId Order by nullAllowedFlag asc")
	public List<ColumnPo> getCloudColumnsById(@Param("tableId") long tableId);

	@Query("select new com.rite.products.convertrite.po.ColumnPo(a.columnId,a.columnName,a.nullAllowedFlag,a.columnType,a.description,a.width)  from CrCloudColumns a where a.tableId =:tableId and a.columnName IN (:columnNames)")
	public List<ColumnPo> getCloudColumnData(@Param("tableId") long tableId,
			@Param("columnNames") List<String> columnNames);

	public List<CrCloudColumns> findByTableIdOrderByColumnId(Long metaDataTableId);

}
