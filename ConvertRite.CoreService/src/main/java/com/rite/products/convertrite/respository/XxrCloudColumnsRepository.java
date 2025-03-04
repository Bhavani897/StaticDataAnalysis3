package com.rite.products.convertrite.respository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.rite.products.convertrite.model.XxrCloudColumns;
import com.rite.products.convertrite.po.CloudColumnsPo;
import com.rite.products.convertrite.po.ColumnPo;

@Repository
public interface XxrCloudColumnsRepository extends JpaRepository<XxrCloudColumns, Long> {

	@Query("select x.columnName from XxrCloudColumns x " + "where x.tableId =:tableId")
	public String[] getColumnName(@Param("tableId") long tableId);

	@Query("select new com.rite.products.convertrite.po.CloudColumnsPo(a.columnId,a.columnName) from XxrCloudColumns a, XxrCloudTable b where a.tableId=b.cloudTableId.tableId and a.objectId=:objectId")
	public List<CloudColumnsPo> getCloudColumns(@Param("objectId") Long objectId);

	/*
	 * @Query("select  a.columnId,a.columnName from XxrCloudColumns a, XxrCloudTable b where a.tableId=b.cloudTableId.tableId and a.objectId=:objectId"
	 * ) public List<Object[]> getCloudColumns(@Param("objectId") Long objectId);
	 */

	@Query("select new com.rite.products.convertrite.po.ColumnPo(a.columnId,a.columnName,a.nullAllowedFlag,a.columnType,a.description,a.width)  from XxrCloudColumns a where a.tableId =:tableId Order by nullAllowedFlag asc")
	public List<ColumnPo> getCloudColumnsById(@Param("tableId") long tableId);

	@Query("select new com.rite.products.convertrite.po.ColumnPo(a.columnId,a.columnName,a.nullAllowedFlag,a.columnType,a.description,a.width)  from XxrCloudColumns a where a.tableId =:tableId and a.columnName IN (:columnNames)")
	public List<ColumnPo> getCloudColumnData(@Param("tableId") long tableId,
			@Param("columnNames") List<String> columnNames);

	public List<XxrCloudColumns> findByTableIdOrderByColumnId(Long metaDataTableId);

}
