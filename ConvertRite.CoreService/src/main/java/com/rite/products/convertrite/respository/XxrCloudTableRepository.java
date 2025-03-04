package com.rite.products.convertrite.respository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.rite.products.convertrite.model.CloudTableId;
import com.rite.products.convertrite.model.XxrCloudTable;
import com.rite.products.convertrite.po.CloudTablesPo;

@Repository
public interface XxrCloudTableRepository extends JpaRepository<XxrCloudTable, CloudTableId> {
	/*
	 * @Query(value =
	 * "SELECT  t.templateName FROM XxrCloudTable c INNER JOIN SourceTemplateHeaders t "
	 * +
	 * "ON c.objectCode = t.saasobjectCode and c.parentObjectCode = t.saasParentObjectCode"
	 * ) public String[] getSourceTemplateHeaders();
	 */
	@Query(value = "SELECT c.cloudTableId.tableId FROM XxrCloudTable c "
			+ "WHERE  lower(c.cloudTableId.tableName) =lower(:tableName)")
	public Long getTableId(@Param("tableName") String tableName);
	
	@Query(value = "SELECT c.cloudTableId.tableName FROM XxrCloudTable c "
			+ "WHERE c.cloudTableId.tableId =:tableId")
	public String getMetaDataTableName(@Param("tableId") Long tableId);
	
	@Query(value = "select new com.rite.products.convertrite.po.CloudTablesPo(c.cloudTableId.tableId,c.cloudTableId.tableName) from XxrCloudTable c where c.objectId=:objectId and c.tableType='T'")
	public List<CloudTablesPo> getTableIdNames(@Param("objectId") Long objectId);

}
