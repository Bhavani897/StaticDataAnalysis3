package com.rite.products.convertrite.respository;

import com.rite.products.convertrite.model.CrCloudTables;
import com.rite.products.convertrite.po.CloudTablesPo;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface CrCloudTableRepository extends JpaRepository<CrCloudTables, Long> {
	@Query(value = "SELECT c.tableId FROM CrCloudTables c "
			+ "WHERE  lower(c.tableName) =lower(:tableName)")
	public Long getTableId(@Param("tableName") String tableName);
	
	@Query(value = "SELECT c.tableName FROM CrCloudTables c "
			+ "WHERE c.tableId =:tableId")
	public String getMetaDataTableName(@Param("tableId") long tableId);
	
	@Query(value = "select new com.rite.products.convertrite.po.CloudTablesPo(c.tableId,c.tableName) from CrCloudTables c where c.objectId=:objectId and c.tableType='T'")
	public List<CloudTablesPo> getTableIdNames(@Param("objectId") long objectId);
	Optional<CrCloudTables> findByTableName(String tableName);
}
