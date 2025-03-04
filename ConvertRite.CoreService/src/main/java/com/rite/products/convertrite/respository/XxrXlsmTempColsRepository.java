package com.rite.products.convertrite.respository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.rite.products.convertrite.model.XxrXlsmTempCols;

@Repository
public interface XxrXlsmTempColsRepository extends JpaRepository<XxrXlsmTempCols, Long> {

	List<XxrXlsmTempCols> findBytemplateId(Long templateId);
	XxrXlsmTempCols findBycolumnId(Long columnId);
	@Query("select  c.databaseColumn,c.sequence from XxrXlsmTempCols c,XxrXlsmTempHdrs d where c.templateId=d.templateId and d.version=:version and d.objectId=:objectId")
	public List<Object> getColumnNameandSequence(@Param("objectId") Long objectId, @Param("version") String version);
}
