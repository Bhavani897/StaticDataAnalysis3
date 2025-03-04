package com.rite.products.convertrite.respository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.rite.products.convertrite.model.XxrFbdiTempCols;

@Repository
public interface XxrFbdiTempColsRepository extends JpaRepository<XxrFbdiTempCols, Long> {
	List<XxrFbdiTempCols> findByfbdiTemplateId(Long fbdiTemplateId);

	@Query("select  c.databaseColumn,c.sequence from XxrFbdiTempCols c,XxrFbdiTempHdrs d where c.fbdiTemplateId=d.fbdiTemplateId and d.version=:version and d.objectId=:objectId")
	public List<Object> getColumnNameandSequence(@Param("objectId") Long objectId,@Param("version") String version);

	XxrFbdiTempCols findByFbdiTemplateIdAndFbdiColumnId(Long fbdiTemplateId, Long fbdiColumnId);

	@Query("select max(c.fbdiColumnId) from XxrFbdiTempCols c  where lower(c.fbdiTemplateId)=lower(:fbdiTemplateId) ")
	public Long getColumnCount(@Param("fbdiTemplateId") Long fbdiTemplateId);

}
