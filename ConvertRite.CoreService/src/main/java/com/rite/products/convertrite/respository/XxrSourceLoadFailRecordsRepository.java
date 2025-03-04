package com.rite.products.convertrite.respository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.rite.products.convertrite.model.XxrSourceLoadFailRecords;

@Repository
public interface XxrSourceLoadFailRecordsRepository extends JpaRepository<XxrSourceLoadFailRecords,Long>{

	public List<XxrSourceLoadFailRecords> findAllByOrderByCreationDateDesc();
	XxrSourceLoadFailRecords findBytemplateId(@Param("templateId") Long templateId);
	
	XxrSourceLoadFailRecords findBytemplateIdAndId(@Param("templateId") Long templateId,@Param("id") Long id);
}
