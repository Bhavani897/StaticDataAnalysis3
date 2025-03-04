package com.rite.products.convertrite.respository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.rite.products.convertrite.model.XxrBlobConvertrite;

@Repository
public interface XxrBlobConvertriteRepository extends JpaRepository<XxrBlobConvertrite,String>{
	
	XxrBlobConvertrite  findByBlobName(@Param("fileName") String fileName);

}
