package com.rite.products.convertrite.respository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.rite.products.convertrite.model.XxrCloudConfig;

@Repository
public interface XxrCloudConfigRepository extends JpaRepository<XxrCloudConfig,String> {
	
	XxrCloudConfig findByObjectCode(@Param("objectCode") String objectCode);

}
