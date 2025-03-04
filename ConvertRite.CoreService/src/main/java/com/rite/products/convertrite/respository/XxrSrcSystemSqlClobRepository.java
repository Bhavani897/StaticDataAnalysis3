package com.rite.products.convertrite.respository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.rite.products.convertrite.model.XxrSrcSystemSqlClob;

@Repository
public interface XxrSrcSystemSqlClobRepository  extends JpaRepository<XxrSrcSystemSqlClob,Long>{

	@Query("select c from  XxrSrcSystemSqlClob c where c.objectCode=:objectCode and c.sourceSystem=:sourceSystem and c.version=:version")
	XxrSrcSystemSqlClob getSrcSystemRec(@Param("objectCode") String objectCode,@Param("sourceSystem") String sourceSystem,@Param("version") String version);
	
	XxrSrcSystemSqlClob findByObjectCodeAndSourceSystemAndVersion(String objectCode, String sourceSystem, String version);

}
