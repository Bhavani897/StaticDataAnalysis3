package com.rite.products.convertrite.respository;

import java.sql.Timestamp;
import java.util.List;
import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.rite.products.convertrite.model.XxrCloudDataProcess;

@Repository
public interface XxrCloudDataProcessRepository extends JpaRepository<XxrCloudDataProcess, String> {

	Optional<XxrCloudDataProcess> findByRequestId( String requestId);

	@Query("select max(creationDate) from XxrCloudDataProcess where scheduledJobCall='Y' and status='completed'")
	Timestamp getCreationDate();

	@Query("select tableName from XxrCloudDataProcess c where c.tableName = :tableName")
	String findByTableNameIgnoreCase(@Param("tableName") String tableName);

	List<XxrCloudDataProcess> findAllByExtractionFlagAndStatus(String extrationFlag,String status);

}
