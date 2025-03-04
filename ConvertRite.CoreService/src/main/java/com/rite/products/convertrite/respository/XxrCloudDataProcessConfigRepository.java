package com.rite.products.convertrite.respository;

import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.rite.products.convertrite.model.XxrCloudDataProcessConfig;
@Repository
public interface XxrCloudDataProcessConfigRepository extends JpaRepository<XxrCloudDataProcessConfig,String>{

	Optional<XxrCloudDataProcessConfig> findByPodIdAndProjectId(Long podId, Long projectId);

	XxrCloudDataProcessConfig findFirstByOrderByCreationDateDesc();

}
