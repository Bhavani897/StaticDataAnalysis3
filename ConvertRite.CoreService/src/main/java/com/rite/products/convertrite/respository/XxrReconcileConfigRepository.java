package com.rite.products.convertrite.respository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.rite.products.convertrite.model.XxrReconcileConfig;

@Repository
public interface XxrReconcileConfigRepository extends JpaRepository<XxrReconcileConfig, Long>{
	

}
