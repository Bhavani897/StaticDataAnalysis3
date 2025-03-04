package com.rite.products.convertrite.respository;

import org.springframework.data.jpa.repository.JpaRepository;

import com.rite.products.convertrite.model.CrProcessRequests;

public interface CrProcessRequestsRepository extends JpaRepository<CrProcessRequests, Long>{

	CrProcessRequests findBySrcTemplateIdAndCrBatchName(Long srcTemplateId, String batchName);

}
