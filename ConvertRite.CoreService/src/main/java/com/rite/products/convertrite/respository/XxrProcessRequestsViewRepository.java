package com.rite.products.convertrite.respository;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.rite.products.convertrite.model.XxrProcessRequestsView;

@Repository
public interface XxrProcessRequestsViewRepository extends JpaRepository<XxrProcessRequestsView,Long> {

	Page<XxrProcessRequestsView> findAllByXxrBatchName(String batchName,Pageable pageable);
	
	Page<XxrProcessRequestsView> findAllByXxrBatchNameAndRequestType(String batchName,String requestType,Pageable pageable);

}
