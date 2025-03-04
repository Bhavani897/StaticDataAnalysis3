package com.rite.products.convertrite.respository;

import com.rite.products.convertrite.model.CrProcessRequestsView;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface CrProcessRequestsViewRepository extends JpaRepository<CrProcessRequestsView,Long> {

	Page<CrProcessRequestsView> findAllByBatchName(String batchName,Pageable pageable);
	
	Page<CrProcessRequestsView> findAllByBatchNameAndRequestType(String batchName,String requestType,Pageable pageable);

    List<CrProcessRequestsView> findAllByObjectId(Long objectId);
}
