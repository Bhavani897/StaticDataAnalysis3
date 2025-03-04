package com.rite.products.convertrite.respository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import com.rite.products.convertrite.model.XxrProcessRequests;
import com.rite.products.convertrite.po.ProcessRequestsPo;

@Repository
public interface XxrProcessRequestsRepository extends JpaRepository<XxrProcessRequests, Long> {

	@Query("select  new com.rite.products.convertrite.po.ProcessRequestsPo(r,j.successRecords,j.failureRecords)   from XxrProcessRequests r,XxrProcessJobs j where r.requestId=j.requestId order by r.requestId desc")
	public List<ProcessRequestsPo> getProcessRequests();

	XxrProcessRequests findByXxrBatchNameAndTemplateId(String batchName,Long srcTemplateId);
}
