package com.rite.products.convertrite.respository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.rite.products.convertrite.model.XxrErpIntegration;

@Repository
public interface XxrErpIntegrationRepository extends JpaRepository<XxrErpIntegration, Long> {

	public List<XxrErpIntegration> findAllByOrderByIdDesc();
	
	public XxrErpIntegration findByResult(Long result);

	public XxrErpIntegration findByXxrBatchNameAndCloudTemplateId(String batchName,Long cldTemplateId);

	public XxrErpIntegration findByXxrBatchNameAndCloudTemplateIdIn(String batchName, List<Long> listofTemplates);
}
