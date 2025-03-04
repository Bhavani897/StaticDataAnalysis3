package com.rite.products.convertrite.respository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.rite.products.convertrite.model.XxrReconcileStatistics;

@Repository
public interface XxrReconcileStatisticsRepository extends JpaRepository<XxrReconcileStatistics, Long>{

	XxrReconcileStatistics findByBatchNameAndCldTemplateName(String batchName,String cldTemplateName);

}
