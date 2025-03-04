package com.rite.products.convertrite.respository;

import org.springframework.data.jpa.repository.JpaRepository;

import com.rite.products.convertrite.model.XxrTemplateStatistics;

public interface XxrTemplateStatisticsRepository extends JpaRepository<XxrTemplateStatistics,String>{
	
	/*
	 * @Query("select :filterBy,podName from XxrTemplateStatistics group by :filterBy "
	 * ) public List getTemplateStatistics(@Param("filterBy") String filterBy);
	 */

}
