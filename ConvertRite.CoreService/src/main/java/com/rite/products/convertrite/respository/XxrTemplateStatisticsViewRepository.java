package com.rite.products.convertrite.respository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.rite.products.convertrite.model.XxrTemplateStatisticsView;
@Repository
public interface XxrTemplateStatisticsViewRepository extends JpaRepository<XxrTemplateStatisticsView, Long>{

}
