package com.rite.products.convertrite.respository;

import com.rite.products.convertrite.model.CrTemplateStatisticsView;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface CrTemplateStatisticsViewRepository extends JpaRepository<CrTemplateStatisticsView,String>{

}
