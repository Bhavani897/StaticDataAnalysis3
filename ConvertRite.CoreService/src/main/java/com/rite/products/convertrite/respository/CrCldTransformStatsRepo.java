package com.rite.products.convertrite.respository;

import com.rite.products.convertrite.model.CrCldTransformStatsView;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface CrCldTransformStatsRepo extends JpaRepository<CrCldTransformStatsView,Long> {
    List<CrCldTransformStatsView> findAllByCloudTemplateName(String cloudTemplateName);

    List<CrCldTransformStatsView> findAllByBatchNameAndCloudTemplateName(String batchName, String cloudTemplateName);

}
