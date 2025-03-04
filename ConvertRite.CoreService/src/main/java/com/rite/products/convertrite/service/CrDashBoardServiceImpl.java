package com.rite.products.convertrite.service;

import com.rite.products.convertrite.model.CrCldTransformStatsView;
import com.rite.products.convertrite.model.CrTemplateStatisticsView;
import com.rite.products.convertrite.respository.CrCldTransformStatsRepo;
import com.rite.products.convertrite.respository.CrTemplateStatisticsViewRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class CrDashBoardServiceImpl implements  CrDashBoardService{
@Autowired
CrTemplateStatisticsViewRepository crTemplateStatisticsViewRepo;
    @Autowired
    CrCldTransformStatsRepo crCldTransformStatsRepo;
    @Override
    public Object getTemplateStatistics() {
    return crTemplateStatisticsViewRepo.findAll();
    }

    @Override
    public Object getCrTransformStats(String cloudTemplateName, String batchName) {
        List<CrCldTransformStatsView> list=null;
        try {
            if (cloudTemplateName != null && !cloudTemplateName.isEmpty()
                && batchName != null && !batchName.isEmpty()) {
                list = crCldTransformStatsRepo.findAllByBatchNameAndCloudTemplateName(batchName, cloudTemplateName);
            } else if (cloudTemplateName !=null && !cloudTemplateName.isEmpty()) {
                list = crCldTransformStatsRepo.findAllByCloudTemplateName(cloudTemplateName);
            } else {
                list = crCldTransformStatsRepo.findAll();
            }

        } catch (Exception e) {
            return new ResponseEntity<>(e.getCause(), HttpStatus.EXPECTATION_FAILED);
        }
        return list;

    }
}
