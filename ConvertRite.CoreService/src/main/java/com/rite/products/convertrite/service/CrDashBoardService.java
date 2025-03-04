package com.rite.products.convertrite.service;

public interface CrDashBoardService {
    Object getTemplateStatistics();

    Object getCrTransformStats(String cloudTemplateName, String batchName);
}
