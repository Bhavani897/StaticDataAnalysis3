package com.rite.products.convertrite.po;

import lombok.Data;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

@Data
public class CrDataSyncScheduleReqPo {
    private String scheduleName;
    private String sourceTemplateIdsList;
    private String batchName;
    private Long scheduleFrequency;
    private Long sourceSystemId;
    private Long targetSystemId;
    private String extractionType;
    private String scheduleStartTime;
    private String status;
    private String isDependent;
    private Map<Integer, List<String>> templateFiles;
}
