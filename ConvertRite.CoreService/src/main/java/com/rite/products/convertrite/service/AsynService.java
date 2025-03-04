package com.rite.products.convertrite.service;

import com.rite.products.convertrite.enums.Status;
import com.rite.products.convertrite.model.CRTableSyncStatus;
import com.rite.products.convertrite.respository.CRTableSyncStatusRepository;
import com.rite.products.convertrite.utils.Utils;
import java.util.Objects;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class AsynService {
    @Autowired
    CRTableSyncStatusRepository crTableSyncStatusRepository;
    @Autowired
    Utils utils;

    @Async
    public void checkStatusOftableSync(String responseBody, Long jobId) {

        log.info(responseBody);
        try {
            JSONObject jsonObject = new JSONObject(responseBody);

            JSONObject crCloudStatusInfo = jsonObject.getJSONObject("crCloudStatusInformation");
            long statusId = crCloudStatusInfo.getLong("statusId");

            JSONObject cloudDataProcess = jsonObject.getJSONObject("cloudDataProcess");
            long id = cloudDataProcess.getLong("id");
            String tableName = cloudDataProcess.getString("tableName");
            //saving into cr_table_sync_status table
            CRTableSyncStatus crTableSyncStatus = saveIntoTableSyncStatusTable(jobId, id, statusId, tableName);
            while (true) {
                JSONObject statusJson = utils.getCldJobStatus(id, statusId);
                String status = statusJson.getString("status");
                if (Objects.equals(status, "completed")) {
                    updateStatusOfSync(crTableSyncStatus.getSyncId(), Status.COMPLETED.getStatus());
                    break;
                }
                if (Objects.equals(status, "error")) {
                    updateStatusOfSync(crTableSyncStatus.getSyncId(), Status.ERROR.getStatus());
                    break;
                }
            }
        } catch (Exception e) {
            log.error("getEssJobStatus-->" + e.getMessage());
        }
    }

    void updateStatusOfSync(Long statusId, String status) {
        Optional<CRTableSyncStatus> crTableSyncStatusOptional = crTableSyncStatusRepository.findById(statusId);
        CRTableSyncStatus crTableSyncStatus = new CRTableSyncStatus();
        if (crTableSyncStatusOptional.isPresent()) {
            crTableSyncStatus = crTableSyncStatusOptional.get();
        }
        crTableSyncStatus.setStatus(status);
        crTableSyncStatusRepository.save(crTableSyncStatus);
    }

    CRTableSyncStatus saveIntoTableSyncStatusTable(Long jobId, Long entityId, Long statusId, String tableName) {
        CRTableSyncStatus crTableSyncStatus = new CRTableSyncStatus();
        crTableSyncStatus.setJobId(jobId);
        crTableSyncStatus.setTableName(tableName);
        crTableSyncStatus.setSyncStatusId(statusId);
        crTableSyncStatus.setSyncEntityId(entityId);
        crTableSyncStatus.setStatus("Inprogress");
        return crTableSyncStatusRepository.save(crTableSyncStatus);
    }
}
