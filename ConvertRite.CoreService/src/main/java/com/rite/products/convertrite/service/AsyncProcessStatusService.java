package com.rite.products.convertrite.service;

import com.rite.products.convertrite.enums.Status;
import com.rite.products.convertrite.model.AsyncProcessStatus;
import com.rite.products.convertrite.model.CrCloudJobStatus;
import com.rite.products.convertrite.respository.AsyncProcessStatusRepository;
import com.rite.products.convertrite.respository.CrCloudJobStatusRepo;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.Optional;

@Service
@Slf4j
public class AsyncProcessStatusService {

    @Autowired
    private AsyncProcessStatusRepository asyncProcessStatusRepository;
    @Autowired
    CrCloudJobStatusRepo crCloudJobStatusRepo;

    public AsyncProcessStatus startProcess(String processName, Long cldTemplateId, String batchName, String createdBy) {
        AsyncProcessStatus status = new AsyncProcessStatus();
        status.setAsyncProcessName(processName);
        status.setAsyncStartTime(new Date());
        status.setAsyncProcessStatus(Status.IN_PROGRESS.getStatus());
        status.setBatchName(batchName);
        status.setCreatedBy(createdBy);
        status.setCreatedDate(new Date());
        return asyncProcessStatusRepository.save(status);
    }

    public void endProcess(Long processId, Long cldTemplateId, String batchName, String status, String errorMessage, String updatedBy, Long jobId) {
        AsyncProcessStatus asyncProcessStatus = asyncProcessStatusRepository.findById(processId)
                .orElseThrow(() -> new RuntimeException("Process not found: " + processId));

        asyncProcessStatus.setAsyncEndTime(new Date());
        asyncProcessStatus.setAsyncProcessStatus(status);
        asyncProcessStatus.setErrorMessage(errorMessage);
        asyncProcessStatus.setUpdatedBy(updatedBy);
        asyncProcessStatus.setUpdatedDate(new Date());
        asyncProcessStatus.setBatchName(batchName);
        asyncProcessStatusRepository.save(asyncProcessStatus);

        if(cldTemplateId!=null && batchName!=null){
            Optional<CrCloudJobStatus> crCloudJobStatus= crCloudJobStatusRepo.findByJobId(jobId);

            if(crCloudJobStatus.isPresent()){
                CrCloudJobStatus cloudJobStatus =crCloudJobStatus.get();
                log.info("Updating crCloudJobStatus-------{}",cloudJobStatus.getJobId() );
                cloudJobStatus.setJobStatus(status);
                if(errorMessage!=null){
                    cloudJobStatus.setErrorMsg(errorMessage);
                }
                crCloudJobStatusRepo.save(cloudJobStatus);
            }else{
                log.info("No record found in  CrCloudJobStatus-------{}",batchName );
            }
        }
    }
}
