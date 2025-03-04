package com.rite.products.convertrite.respository;

import com.rite.products.convertrite.model.CrCloudJobStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import javax.transaction.Transactional;
import java.util.List;
import java.util.Optional;

public interface CrCloudJobStatusRepo extends JpaRepository<CrCloudJobStatus, Long> {
    List<CrCloudJobStatus> findByLoadRequestId(Long requestId);

    List<CrCloudJobStatus> findByJobStatus(String status);

    CrCloudJobStatus findByCldTemplateIdAndBatchName(Long cldTemplateId, String batchName);

    List<CrCloudJobStatus> findByCldTemplateIdAndBatchNameOrderByCreationDateDesc(Long cldTemplateId, String batchName);

    List<CrCloudJobStatus> findByBatchNameAndCldTemplateIdIn(String batchName, List<Long> cldTemplateIdList);

    Optional<CrCloudJobStatus> findByJobId(Long jobId);
}
