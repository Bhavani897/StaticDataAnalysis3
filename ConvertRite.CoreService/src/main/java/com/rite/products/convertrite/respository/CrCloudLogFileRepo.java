package com.rite.products.convertrite.respository;

import com.rite.products.convertrite.model.CrCloudJobStatus;
import com.rite.products.convertrite.model.CrCloudLogDetails;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface CrCloudLogFileRepo extends JpaRepository<CrCloudLogDetails, Long>{
    CrCloudLogDetails findByLoadRequestId(Long loadRequestId);
}
