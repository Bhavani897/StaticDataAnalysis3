package com.rite.products.convertrite.respository;

import com.rite.products.convertrite.model.CrBatchProcessDetails;
import com.rite.products.convertrite.model.CrSourceTemplateHeaders;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface CrBatchProcessingDetailsRepo extends JpaRepository<CrBatchProcessDetails,Long> {
    boolean  existsBySrcTempIdAndBatchName(Long srcTempId, String batchName);

}