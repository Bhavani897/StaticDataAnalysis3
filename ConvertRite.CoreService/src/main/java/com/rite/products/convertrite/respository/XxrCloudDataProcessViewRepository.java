package com.rite.products.convertrite.respository;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.rite.products.convertrite.model.XxrCloudDataProcessView;

@Repository
public interface XxrCloudDataProcessViewRepository extends JpaRepository<XxrCloudDataProcessView, String>{

    Page<XxrCloudDataProcessView> findAllByExtractionFlag(String extractionFlag, Pageable pageable);
}
