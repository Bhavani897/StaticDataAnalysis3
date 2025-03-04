package com.rite.products.convertrite.respository;

import com.rite.products.convertrite.model.CrFileDetails;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

@Repository
public interface CrFileDetailsRepo extends JpaRepository<CrFileDetails, Long> {
    @Transactional
    CrFileDetails findByFileName(String fileName);

    Optional<CrFileDetails> findByCldFileId(Long cldFileId);
}
