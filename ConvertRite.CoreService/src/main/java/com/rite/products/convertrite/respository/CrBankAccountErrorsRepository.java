package com.rite.products.convertrite.respository;

import com.rite.products.convertrite.model.CrBankAccountErrors;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface CrBankAccountErrorsRepository extends JpaRepository<CrBankAccountErrors, Long> {
    List<CrBankAccountErrors> findAllByCldTempIdAndCrBatchName(Long cldTempId, String batchName);
}
