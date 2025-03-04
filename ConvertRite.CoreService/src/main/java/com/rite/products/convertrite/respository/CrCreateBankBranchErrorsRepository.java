package com.rite.products.convertrite.respository;

import com.rite.products.convertrite.model.CrBankAccountErrors;
import com.rite.products.convertrite.model.CrCreateBankBranchErrors;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface CrCreateBankBranchErrorsRepository extends JpaRepository<CrCreateBankBranchErrors,Long> {
    List<CrCreateBankBranchErrors> findAllByCldTempIdAndCrBatchName(Long cldTempId, String batchName);
}
