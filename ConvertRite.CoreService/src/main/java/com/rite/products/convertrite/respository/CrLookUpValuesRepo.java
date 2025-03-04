package com.rite.products.convertrite.respository;

import com.rite.products.convertrite.model.CrLookUpValues;
import com.rite.products.convertrite.po.LovValuesPo;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

public interface CrLookUpValuesRepo extends JpaRepository<CrLookUpValues, Long> {

    @Query("select new com.rite.products.convertrite.po.LovValuesPo(v.lookUpValue , v.lookUpValueId, s.lookUpSetName  ) from CrLookUpValues v,CrLookUpSets s where  s.lookUpSetId = v.lookUpSetId and  s.lookUpSetName = :columnId")
    List<LovValuesPo> getLookUpValues(@Param("columnId") String columnId);

    @Query("Select C from CrLookUpValues C Where C.lookUpSetId = :lookUpSetId")
    List<CrLookUpValues> getLookupValuesByLookUpSetId(Long lookUpSetId);

    @Modifying
    @Transactional
    @Query("Delete  From CrLookUpValues C Where C.lookUpSetId = :lookUpSetId ")
    void deleteAllLookupValuesBySetId(Long lookUpSetId);

    @Query("select c.lookUpValue  from CrLookUpValues c where c.lookUpValueId=:parentObjectId")
    String getValueById(Long parentObjectId);

    @Modifying
    @Transactional
    void deleteAllByLookUpSetId(Long lookUpSetValueId);

    @Modifying
    @Transactional
    void deleteById(Long lookUpSetValueId);
}
