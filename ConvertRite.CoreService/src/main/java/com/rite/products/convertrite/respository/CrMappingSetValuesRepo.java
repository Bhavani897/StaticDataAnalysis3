package com.rite.products.convertrite.respository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.transaction.annotation.Transactional;

import com.rite.products.convertrite.model.CrMappingSetValues;

public interface CrMappingSetValuesRepo extends JpaRepository<CrMappingSetValues, Long> {

    @Query("select C from CrMappingSetValues C where C.mapSetId = :mappingsetId")
    List<CrMappingSetValues> getValuesByMappingSetId(Long mappingsetId);

    @Modifying
    @Transactional
    void deleteAllByMapSetId(Long mapSetId);
	
	@Modifying
    @Transactional
    @Query("Delete from CrMappingSetValues C where C.mapSetId = :mapSetId")
    void deleteAllMappingValuesBySetId(Long mapSetId);
	
	
}
