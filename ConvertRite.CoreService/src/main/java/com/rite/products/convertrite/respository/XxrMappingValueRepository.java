package com.rite.products.convertrite.respository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.rite.products.convertrite.model.XxrMappingValue;

@Repository
public interface XxrMappingValueRepository extends JpaRepository<XxrMappingValue,Long>{

	List<XxrMappingValue> findByMapSetId(Long mappingsetId);

	XxrMappingValue findByMapLineId(Long mapLineId);
}
