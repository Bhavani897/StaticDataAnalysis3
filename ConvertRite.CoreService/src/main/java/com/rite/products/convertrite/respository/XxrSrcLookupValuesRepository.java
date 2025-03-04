package com.rite.products.convertrite.respository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.rite.products.convertrite.model.XxrSrcLookupValues;

@Repository
public interface XxrSrcLookupValuesRepository extends JpaRepository<XxrSrcLookupValues,Long>{

	XxrSrcLookupValues findByLookupValueAndLookupSetId(String lookupValue,Long lookupsetId);
	
	List<XxrSrcLookupValues> findByLookupSetId(Long lookupsetId);
	
}
