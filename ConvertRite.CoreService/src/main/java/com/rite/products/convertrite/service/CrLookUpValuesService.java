package com.rite.products.convertrite.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import com.rite.products.convertrite.model.CrLookUpValues;
import com.rite.products.convertrite.po.BasicResponsePo;
import com.rite.products.convertrite.po.CrLookupSetValueCreateReqPo;
import com.rite.products.convertrite.po.LoadLookupResPo;
import org.springframework.web.multipart.MultipartFile;


public interface CrLookUpValuesService {

    List<CrLookUpValues> getAllLookupSetValues(Long lookUpSetId);

    Optional<CrLookUpValues> getLookupSetValueById(Long lookUpValueId);

    List<CrLookUpValues> saveAllLookupSet(ArrayList<CrLookupSetValueCreateReqPo> crLookupSetValueCreateReqPoList, Long lookupSetId);

    BasicResponsePo deleteLookupSet(Long lookUpSetValueId);

    BasicResponsePo deleteLookupValueById(Long lookUpValueId);

    String deleteAllbyLookUpSetId(Long lookupSetId);

    String SaveLookupValue(CrLookupSetValueCreateReqPo createReqPo);

    LoadLookupResPo loadLookupFromFile(MultipartFile file, Long lookupSetId) throws Exception;

}
