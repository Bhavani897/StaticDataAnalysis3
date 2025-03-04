package com.rite.products.convertrite.service;

import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.rite.products.convertrite.model.CrMappingSetHdr;
import com.rite.products.convertrite.po.CrMappingSetHdrCreateReqPo;
import com.rite.products.convertrite.respository.CrMappingSetHeaderRepo;

@Service
@Slf4j
public class CrMappingSetHdrServiceImpl implements CrMappingSetHdrService {

    @Autowired
    CrMappingSetHeaderRepo crMappingSetHeaderRepo;

    @Override
    public List<CrMappingSetHdr> getAllMappingsets() {
        return crMappingSetHeaderRepo.findAll();
    }

    @Override
    public Optional<CrMappingSetHdr> findById(long mappingSetId) {
        return crMappingSetHeaderRepo.findById(mappingSetId);
    }

    @Override
    public CrMappingSetHdr saveMappingSetHdr(CrMappingSetHdrCreateReqPo reqPO) {
        try {

            CrMappingSetHdr crMappingSetHdr = new CrMappingSetHdr();

            if (Objects.isNull(reqPO.getMapSetId())) {
                boolean exists = crMappingSetHeaderRepo.existsByMapSetCode(reqPO.getMapSetCode());
                if (exists) {
                    throw new RuntimeException("Mapping set with code " + reqPO.getMapSetCode() + " already exists.");
                }
                crMappingSetHdr.setCreationDate(new Date());
                crMappingSetHdr.setCreatedBy("Somu");
            } else {
                crMappingSetHdr.setMapSetId(reqPO.getMapSetId());
                crMappingSetHdr.setLastUpdateBy("Somu");
                crMappingSetHdr.setLastUpdateDate(new Date());
            }

            crMappingSetHdr.setAttribute1(reqPO.getAttribute1());
            crMappingSetHdr.setAttribute2(reqPO.getAttribute2());
            crMappingSetHdr.setAttribute3(reqPO.getAttribute3());
            crMappingSetHdr.setAttribute4(reqPO.getAttribute4());
            crMappingSetHdr.setAttribute5(reqPO.getAttribute5());
            crMappingSetHdr.setMapSetCode(reqPO.getMapSetCode());
            crMappingSetHdr.setLookupSetId(reqPO.getLookupSetId());
            crMappingSetHdr.setValidationType(reqPO.getValidationType());
            crMappingSetHdr.setMapSetType(reqPO.getMapSetType());
            crMappingSetHdr.setMapSetName(reqPO.getMapSetName());
            crMappingSetHdr.setSqlQuery(reqPO.getSqlQuery());

            crMappingSetHdr = crMappingSetHeaderRepo.save(crMappingSetHdr);
            log.info("Mapping set saved successfully: {}", crMappingSetHdr);

            return crMappingSetHdr;
        } catch (Exception e) {

            log.error("An error occurred while saving mapping set: {}", e.getMessage());
            throw new RuntimeException("Failed to save mapping set", e);
        }
    }

    @Override
    public String deleteById(long mapingSetId) {
        crMappingSetHeaderRepo.deleteById(mapingSetId);
        return "Success";
    }

    @Override
    public CrMappingSetHdr findByMapCode(String mappingSetCode) {
        return crMappingSetHeaderRepo.findByMapSetCode(mappingSetCode);
    }

}
