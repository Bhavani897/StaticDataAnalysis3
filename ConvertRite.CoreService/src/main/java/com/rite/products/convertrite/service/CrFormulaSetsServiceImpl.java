package com.rite.products.convertrite.service;

import java.util.*;

import com.rite.products.convertrite.model.CrCloudTemplateHeaders;
import com.rite.products.convertrite.po.BasicResponsePo;
import com.rite.products.convertrite.respository.CrCloudTemplateHeadersRepository;
import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.rite.products.convertrite.model.CrFormulaSets;
import com.rite.products.convertrite.po.CrFormulaSetsCreateReqPo;
import com.rite.products.convertrite.respository.CrFomrulaSetsRepo;

@Service
@Slf4j
public class CrFormulaSetsServiceImpl implements CrFormulaSetsService {

    @Autowired
    CrFomrulaSetsRepo crFomrulaSetsRepo;
    @Autowired
    CrCloudTemplateHeadersRepository crCldTempHdrsRepo;

    @Override
    public List<CrFormulaSets> getAllFormulaSets() {
        return crFomrulaSetsRepo.findAll();
    }

    @Override
    public Optional<CrFormulaSets> getFormulaSetById(Long FormulaSetId) {
        return crFomrulaSetsRepo.findById(FormulaSetId);
    }

    @Override
    public CrFormulaSets saveFormulaSet(CrFormulaSetsCreateReqPo createReqPo) {
        log.info("Start saveFormulaSet...");
        try {
            CrFormulaSets crFormulaSets = new CrFormulaSets();

            if (Objects.isNull(createReqPo.getFormulaSetId())) {
                boolean exists = crFomrulaSetsRepo.existsByFormulaSetCode(createReqPo.getFormulaSetCode());
                if (exists) {
                    throw new RuntimeException("Formula set with code " + createReqPo.getFormulaSetCode() + " already exists.");
                }
                crFormulaSets.setCreatedBy("Somu");
                crFormulaSets.setCreationDate(new Date());
            } else {
                crFormulaSets.setLastUpdateBy("Somu");
                crFormulaSets.setLastUpdatedDate(new Date());
                crFormulaSets.setFormulaSetId(createReqPo.getFormulaSetId());
            }
            crFormulaSets.setAttribute1(createReqPo.getAttribute1());
            crFormulaSets.setAttribute2(createReqPo.getAttribute2());
            crFormulaSets.setAttribute3(createReqPo.getAttribute3());
            crFormulaSets.setAttribute4(createReqPo.getAttribute4());
            crFormulaSets.setAttribute5(createReqPo.getAttribute5());
            crFormulaSets.setDescription(createReqPo.getDescription());
            crFormulaSets.setFormulaSetCode(createReqPo.getFormulaSetCode());
            crFormulaSets.setFormulaSetName(createReqPo.getFormulaSetName());
            crFormulaSets.setFormulaText(createReqPo.getFormulaText());
            crFormulaSets.setFormulaType(createReqPo.getFormulaType());
            crFormulaSets.setCountOfParams(createReqPo.getCountOfParams());

            CrFormulaSets savedFormulaSet = crFomrulaSetsRepo.save(crFormulaSets);
            log.info("Formula set saved");

            log.info("Retrieved formula set by code: {}", savedFormulaSet);

            log.info("End saveFormulaSet.");
            return savedFormulaSet;
        } catch (Exception e) {
            e.printStackTrace();
            log.error("An error occurred while saving formula set: {}", e.getMessage());
            throw new RuntimeException("Failed to save formula set", e);
        }
    }

    public BasicResponsePo deleteFormulaSetById(String mappingType, Long formulaId) {
        BasicResponsePo resPo = new BasicResponsePo();
        List<String> cldTempNamesList;
        try {
            cldTempNamesList = crCldTempHdrsRepo.selectCldNameFromCldTempHdsAndCols(mappingType, formulaId);
            if (cldTempNamesList.isEmpty()) {
                crFomrulaSetsRepo.deleteById(formulaId);
                resPo.setMessage("Formula set deleted Successfully");
            } else {
                resPo.setMessage("Formula set cannot be deleted! It is being used in the following Cloud Template(s):");
                resPo.setPayload(cldTempNamesList);
                resPo.setError("Deletion Failed");
            }
        } catch (Exception e) {
            log.error("Error in deleteFormulaSetById()------>"+e.getMessage());
            resPo.setMessage("Error while Deleting Formula set");
            resPo.setError(e.getMessage());
        }
        return resPo;
    }

    @Override
    public CrFormulaSets getFormulaByCode(String formulaCode) {

        return crFomrulaSetsRepo.getFormulaByCode(formulaCode);
    }

}
