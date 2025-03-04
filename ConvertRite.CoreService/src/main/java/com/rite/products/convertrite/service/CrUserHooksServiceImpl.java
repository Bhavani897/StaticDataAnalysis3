package com.rite.products.convertrite.service;

import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import com.rite.products.convertrite.respository.CrHookUsagesRepo;
import com.rite.products.convertrite.model.CrUserHooks;
import com.rite.products.convertrite.po.CrUserHooksCreateReqPo;
import com.rite.products.convertrite.respository.CrUserHooksRepo;
import com.rite.products.convertrite.po.BasicResponsePo;

@Service
@Slf4j
public class CrUserHooksServiceImpl implements CrUserHooksService {

    @Autowired
    CrUserHooksRepo crUserHooksRepo;

    @Autowired
    CrHookUsagesRepo crHookUsagesRepo;

    @Override
    public List<CrUserHooks> getAllUserHooks() {
        return crUserHooksRepo.findAll();
    }

    @Override
    public Optional<CrUserHooks> getUserHookById(Long hookId) {
        return crUserHooksRepo.findById(hookId);
    }

    @Override
    public CrUserHooks getUserHookByCode(String hookCode) {
        return crUserHooksRepo.findByUserHookCode(hookCode);
    }

    @Override
    public BasicResponsePo deleteUserHook(Long hookId) {
        BasicResponsePo resPo = new BasicResponsePo();
        List<String> cldTempNamesList;
        try {
            cldTempNamesList = crHookUsagesRepo.selectCldNameFromCldTempHds(hookId);
            if (cldTempNamesList.size() == 0) {
                crUserHooksRepo.deleteById(hookId);
                resPo.setMessage("User Hooks deleted Successfully");
            } else {
                resPo.setMessage("User Hook Cannot be deleted! It is being used in the following Cloud Template(s):");
                resPo.setError("Deletion Failed");
                resPo.setPayload(cldTempNamesList);
            }
        } catch (Exception e) {
            log.error("Error in deleteUserHooksById()------>"+e.getMessage());
            resPo.setMessage("Error while Deleting User Hooks");
            resPo.setError(e.getMessage());
        }
        return resPo;
    }


    @Override
    public CrUserHooks saveUserHook(CrUserHooksCreateReqPo createReqPo) {
        CrUserHooks crUserHooks = new CrUserHooks();
        try {
            if (Objects.isNull(createReqPo.getHookId())) {
                boolean exists = crUserHooksRepo.existsByHookCode(createReqPo.getHookCode());
                if (exists) {
                    throw new RuntimeException("UserHook with code " + createReqPo.getHookCode() + " already exists.");
                }
                crUserHooks.setCreatedBy("Somu");
                crUserHooks.setCreationDate(new Date());
            } else {
                crUserHooks.setLastUpdateBy("Somu");
                crUserHooks.setLastUpdateDate(new Date());
                crUserHooks.setHookId(createReqPo.getHookId());
            }
            crUserHooks.setAttribute1(createReqPo.getAttribute1());
            crUserHooks.setAttribute2(createReqPo.getAttribute2());
            crUserHooks.setAttribute3(createReqPo.getAttribute3());
            crUserHooks.setAttribute4(createReqPo.getAttribute4());
            crUserHooks.setAttribute5(createReqPo.getAttribute5());
            crUserHooks.setDescription(createReqPo.getDescription());
            crUserHooks.setHookCode(createReqPo.getHookCode());
            crUserHooks.setHookName(createReqPo.getHookName());
            crUserHooks.setHookText(createReqPo.getHookText());
            crUserHooks.setHookType(createReqPo.getHookType());

            crUserHooks = crUserHooksRepo.save(crUserHooks);
            return crUserHooks;
        } catch (Exception e) {
            log.error("An error occurred while saving formula set: {}", e.getMessage());
            throw new RuntimeException("Failed to save formula set", e);
        }
    }
}
