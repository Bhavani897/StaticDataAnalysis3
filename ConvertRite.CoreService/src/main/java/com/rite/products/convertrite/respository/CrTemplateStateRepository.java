package com.rite.products.convertrite.respository;

import com.rite.products.convertrite.model.CrTemplateStateView;
import org.springframework.data.jpa.repository.JpaRepository;

public interface CrTemplateStateRepository extends JpaRepository<CrTemplateStateView,String> {

    CrTemplateStateView findBySrcTemplateId(Long templateId);
}
