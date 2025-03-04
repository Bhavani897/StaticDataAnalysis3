package com.rite.products.convertrite.respository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.rite.products.convertrite.model.XxrUserHooksDetailsView;

@Repository
public interface XxrUserHooksDetailsViewRepository extends JpaRepository<XxrUserHooksDetailsView,Long>{

	XxrUserHooksDetailsView findByCldTemplateId(Long cldTemplateId);

}
