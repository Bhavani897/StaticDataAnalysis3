package com.rite.products.convertrite.respository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.rite.products.convertrite.model.XxrSrcTempHdrsView;

@Repository
public interface XxrSrcTempHdrsViewRepository  extends JpaRepository<XxrSrcTempHdrsView,Long>{

	XxrSrcTempHdrsView findByTemplateId(Long sourceTemplateId);

	List<XxrSrcTempHdrsView> findByRoleId(Long roleId);

}
