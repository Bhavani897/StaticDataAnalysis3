package com.rite.products.convertrite.respository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.rite.products.convertrite.model.XxrFormulaSetsView;

@Repository
public interface XxrFormulaSetsViewRepository  extends JpaRepository<XxrFormulaSetsView,Long>{

	List<XxrFormulaSetsView> findByRoleId(Long roleId);

}
