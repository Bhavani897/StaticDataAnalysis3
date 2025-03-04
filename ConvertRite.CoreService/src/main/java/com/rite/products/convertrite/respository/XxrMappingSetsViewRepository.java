package com.rite.products.convertrite.respository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.rite.products.convertrite.model.XxrMappingSetsView;
@Repository
public interface XxrMappingSetsViewRepository extends JpaRepository<XxrMappingSetsView, Long>{

	List<XxrMappingSetsView> findByRoleId(Long roleId);

}
