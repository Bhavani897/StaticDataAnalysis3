package com.rite.products.convertrite.respository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.rite.products.convertrite.model.FbdiTempHdrsView;

@Repository
public interface XxrFbdiTempHdrsViewRepository extends JpaRepository<FbdiTempHdrsView,Long>{

	List<FbdiTempHdrsView> findByRoleId(Long roleId);

}
