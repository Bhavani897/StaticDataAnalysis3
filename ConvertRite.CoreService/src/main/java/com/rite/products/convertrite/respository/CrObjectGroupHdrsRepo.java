package com.rite.products.convertrite.respository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import com.rite.products.convertrite.model.CrObjectGroupHdrs;
import com.rite.products.convertrite.model.CrObjectGroupHdrsView;
import org.springframework.stereotype.Repository;

@Repository
public interface CrObjectGroupHdrsRepo  extends JpaRepository<CrObjectGroupHdrs, Long>{

	@Query("SELECT C FROM CrObjectGroupHdrs C WHERE C.groupCode = :groupCode")
	public CrObjectGroupHdrs findByGroupCode(String groupCode);
	
	@Query("SELECT C FROM CrObjectGroupHdrsView C")
	public List<CrObjectGroupHdrsView> getAllObjectGroupsHdrs();


}
