package com.rite.products.convertrite.respository;

import org.springframework.data.jpa.repository.JpaRepository;

import com.rite.products.convertrite.model.CrEbsConnectionDetails;


public interface CrEbsConnectionDetailsRepository extends JpaRepository<CrEbsConnectionDetails,Long>{

	CrEbsConnectionDetails findByConnectionName(String connectionName);


}
