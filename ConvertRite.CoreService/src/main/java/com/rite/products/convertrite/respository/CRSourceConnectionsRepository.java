package com.rite.products.convertrite.respository;

import com.rite.products.convertrite.model.CRSourceConnections;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface CRSourceConnectionsRepository extends JpaRepository<CRSourceConnections,Long> {
    boolean existsByStorageName(String storageName);

}
