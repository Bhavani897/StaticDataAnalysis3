package com.rite.products.convertrite.respository;

import com.rite.products.convertrite.model.AsyncProcessStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface AsyncProcessStatusRepository extends JpaRepository<AsyncProcessStatus, Long> {

}

