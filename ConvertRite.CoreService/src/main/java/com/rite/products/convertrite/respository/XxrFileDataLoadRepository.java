package com.rite.products.convertrite.respository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.rite.products.convertrite.model.XxrFileDataLoad;
@Repository
public interface XxrFileDataLoadRepository extends JpaRepository<XxrFileDataLoad,String> {

}
