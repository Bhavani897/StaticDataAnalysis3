package com.rite.products.convertrite.model;


import lombok.Data;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import java.util.Date;

@Data
@Entity
@Table(name = "CR_LOOKUP_VALUES")
public class CrLookUpValues {

    @Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	@Column(name = "LOOKUP_VALUE_ID", columnDefinition = "serial")
    public Long lookUpValueId;
    
    @Column(name = "LOOKUP_SET_ID")
    public Long lookUpSetId;
    
    @Column(name = "LOOKUP_VALUE")
    public String lookUpValue;
    
    @Column(name = "ENABLED_FLAG")
    public String enabledFlag;
    
    @Column(name = "ACTUAL_VALUE")
    public String actualValue;
    
    @Column(name = "ATTRIBUTE1")
    public String attribute1;
    
    @Column(name = "ATTRIBUTE2")
    public String attribute2;
    
    @Column(name = "ATTRIBUTE3")
    public String attribute3;
    
    @Column(name = "ATTRIBUTE4")
    public String attribute4;
    
    @Column(name = "ATTRIBUTE5")
    public String attribute5;
    
    @Column(name = "CREATED_BY")
    public String createdBy;
    
    @Column(name = "CREATION_DATE")
    public Date creationDate;
    
    @Column(name = "LAST_UPDATE_DATE")
    public Date lastUpdateDate;
    
    @Column(name = "LAST_UPDATED_BY")
    public String lastUpdateBy;
}
