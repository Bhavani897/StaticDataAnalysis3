package com.rite.products.convertrite.model;

import lombok.Data;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;
import org.hibernate.validator.constraints.UniqueElements;

import javax.persistence.*;
import java.util.Date;

@Entity
@Data
@Table(name="CR_CLOUD_LOG_DETAILS")
public class CrCloudLogDetails {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "LOG_ID")
    private long logId;
    @Column(name = "LOAD_REQUEST_ID")
    private long loadRequestId;
    @Column(name = "LOG_FILE")
    private byte[] logFileZip;
    @Column(name = "LAST_UPDATE_DATE")
    @UpdateTimestamp
    private Date lastUpdatedDate;
    @Column(name = "LAST_UPDATED_BY")
    private String lastUpdatedBy = "ConvertRite-Core";
    @Column(name = "CREATION_DATE")
    @CreationTimestamp
    private Date creationDate;
    @Column(name = "CREATED_BY")
    private String createdBy = "ConvertRite-Core";
}
