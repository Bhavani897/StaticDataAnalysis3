package com.rite.products.convertrite.model;

import java.util.Date;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import lombok.Data;

@Entity
@Table(name = "CR_ASYNC_PROCESS_STATUS")
@Data
public class AsyncProcessStatus {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "ASYNC_PROCESS_ID", nullable = false)
    private Long asyncProcessId;

    @Column(name = "ASYNC_PROCESS_NAME", nullable = false, length = 255)
    private String asyncProcessName;

    @Column(name = "BATCH_NAME", nullable = false)
    private String batchName;

    @Column(name = "ASYNC_START_TIME", nullable = false)
    private Date asyncStartTime;

    @Column(name = "ASYNC_END_TIME")
    private Date asyncEndTime;

    @Column(name = "ASYNC_PROCESS_STATUS", nullable = false, length = 50)
    private String asyncProcessStatus;

    @Column(name = "ERROR_MESSAGE", length = 4000)
    private String errorMessage;

    @Column(name = "CREATED_BY", length = 50)
    private String createdBy;

    @Column(name = "CREATED_DATE", updatable = false)
    private Date createdDate;

    @Column(name = "LAST_UPDATED_BY", length = 50)
    private String updatedBy;

    @Column(name = "LAST_UPDATED_DATE")
    private Date updatedDate;
}

