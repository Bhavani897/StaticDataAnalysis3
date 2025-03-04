package com.rite.products.convertrite.model;


import lombok.Data;

import javax.persistence.Column;
import java.io.Serializable;

@Data
public class CrValidateCvrCcidId implements Serializable {
    private static final long serialVersionUID = 1L;

    @Column(name = "CR_BATCH_NAME")
    private String crBatchName;
    @Column(name = "CCID")
    private String ccid;
    @Column(name = "REQUEST_ID")
    private Long requestId;
}
