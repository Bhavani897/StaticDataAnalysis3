package com.rite.products.convertrite.model;

import java.io.Serializable;
import javax.persistence.IdClass;
import lombok.Data;

@Data

public class CrScheduleJobViewId implements Serializable {

    private Long scheduleId;
    private String batchName;
    private Long sourceTemplateId;
}
