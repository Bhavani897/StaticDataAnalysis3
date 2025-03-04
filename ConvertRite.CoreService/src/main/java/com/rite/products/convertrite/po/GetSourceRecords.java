package com.rite.products.convertrite.po;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Positive;
import lombok.Data;
import lombok.Getter;

@Data
@Getter
@NotNull
public class GetSourceRecords {
    private String cloudTemplateName;
    private String status;
    private String type;
    private String batchName;
    @Positive
    private Long pageNo;
    @Positive
    private Long pageSize;
}