package com.rite.products.convertrite.po;

import lombok.AllArgsConstructor;
import lombok.Data;
import java.math.BigDecimal;

@Data
@AllArgsConstructor
public class DescriptionTableResPo {
    private String columnName;
    private String dataType;
    private String nullableFlag;
    private BigDecimal seq;
}
