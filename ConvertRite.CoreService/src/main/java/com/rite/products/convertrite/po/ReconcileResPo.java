package com.rite.products.convertrite.po;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;

@Data
public class ReconcileResPo {
    String resCode;
    String resMsg;
    String clobString;
}
