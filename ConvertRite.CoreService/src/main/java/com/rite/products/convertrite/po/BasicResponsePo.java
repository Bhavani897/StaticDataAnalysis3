package com.rite.products.convertrite.po;

import lombok.Data;

@Data
public class BasicResponsePo {
    private String error;
    private String message;
    private Object payload;
}
