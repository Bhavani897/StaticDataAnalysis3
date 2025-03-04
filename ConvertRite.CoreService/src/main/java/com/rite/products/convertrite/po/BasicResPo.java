package com.rite.products.convertrite.po;

import lombok.Data;
import org.springframework.http.HttpStatus;
@Data
public class BasicResPo {
    private String status;
    private String message;
    private Object payload;
    private HttpStatus statusCode;

}
