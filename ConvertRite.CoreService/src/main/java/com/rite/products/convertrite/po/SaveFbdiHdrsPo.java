package com.rite.products.convertrite.po;

import com.rite.products.convertrite.model.CrFbdiHdrs;
import lombok.Data;

@Data
public class SaveFbdiHdrsPo {

    private CrFbdiHdrs crFbdiHdrs;
    private String message;
    private String error;
}
