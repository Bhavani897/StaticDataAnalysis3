package com.rite.products.convertrite.po;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class CrUserHookResPo {

    private String usageType;
    private String hookText;
    private String description;
    private String hookName;

}
