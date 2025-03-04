package com.rite.products.convertrite.po;

import com.rite.products.convertrite.model.CrPreLoadCldSetupStatus;
import com.rite.products.convertrite.model.CrPreLoadCldSetupStatus;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class CrTemplateMetaDataJobStatusResPo {
    private CrPreLoadCldSetupStatus crPreLoadCldSetupStatus;
    private String objectName;
}
