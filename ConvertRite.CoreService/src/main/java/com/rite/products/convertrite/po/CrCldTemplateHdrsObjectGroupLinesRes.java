package com.rite.products.convertrite.po;

import com.rite.products.convertrite.model.CrCloudTemplateHeadersView;
import com.rite.products.convertrite.model.CrObjectGroupLinesView;
import lombok.Data;

@Data
public class CrCldTemplateHdrsObjectGroupLinesRes {
    private CrObjectGroupLinesView objectGroupLinesView;
    private CrCloudTemplateHeadersView cloudTemplateHeadersView;
    public CrCldTemplateHdrsObjectGroupLinesRes(CrObjectGroupLinesView objectGroupLinesView,CrCloudTemplateHeadersView cloudTemplateHeadersView){
        this.objectGroupLinesView=objectGroupLinesView;
        this.cloudTemplateHeadersView=cloudTemplateHeadersView;
    }
}

