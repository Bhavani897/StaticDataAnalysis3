package com.rite.products.convertrite.po;

import com.rite.products.convertrite.model.CrFbdiCols;
import lombok.Data;

import java.util.List;

@Data
public class CrSaveFbdiTempColsResPo
{
    private String message;
    private List<CrFbdiCols> fbdiTemplateColumns;
    private String error;
}
