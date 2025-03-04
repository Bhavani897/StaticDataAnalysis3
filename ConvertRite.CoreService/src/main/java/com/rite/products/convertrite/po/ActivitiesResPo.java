package com.rite.products.convertrite.po;

import java.util.List;

import com.rite.products.convertrite.model.CrProjectActivities;
import lombok.Data;

@Data
public class ActivitiesResPo {

	//private List<XxrActivities> xxrActivities;
	private List<CrProjectActivities> crActivities;
	private String message;
	private String error;

}
