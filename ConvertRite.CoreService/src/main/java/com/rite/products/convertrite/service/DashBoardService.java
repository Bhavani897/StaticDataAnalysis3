package com.rite.products.convertrite.service;

import java.util.List;

import com.rite.products.convertrite.exception.BadRequestException;
import com.rite.products.convertrite.po.DashBoardResPo;
import com.rite.products.convertrite.po.GantChatResPo;

public interface DashBoardService {
	List<DashBoardResPo> getTask(String user) throws BadRequestException,Exception;
	
	 List<GantChatResPo> getGanttChat(String user) throws BadRequestException,Exception;

}
