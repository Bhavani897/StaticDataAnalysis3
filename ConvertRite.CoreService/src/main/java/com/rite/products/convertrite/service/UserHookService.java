package com.rite.products.convertrite.service;

import com.rite.products.convertrite.exception.ValidationException;
import com.rite.products.convertrite.model.XxrUserHooksDetailsView;
import com.rite.products.convertrite.po.SaveUserHooksResPo;
import com.rite.products.convertrite.po.UserHooksDetailsReqPo;

public interface UserHookService {

	SaveUserHooksResPo saveUserHooksDetails(UserHooksDetailsReqPo userHooksDetailsReqPo)throws ValidationException;

	XxrUserHooksDetailsView getUserHooksDtls(Long cldTemplateId);

}
