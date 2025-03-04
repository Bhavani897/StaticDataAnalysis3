package com.rite.products.convertrite.service;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.rite.products.convertrite.model.XxrUsers;
import com.rite.products.convertrite.po.UserRegistrationStatusRes;
import com.rite.products.convertrite.po.XxrUserResPo;
import com.rite.products.convertrite.respository.XxrUsersRepository;

@Service
public class UserServiceImpl implements UserService {

	private static final Logger log = LoggerFactory.getLogger(UserServiceImpl.class);

	@Autowired
	XxrUsersRepository xxrUsersRepository;

	public XxrUserResPo registerUser(XxrUsers xxrUsers) throws Exception {
		log.info("Start of registerUser Method#####");
		XxrUserResPo xxrUserResPo = new XxrUserResPo();
		try {
			XxrUsers userExists = xxrUsersRepository.findByNameIgnoreCaseAndEmailIdIgnoreCase(xxrUsers.getName(),
					xxrUsers.getEmailId());
			if (userExists != null) {
				userExists.setIsAdmin(xxrUsers.getIsAdmin());
				userExists.setEmailId(xxrUsers.getEmailId());
				userExists.setName(xxrUsers.getName());
				xxrUsersRepository.save(userExists);
				xxrUserResPo.setMessage("Combination of this UserName & EmailId Already Exists");
				xxrUserResPo.setXxrUser(userExists);
			} else {
				XxrUsers xxrUser = xxrUsersRepository.save(xxrUsers);
				xxrUserResPo.setXxrUser(xxrUser);
				xxrUserResPo.setMessage("Successfully Registered User");
			}
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return xxrUserResPo;
	}

	@Override
	public UserRegistrationStatusRes userRegistrationStatus(String userName) throws Exception {
		log.info("Start of userRegistrationStatus in Service ####");
		UserRegistrationStatusRes userRegistrationStatusRes = new UserRegistrationStatusRes();
		try {
			XxrUsers user = xxrUsersRepository.findByNameIgnoreCase(userName);
			if (user == null)
				userRegistrationStatusRes.setUserRegStatus("NotReg");
			else {
				userRegistrationStatusRes.setUserRegStatus("Reg");
			}

		} catch (Exception e) {
			log.error(e.getMessage());
			throw new Exception(e.getMessage());
		}
		return userRegistrationStatusRes;
	}

	@Override
	public List<XxrUsers> getUsers() throws Exception {
		log.info("Start of getUsers Method######");
		List<XxrUsers> usersLi = new ArrayList<>();
		try {
			usersLi = xxrUsersRepository.findAll();
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return usersLi;
	}

	@Override
	public UserRegistrationStatusRes userRegistrationStatusByEmail(String email) throws Exception {
		log.info("Start of userRegistrationStatus in Service ####");
		UserRegistrationStatusRes userRegistrationStatusRes = new UserRegistrationStatusRes();
		try {
			XxrUsers user = xxrUsersRepository.findByEmailIdIgnoreCase(email);
			if (user == null)
				userRegistrationStatusRes.setUserRegStatus("NotReg");
			else {
				userRegistrationStatusRes.setUserRegStatus("Reg");
			}

		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return userRegistrationStatusRes;
	}

}
