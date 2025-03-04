package com.rite.products.convertrite.po;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

import lombok.Data;

@Data
public class CrSaveEbsConnectionDetailsReqPo {
	private Long connectionId;
	@NotBlank(message = "connectionName cannot be blank")
	private String connectionName;
	@NotBlank(message ="hostName cannot be blank")
	private String hostName;
	@NotNull(message="port cannot be null")
	private Integer port;
	@NotBlank(message="serviceName cannot be blank")
	private String serviceName;
	@NotBlank(message="userName cannot be blank")
	private String userName;
	@NotBlank(message="password cannot be blank")
	private String password;
	@NotBlank(message="connection type cannot be blank")
	private String connectionType;
}
