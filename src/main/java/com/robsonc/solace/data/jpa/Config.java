package com.robsonc.solace.data.jpa;

import com.solacesystems.jcsmp.JCSMPProperties;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class Config {

	@Value("${solace.host:localhost}")
	private String host;

	@Value("${solace.username:}")
	private String username;

	//No! This should not be a hardcoded property of the application but must be chosen by clients
	@Value("${solace.vpn_name:default}")
	private String vpnName;

	@Value("${solace.password:}")
	private String password;

	@Bean
	public JCSMPProperties properties() {
		JCSMPProperties properties = new JCSMPProperties();
		properties.setProperty(JCSMPProperties.HOST, host);
		properties.setProperty(JCSMPProperties.USERNAME, username);
		properties.setProperty(JCSMPProperties.VPN_NAME, vpnName);
		properties.setProperty(JCSMPProperties.PASSWORD, password);
		return properties;
	}
}
