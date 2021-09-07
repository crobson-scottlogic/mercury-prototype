package com.robsonc.solace.data.jpa.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class MessageInDto {
	private String payload;
	private String messageVpn;
	private String destination;
	private String destinationType;
}
