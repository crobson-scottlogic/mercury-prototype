package com.robsonc.solace.data.jpa.domain;

import java.sql.Date;
import java.util.Objects;

import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Entity
@NoArgsConstructor
@AllArgsConstructor
@Getter
@ToString
public class QueueMessage {
	@Id
	@GeneratedValue(strategy=GenerationType.AUTO)
	private Long id;
	@Setter
	private String content;
	@Setter
	private String messageVpn;
	@Setter
	private String destination;
	@Setter
	@Enumerated(EnumType.STRING)
	private DestinationType destinationType;
	@Setter
	private String applicationId;
	private Date published;

	public QueueMessage(String content, String messageVpn, String destination, DestinationType destinationType, String applicationId) {
		this.content = Objects.requireNonNull(content);
		this.messageVpn = messageVpn;
		this.destination = Objects.requireNonNull(destination);
		this.destinationType = Objects.requireNonNull(destinationType);
		this.applicationId = Objects.requireNonNull(applicationId);
	}
}
