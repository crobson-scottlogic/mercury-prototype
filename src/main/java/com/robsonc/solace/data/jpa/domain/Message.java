package com.robsonc.solace.data.jpa.domain;

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

@Entity
@NoArgsConstructor
@AllArgsConstructor
@Getter
public class Message {
	@Id
	@GeneratedValue(strategy=GenerationType.AUTO)
	private Long id;
	@Setter
	private String content;
	@Setter
	private String destination;
	@Setter
	@Enumerated(EnumType.STRING)
	private DestinationType destinationType;

	public Message(String content, String destination, DestinationType destinationType) {
		this.content = content;
		this.destination = destination;
		this.destinationType = destinationType;
	}
}
