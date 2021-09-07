package com.robsonc.solace.data.jpa.domain;

import javax.persistence.Entity;
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
public class Message {
	@Id
	@Getter
	@GeneratedValue(strategy=GenerationType.AUTO)
	private Long id;
	@Getter
	@Setter
	private String content;

	public Message(String content) {
		this.content = content;
	}
}
