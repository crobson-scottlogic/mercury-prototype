package com.robsonc.solace.data.jpa.domain;

import java.sql.Date;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class SearchParameters {
	private String messageVpn;
	private String queueName;
	private String destination;
	private Date earliestPublished;
	private Date latestPublished;
	private String searchText;
}
