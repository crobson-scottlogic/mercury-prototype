package com.robsonc.solace.data.jpa.domain;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class SearchParameters {
	private String messageVpn;
	private String queueName;
	private String destination;
	private Long earliestPublished;
	private Long latestPublished;
	private String searchText;
}
