package com.robsonc.solace.data.jpa.service;

import com.solacesystems.jcsmp.BytesXMLMessage;

public class MsgInfo {

	public volatile boolean acked = false;
	public volatile boolean publishedSuccessfully = false;
	public BytesXMLMessage sessionIndependentMessage = null;
	public final long id;

	public MsgInfo(long id) {
		this.id = id;
	}

	@Override
	public String toString() {
		return "MsgInfo [acked=" + acked + ", id=" + id + ", publishedSuccessfully=" + publishedSuccessfully + "]";
	}
}

