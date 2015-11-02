package com.marklogic.developer.corb;

public class CorbException extends Exception {

	private static final long serialVersionUID = 1L;

	public CorbException(String msg) {
		super(msg);
	}

	public CorbException(String msg, Throwable th) {
		super(msg, th);
	}
}
