package com.azprogrammer.db;

public class DBException extends Exception {
	/**
	 * DBException constructor.
	 */
	public DBException() {
		super();
	}
	/**
	 * DBException constructor.
	 * @param errorMessage java.lang.String
	 */
	public DBException(String errorMessage) {
		super(errorMessage);
	}
}