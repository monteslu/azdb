package com.azprogrammer.db;

/**
 * A DBSelector reprents one piece of selection criteria.<BR>
 * Each DBSelector in a DBQuery may be appended to the WHERE clause created by a DBFinder.<BR>
 * Creation date: (11/15/2000 11:11:34 AM)
 * @author: Luis Montes 
 */

public class DBSelector {
	private String columnName;
	private int type;
	private Object value;
	public static final int TYPE_EQUAL = 0;
	public static final int TYPE_INEQUAL = 1;
	public static final int TYPE_LIKE = 2;
	public static final int TYPE_GREATER_THAN = 3;
	public static final int TYPE_LESS_THAN = 4;
	public static final int TYPE_IGNORE_CASE_EQUAL = 5;
	public static final int TYPE_IN_LIST = 8;
	public static final int TYPE_LIKE_CASE_SENSITIVE = 6;
	public static final int TYPE_LITERAL_DONT_PARSE = 7;
	public static final int TYPE_GREATER_EQUAL = 9;
	public static final int TYPE_LESS_EQUAL = 10;

	/**
	 * 
	 * This constructor is used to create SQL selection criteria that is not one of the standard DBSelector types.<BR><BR>
	 * For example:<BR>
	 * 	DBSelector mySelector = new DBSelector("FIRST_NAME = SOME_SQL_FUNCTION('LUIS')");
	 */
	public DBSelector(String exactSQL) {
		super();
		setColumnName("NO_COLUMN_USED");
		setValue(exactSQL);
		setType(TYPE_LITERAL_DONT_PARSE);
	}
	/**
	 * DBSelector constructor.
	 */
	public DBSelector(String aColumnName, String columnValue) {
		super();
		setColumnName(aColumnName);
		setValue(columnValue);
		setType(TYPE_EQUAL);
	}
	
	/**
	 * DBSelector constructor.
	 */
	public DBSelector(String aColumnName, Object columnValue) {
		super();
		setColumnName(aColumnName);
		setValue(columnValue);
		setType(TYPE_EQUAL);
	}
	
	/**
	 * DBSelector constructor.
	 */
	public DBSelector(
		String aColumnName,
		String columnValue,
		int selectorType) {
		super();
		setColumnName(aColumnName);
		setValue(columnValue);
		setType(selectorType);
	}
	
	/**
	 * DBSelector constructor.
	 */
	public DBSelector(
		String aColumnName,
		Object columnValue,
		int selectorType) {
		super();
		setColumnName(aColumnName);
		setValue(columnValue);
		setType(selectorType);
	}
	/**
	 *
	 * Creation date: (11/15/2000 11:30:40 AM)
	 * @return java.lang.String
	 */
	public java.lang.String getColumnName() {
		return columnName;
	}
	public String getSQL() {
		String cn = getColumnName();
		String val = getValue();
		if ((cn == null)
			|| (val == null)
			|| (cn.trim().equals(""))
			|| (val.trim().equals(""))) {
			return null;
		}

		String sql = getColumnName().trim() + " ";
		String cleanValue = DBUtil.getNoApostropheString(val).trim();

		switch (getType()) {

			case TYPE_EQUAL :
				sql += "= '" + cleanValue + "'";
				break;

			case TYPE_LIKE :
				sql =
					"upper("
						+ getColumnName().trim()
						+ ") like '%"
						+ cleanValue.toUpperCase()
						+ "%'";
				break;

			case TYPE_LIKE_CASE_SENSITIVE :
				sql = getColumnName().trim() + " like '%" + cleanValue + "%'";
				break;

			case TYPE_INEQUAL :
				sql += "<> '" + cleanValue + "'";
				break;

			case TYPE_GREATER_THAN :
				sql += "> '" + cleanValue + "'";
				break;

			case TYPE_GREATER_EQUAL :
				sql += ">= '" + cleanValue + "'";
				break;

			case TYPE_LESS_THAN :
				sql += "< '" + cleanValue + "'";
				break;

			case TYPE_LESS_EQUAL :
				sql += "<= '" + cleanValue + "'";
				break;

			case TYPE_IGNORE_CASE_EQUAL :
				sql =
					"upper("
						+ getColumnName().trim()
						+ ") = '"
						+ cleanValue.toUpperCase()
						+ "'";
				break;

			case TYPE_LITERAL_DONT_PARSE :
				sql = val;
				break;

			case TYPE_IN_LIST :
				sql += "IN (" + val.trim() + ")";
				break;

			default :
				sql += "= '" + cleanValue + "'";
				break;
		}

		return sql;
	}
	/**
	 * 
	 * Creation date: (11/15/2000 11:30:40 AM)
	 * @return int
	 */
	public int getType() {
		return type;
	}
	/**
	 * .
	 * Creation date: (11/15/2000 11:39:05 AM)
	 * @return java.lang.String
	 */
	public java.lang.String getValue() {
		return value.toString();
	}
	/**
	 * 
	 * Creation date: (11/15/2000 11:30:40 AM)
	 * @param newColumnName java.lang.String
	 */
	public void setColumnName(java.lang.String newColumnName) {
		columnName = newColumnName;
	}
	/**
	 * 
	 * Creation date: (11/15/2000 11:30:40 AM)
	 * @param newType int
	 */
	public void setType(int newType) {
		type = newType;
	}
	/**
	 * 
	 * Creation date: (11/15/2000 11:39:05 AM)
	 * @param newValue java.lang.String
	 */
	public void setValue(Object newValue) {
		value = newValue;
	}
	
	public Object getObjectValue(){
		return value;
	}
}
