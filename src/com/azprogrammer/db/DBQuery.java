package com.azprogrammer.db;

import java.util.*;

/**
 * A DBQuery is used to create the WHERE clause in a SQL query.<BR>
 * Creation date: (11/15/2000 11:05:22 AM)
 * @author: Luis Montes
 */

public abstract class DBQuery extends PropertyChanger {
	private java.lang.String fieldOrderByColumn = new String();
	private boolean fieldOrderByDescending = false;
	/**
	 * DBQuery constructor.
	 */
	public DBQuery() {
		super();
	}
	/**
	 * Gets the orderByColumn property (java.lang.String) value.
	 * @return The orderByColumn property value.
	 * @see #setOrderByColumn
	 */
	public java.lang.String getOrderByColumn() {
		return fieldOrderByColumn;
	}
	/**
	 * Gets the orderByDescending property (boolean) value.
	 * @return The orderByDescending property value.
	 * @see #setOrderByDescending
	 */
	public boolean getOrderByDescending() {
		return fieldOrderByDescending;
	}
	public String getOrderClause() {
		String retVal = null;
		String orderByStr = getOrderByColumn();
		if ((orderByStr != null) && (!orderByStr.trim().equals(""))) {
			retVal = "order by " + orderByStr;
			if (getOrderByDescending()) {
				retVal += " desc";
			}
		}

		return retVal;
	}
	abstract public List<DBSelector> getSelectors();
	
	public String getWhereClause() {
		List<DBSelector> rawSelectors = getSelectors();

		// no selectors = no where clause
		if ((rawSelectors == null)) {
			return null;
		}

		List<DBSelector> selectors = new ArrayList<DBSelector>();

		//take out all invalid selectors
		for (int i = 0; i < rawSelectors.size(); i++) {
			if (rawSelectors.get(i) instanceof DBSelector) {
				DBSelector selector = (DBSelector) rawSelectors.get(i);
				if (selector.getSQL() != null) {
					selectors.add (selector);
				}
			}

		}

		int numSelectors = selectors.size();

		if (numSelectors == 0) {
			return null;
		}

		//create the where clause
		String clause = "WHERE ";
		int appendedSelections = 0;
		for (int i = 0; i < numSelectors; i++) {
			DBSelector select = (DBSelector) selectors.get(i);
			String sqlPiece = select.getSQL();
			clause += sqlPiece;
			appendedSelections++;
			if (i != (numSelectors - 1)) {
				clause += " AND ";
			}
		}

		//we dont just want "where"
		if (appendedSelections == 0) {
			return null;
		}

		return clause;
	}
	/**
	 * Sets the orderByColumn property (java.lang.String) value.
	 * @param orderByColumn The new value for the property.
	 * @see #getOrderByColumn
	 */
	public void setOrderByColumn(java.lang.String orderByColumn) {
		String oldValue = fieldOrderByColumn;
		fieldOrderByColumn = orderByColumn;
		firePropertyChange("orderByColumn", oldValue, orderByColumn);
	}
	/**
	 * Sets the orderByDescending property (boolean) value.
	 * @param orderByDescending The new value for the property.
	 * @see #getOrderByDescending
	 */
	public void setOrderByDescending(boolean orderByDescending) {
		boolean oldValue = fieldOrderByDescending;
		fieldOrderByDescending = orderByDescending;
		firePropertyChange(
			"orderByDescending",
			new Boolean(oldValue),
			new Boolean(orderByDescending));
	}
}
