package com.azprogrammer.db;

import java.util.*;

/**
 * A DBKey represents the Primary Key of a DBObject.<BR>
 * Creation date: (10/10/2000 1:29:56 PM)
 * @author: Luis Montes
 */


public abstract class DBKey {
	private Map lookupTable = null;
	private DBRecord record = null;
	/**
	 * DBKey constructor.
	 */
	public DBKey() {
		super();
	}
	public abstract DBRecord getKeyColumns();
	protected DBRecord getRecord() {
		if (record == null) {
			record = new DBRecord();
		}
		return record;
	}
	public String getWhereClause() {
		String sql = "WHERE ";
		Map cols = getKeyColumns().getLookUpTable();
		int size = cols.size();

		//Collection values = cols.values();
		Collection keys = cols.keySet();
		//Iterator valIt = values.iterator();
		Iterator keyIt = keys.iterator();
		
		for (int i = 0; i < size; i++) {
			String colName = keyIt.next().toString();
			Object colValue = cols.get(colName);
			String colStrValue = null;
			if (colValue instanceof java.util.Date) {
				colStrValue = DBUtil.getDBDateString((java.util.Date) colValue);
			} else {
				colStrValue = colValue.toString();
			}
			if (i == (size - 1)) {
				sql = sql + colName + " = '" + colStrValue + "'";
			} else {
				sql = sql + colName + " = '" + colStrValue + "' AND ";
			}
		}
		return sql;
	}
}