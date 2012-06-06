package com.azprogrammer.db;

import java.util.*;

/**
 *
 * This class is used internally as a place holder for a single record in a database.
 *
 */

public class DBRecord {

	private Map<String,Object> lookUpTable_;
	public static final String NULL_VALUE = "";
	/**
	 * DBRecord constructor.
	 */
	public DBRecord() {
		super();

	}
	public Date getDateField(String columnName) throws DBException {
		try {
			Object valInTable = getLookUpTable().get(columnName.toUpperCase());
			if ((valInTable == null) || ("".equals(valInTable))) {
				return null;
			} else {
				return new Date(((java.sql.Date) valInTable).getTime());
			}
		} catch (Exception e) {
			throw new DBException(e.toString());
		}
	}
        public java.sql.Timestamp getTimestampField(String columnName) throws DBException {
                try {
                        Object valInTable = getLookUpTable().get(columnName.toUpperCase());
                        if ((valInTable == null) || ("".equals(valInTable))) {
                                return null;
                        } else {
                                return new java.sql.Timestamp(((java.sql.Date) valInTable).getTime());
                        }
                } catch (Exception e) {
                        throw new DBException(e.toString());
                }
        }

	public double getDoubleField(String columnName) throws DBException {
		try {
			String valInTable =
				(String) (getLookUpTable().get(columnName.toUpperCase()));
			if ("".equals(valInTable.trim())) {
				return 0.0;
			}
			double retVal = (new Double(valInTable)).doubleValue();
			return retVal;
		} catch (Exception e) {
			throw new DBException(e.toString());
		}
	}

	public String getField(String columnName) throws DBException {

		try {
			String valInTable =
				(String) (getLookUpTable().get(columnName.toUpperCase()));
			return valInTable.trim();
		} catch (Exception e) {
			throw new DBException(e.toString());
		}

	}
	public int getIntField(String column) throws DBException {

		try {
			String valInTable =
				(String) (getLookUpTable().get(column.toUpperCase()));
			if ("".equals(valInTable.trim())) {
				return 0;
			}

			int retVal = Integer.parseInt(valInTable);
			return retVal;
		} catch (Exception e) {
			throw new DBException(e.toString());
		}
	}
	public long getLongField(String column) throws DBException {

		try {
			String valInTable =
				(String) (getLookUpTable().get(column.toUpperCase()));
			if ("".equals(valInTable.trim())) {
				return 0;
			}
			long retVal = Long.parseLong(valInTable);
			return retVal;
		} catch (Exception e) {
			throw new DBException(e.toString());
		}
	}
	public Map<String,Object> getLookUpTable() {
		if (lookUpTable_ == null) {
			lookUpTable_ = new HashMap <String, Object>();
		}
		return lookUpTable_;
	}
	public void putField(String column, char field) {
		getLookUpTable().put(column, new Character(field));
	}
	public void putField(String column, double field) {
		getLookUpTable().put(column, new Double(field));
	}
	public void putField(String column, float field) {
		getLookUpTable().put(column, new Float(field));
	}
	public void putField(String column, int field) {
		getLookUpTable().put(column, new Integer(field));
	}
	public void putField(String column, long field) {
		getLookUpTable().put(column, new Long(field));
	}
	public void putField(String column, String field) {
		field = DBUtil.getNoApostropheString(field);
		getLookUpTable().put(column, field);
	}
	public void putField(String column, Object field){
	    getLookUpTable().put(column, field);
	}
	public void putField(String column, java.util.Date field) {
		if (field == null) {
			getLookUpTable().put(column, DBUtil.NULL_DATE);
		}else{
                  getLookUpTable().put(column, field);
                }
	}

    public void putField(String column, java.sql.Timestamp field) {
      if (field == null) {
              getLookUpTable().put(column, DBUtil.NULL_TIMESTAMP);
      }else{
        getLookUpTable().put(column, field);
      }

    }


	public boolean getBooleanField(String columnName) throws DBException {
		try {
			String valInTable =
				(String) getLookUpTable().get(columnName.toUpperCase());
			if (valInTable != null) {
				valInTable = valInTable.trim();
			}

			if (("Y".equalsIgnoreCase(valInTable))) {
				return true;
			} else {
				return false;
			}
		} catch (Exception e) {
			throw new DBException(e.toString());
		}
	}
	public Object getObjectField(String columnName) throws DBException{
	    return getLookUpTable().get(columnName.toUpperCase ());
	}

	public java.sql.Date getSQLDateField(String columnName)
		throws DBException {
		try {
			Object valInTable = getLookUpTable().get(columnName);
			if ((valInTable == null) || ("".equals(valInTable))) {
				return null;
			} else {

				return (java.sql.Date) valInTable;
			}
		} catch (Exception e) {
			throw new DBException(e.toString());
		}
	}

	public void putField(String column, boolean field) {
		if (field == true) {
			getLookUpTable().put(column, "Y");
		} else {
			getLookUpTable().put(column, "N");
		}
	}
}
