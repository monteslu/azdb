package com.azprogrammer.db;

import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;
import java.util.*;


/**
 * DBUtil has various utilities for use with DBObjects and supporting classes.<BR>
 * Creation date: (5/28/00 5:11:16 PM)
 * @author: Luis Montes
 */


public class DBUtil {

	//this doesnt have to match the Database's SYSTEM DATE
	//it just creates a SQL statement that uses the Database's system date.
	public static final java.util.Date SYSDATE = new java.util.Date();

        //this doesnt have to match the Database's SYSTEM TIMESTAMP
        //it just creates a SQL statement that uses the Database's system timestamp.
        public static final java.sql.Timestamp SYSTEM_TIMESTAMP = new java.sql.Timestamp(System.currentTimeMillis());



	//this causes a "null" to be generated in the SQL statement
	public static final java.util.Date NULL_DATE = new java.util.Date(0);

        //this causes a "null" to be generated in the SQL statement
	public static final java.sql.Timestamp NULL_TIMESTAMP = new java.sql.Timestamp(0);

	private static boolean traceOn = false;

	private static DBSpecificLayer dbLayer = null;

	/**
	 * DBUtil constructor.
	 */
	public DBUtil() {
		super();
	}
	public static String clearNull(String s) {
		if (s == null) {
			return "";
		} else {
			return s;
		}
	}
	public static Date getDate(String formattedDate) {
		try {
			return getFormatter().parse(formattedDate);
		} catch (Exception e) {
			DBUtil.trace("Error retrieving date :" + e);
			return null;
		}
	}

        public static java.sql.Timestamp getTimestamp(String formattedDate) {
                try {
                        return new java.sql.Timestamp(getTimestampFormatter().parse(formattedDate).getTime());
                } catch (Exception e) {
                        DBUtil.trace("Error retrieving date :" + e);
                        return null;
                }
        }


	/**
	 *
	 * Creation date: (12/13/2000 1:17:37 PM)
	 * @return java.lang.String
	 */
	public static java.lang.String getDateFormat() {
		return getDBLayer().getDateFormat();
	}
	public static String getDBBooleanString(boolean b) {
		if (b) {
			return "Y";
		} else {
			return "N";
		}

	}
	public static String getDBDateString(Date date) {
		if (date == null) {
			return "";
		} else {
			return getFormatter().format(date);
		}

	}

        public static String getDBDateTimeString(java.sql.Timestamp aTime) {
                if (aTime == null) {
                        return "";
                } else {
                        return getFormatter().format(aTime);
                }

        }

	public static DBSpecificLayer getDBLayer() {
		if (dbLayer == null) {
			//defaults to Oracle.
			//return OracleLayer.getInstance();
                        //DBUtil.trace("defaulting to mysql");
           // return MySQLLayer.getInstance();
                        //return PostgreSQLLayer.getInstance();
		}
		return dbLayer;
	}
	public static String getDBString(String s, int size) {
		if (s == null) {
			s = "";
		}
		String Temp = new String(new char[size]);
		s = s + Temp;
		s = s.substring(0, size);
		s = s.trim();

		s = getNoApostropheString(s);

		return s;

	}
	/**
	 *
	 * Creation date: (12/13/2000 1:22:20 PM)
	 * @return java.text.SimpleDateFormat
	 */
	private static java.text.SimpleDateFormat getFormatter() {

		return getDBLayer().getDateFormatter();
	}

        private static java.text.SimpleDateFormat getTimestampFormatter() {

        return getDBLayer().getDateTimeFormatter();
      }


	/**
	 *
	 * This function delimits any apostrophes from a SQL statment by replacing them with double apostrophes.
	 */
	public static String getNoApostropheString(String s) {
		if (s == null) {
			s = "";
		}

		for (int i = 0; i < s.length(); i++) {
			if ((i == 0) && (s.toCharArray()[i] == '\'')) {
				s = '\'' + s;
				i++;
			} else {

				if (s.toCharArray()[i] == '\'') {
					s = s.substring(0, i) + '\'' + s.substring(i);
					i++;

				}

			}

		}

		return s;

	}
	public static void setDBLayer(DBSpecificLayer databaseLayer) {
		dbLayer = databaseLayer;
	}
	public static void trace(double d) {
		if (traceOn) {
			System.out.println(d);
		}
	}
	public static void trace(int i) {
		if (traceOn) {
			System.out.println(i);
		}

	}
	public static void trace(Object o) {
		if (traceOn) {
			System.out.println(o);
		}
	}

	/**
	 *
	 * Turn off logging of SQL statements and errors to System.out
	 */
	public static void traceOff() {
		traceOn = false;

	}

	/**
	 *
	 * Turn on logging of SQL statements and errors to System.out
	 */
	public static void traceOn() {
		traceOn = true;
	}

	public static String trimTillDot(String column) {
		if ((column == null) || (column.indexOf('.') == -1)) {
			return column;
		}
		int len = column.length();
		if (len < 2) {
			return column;
		}
		int dot = column.indexOf('.');

		return column.substring((dot + 1), len);

	}
	
	public static String propNameFromCol(String col){
        if(col == null || "".equals(col.trim())){
            return "";
        }
        col = col.toLowerCase();
        char[] chars = col.toCharArray();
        
        for (int i = 0; i < chars.length; i++){
            if((chars[i] == '_') && (i < (chars.length + 1))){
                chars[i+1] = Character.toUpperCase(chars[i+1]);
            }
        }
        String retVal = new String(chars);
        retVal = retVal.replace ("_","");
        
        
        return retVal;  
        
    }
	
	public static Map <String, Method> getPropertySetters (Class<Object> beanClass)
    {
        Map <String, Method> m_propertySetters = new HashMap <String, Method>();
            try{
                HashMap <String, Method> temp = new HashMap <String, Method>();
                PropertyDescriptor[] props = Introspector.getBeanInfo(beanClass).getPropertyDescriptors();
                for (int i = 0; i < props.length; i++)
                {
                    temp.put (props[i].getName (), props[i].getWriteMethod ());
                }
                m_propertySetters = temp;
            }catch(Exception e){
                trace("Error with introspection on bean class for prop setters: " + e.getMessage ());
            }

        return m_propertySetters;
    }
	
	public static void setProperties(Object bean, Map <String, Object> propValueMap, Map <String, Method> propSetterMap){
	    
	    if((bean != null) && (bean != null) && (bean != null)){
    	       
    	        for (Iterator i = propSetterMap.keySet().iterator(); i.hasNext();) {
                    String prop = (String) i.next();
                    Method method = propSetterMap.get (prop);
                    Object value = propValueMap.get (prop);
                    if(value != null){
                        try{
                            method.invoke (bean, value);
                        }catch(Exception e){
                            //no op, the prop just doesn't get set
                        }
                    }
                } 
            
	    }

        return;
    }
	
}
