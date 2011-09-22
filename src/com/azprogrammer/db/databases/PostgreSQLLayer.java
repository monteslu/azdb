package com.azprogrammer.db.databases;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.*;

import com.azprogrammer.db.*;

/**
 * PostgreSQLLayer is the DBSpecificLayer implenetation to support PostgreSQL databases.
 * Creation date: (10/17/2002 11:18:14 AM)
 * @author: Luis Montes
 */


public class PostgreSQLLayer extends DBSpecificLayer {
	private static PostgreSQLLayer singleton = null;
	private static SimpleDateFormat formatter = null;
        private static SimpleDateFormat dateTimeformatter = null;
        private static SimpleDateFormat dateTimeInsertformatter = null;

	private static String dateFormat = "yyyy-MM-dd";
        private static String dateTimeFormat = "dd-MMM-yyyy hh:mm:ss aa";
        private static String dateTimeInsertFormat = "dd-MMM-yyyy HH:mm:ss";

	private static String SYSTEM_DATE = "CURRENT_DATE";
        private static String SYSTEM_TIMESTAMP = "CURRENT_TIMESTAMP";
	/**
	 * PostgreSQLLayer constructor.
	 */
	protected PostgreSQLLayer() {
		super();
	}
	public String getDateFormat() {
		return dateFormat;
	}
	public SimpleDateFormat getDateFormatter() {
		if (formatter == null) {
			formatter = new SimpleDateFormat(dateFormat);
		}
		return formatter;
	}
	public static PostgreSQLLayer getInstance() {
		if (singleton == null) {
			singleton = new PostgreSQLLayer();
		}

		return singleton;
	}
	//
	public String getSequenceNextValSQL(String sequenceName) {
		return "select nextval('" + sequenceName + "') as SEQ";
	}
	public String getSystemDateConstant() {

		return SYSTEM_DATE;
	}

        public SimpleDateFormat getDateTimeFormatter() {
       if (dateTimeformatter == null) {
               dateTimeformatter = new SimpleDateFormat(dateTimeFormat);
       }
       return dateTimeformatter;
     }

     public SimpleDateFormat getDateTimeInsertFormatter() {
       if (dateTimeInsertformatter == null) {
               dateTimeInsertformatter = new SimpleDateFormat(dateTimeInsertFormat);
       }
       return dateTimeInsertformatter;
     }

        public String getSystemDateTimeConstant() {

                return SYSTEM_TIMESTAMP;
        }

        public String getDateTimeInsertString(java.sql.Timestamp aTimestamp){
         try{
           if(aTimestamp != null){
             return "to_timestamp('" + getDateTimeInsertFormatter().format(aTimestamp) + "','DD-MON-YYYY HH24:MI:SS')";
           }else{
             return "NULL";
           }
         }catch(Exception e){
             return "";
         }
      }
        
        public int getSequenceNextVal(String sequenceName, Connection con)
		throws DBException {
	String sql = getSequenceNextValSQL(sequenceName);
	try {
		PreparedStatement state = con.prepareStatement(sql);
		
		ResultSet rs = state.executeQuery(sql);
		rs.next();
		int retVal = Integer.parseInt(rs.getString("SEQ"));
		state.close();
		return retVal;

	} catch (Exception e) {
		throw new DBException(e.getMessage());
	}
}

}
