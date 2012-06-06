package com.azprogrammer.db.databases;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.*;

import com.azprogrammer.db.*;

/**
 * MySQLLayer is the DBSpecificLayer implenetation to support MySQL databases.
 * Creation date: (10/17/2002 11:18:14 AM)
 * @author: Luis Montes
 */


public class MySQLLayer extends DBSpecificLayer {
	private static MySQLLayer singleton = null;
	private static SimpleDateFormat formatter = null;
        private static SimpleDateFormat dateTimeformatter = null;
        private static SimpleDateFormat dateTimeInsertformatter = null;
	private static String dateFormat = "yyyy-MM-dd";
        private static String dateTimeFormat = "yyyy-MM-dd hh:mm:ss aa";
        private static String dateTimeInsertFormat = "yyyy-MM-dd HH:mm:ss";
	private static String SYSTEM_DATE = "SYSDATE()";
	private static String SYSTEM_DATE_TIME = "NOW()";
       // private static String SYSTEM_DATE_TIME = "SYSDATE()";

	/**
	 * MySQLLayer constructor.
	 */
	protected MySQLLayer() {
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


	public static MySQLLayer getInstance() {
		if (singleton == null) {
			singleton = new MySQLLayer();
		}

		return singleton;
	}
	/**
	 *
	 * MySQL doesn't uses sequences.
	 *
	 */
	public String getSequenceNextValSQL(String sequenceName) {
		return "INSERT INTO " + sequenceName + " () VALUES ()";
	}
	public String getSystemDateConstant() {

		return SYSTEM_DATE;
	}
        public String getSystemDateTimeConstant() {

                return SYSTEM_DATE_TIME;
        }

       public String getDateTimeInsertString(java.sql.Timestamp aTimestamp){
         try{
           if(aTimestamp != null){
             return "'" + getDateTimeInsertFormatter().format(aTimestamp) + "'";
           }else{
             return "NULL";
           }
         }catch(Exception e){
             return "";
         }
      }

       public int getSequenceNextVal(String sequenceName, Connection con) throws DBException{
    	   //mysql as of version 5 still doesn't support sequences. 
    	   // We do have the HACK option of using a table with an auto-increment field as a sequence
		
    	   String sql = getSequenceNextValSQL(sequenceName);
    	   try{
    		   PreparedStatement state = con.prepareStatement(sql);
    		   state.executeUpdate();

    		   PreparedStatement statementlastID = con.prepareStatement("SELECT LAST_INSERT_ID()");
    		   ResultSet lastID = statementlastID.executeQuery();
    		   lastID.next();
    		   int retVal = lastID.getInt(1);
    		   state.close();
    		   statementlastID.close();
    		   return retVal;
    		   
    	   }catch(Exception e){
    		   throw new DBException(e.getMessage());
    	   }


	}


}
