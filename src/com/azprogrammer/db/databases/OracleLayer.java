package com.azprogrammer.db.databases;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.*;

import com.azprogrammer.db.*;

/**
 * OracleLayer is the DBSpecificLayer implenetation to support Oracle (TM) databases.
 * Creation date: (10/17/2002 11:18:14 AM)
 * @author: Luis Montes
 */


public class OracleLayer extends DBSpecificLayer {
	private static OracleLayer singleton = null;
	private static SimpleDateFormat formatter = null;
        private static SimpleDateFormat dateTimeformatter = null;
        private static SimpleDateFormat dateTimeInsertformatter = null;
	private static String dateFormat = "dd-MMM-yyyy";
       private static String dateTimeFormat = "dd-MMM-yyyy hh:mm:ss aa";
        private static String dateTimeInsertFormat = "dd-MMM-yyyy HH:mm:ss";
	private static String SYSTEM_DATE_ORACLE = "SYSDATE";
       // private static String SYSTEM_TIMESTAMP = "SYSTIMESTAMP";
	/**
	 * OracleLayer constructor comment.
	 */
	protected OracleLayer() {
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
	public static OracleLayer getInstance() {
		if (singleton == null) {
			singleton = new OracleLayer();
		}

		return singleton;
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


	//
	public String getSequenceNextValSQL(String sequenceName) {
		return "SELECT " + sequenceName + ".nextval SEQ from dual";
	}
	public String getSystemDateConstant() {

		return SYSTEM_DATE_ORACLE;
	}
        public String getSystemDateTimeConstant() {

                return SYSTEM_DATE_ORACLE;
        }

        public String getDateTimeInsertString(java.sql.Timestamp aTimestamp){
         try{
           if(aTimestamp != null){
             return "to_date('" + getDateTimeInsertFormatter().format(aTimestamp) + "','DD-MON-YYYY HH24:MI:SS')";
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
        	DBUtil.trace(sql);
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
