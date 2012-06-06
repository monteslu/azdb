package com.azprogrammer.db;

import java.sql.Connection;

/**
 * A DBSpecificLayer is used to support different databases.<BR>
 * Some databases add or remove things from the SQL standard. <BR>
 * This layer helps to keep DBObjects database-independant.<BR><BR>
 *
 * Subclasses should be singletons.<BR><BR>
 *
 * Creation date: (10/17/2002 11:11:29 AM)
 * @author: Luis Montes
 */

public abstract class DBSpecificLayer {
  public abstract String getDateFormat();

  public abstract java.text.SimpleDateFormat getDateFormatter();

  public abstract java.text.SimpleDateFormat getDateTimeFormatter();

  //public abstract java.text.SimpleDateFormat getDateTimeFormatter();
  //public abstract String getSequenceNextValSQL(String sequenceName);
  
  public abstract int getSequenceNextVal(String sequenceName, Connection con) throws DBException;

  public abstract String getSystemDateConstant();

  public abstract String getSystemDateTimeConstant();

  public String getDateInsertString(java.util.Date aDate) {

    return "'" + DBUtil.getDBDateString(aDate) + "'";

  }

  //this will usually be overridden.
  public String getDateTimeInsertString(java.sql.Timestamp aTimestamp){
         return "'" + DBUtil.getDBDateTimeString(aTimestamp) + "'";
  }

}

