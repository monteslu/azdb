package com.azprogrammer.db.spring;

import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.jdbc.core.SqlParameter;
import org.springframework.jdbc.object.SqlUpdate;

import com.azprogrammer.db.*;

public class DBUpdate extends SqlUpdate {

	Object dBObject = null;
	DBDirector m_director;
	List paramValues = null;
	protected DBSpecificLayer dBLayer = null;
	
	public DBUpdate(DataSource ds, Object obj, DBDirector dbDirector) {
		super();
		setDataSource(ds);
		setDBObject(obj);
		setDirector (dbDirector);
	}
	


	public int delete(){
		String sql =
			"DELETE FROM "
				+ getDirector ().getFQTableName()
				+ " WHERE "
				+ buildKeyParams(getDirector ().createKey (getDBObject()));
		setSql(sql);
		return update(getParamValues().toArray());
	}
	
	public void buildInsert() throws SQLException,DBException {
		
		String sql = "INSERT INTO " + getDirector ().getFQTableName() + " (";
		String sqlCols = "";
		String sqlVals = "";

		Map cols = getDirector ().fillDatastore(getDBObject ()).getLookUpTable();
		int size = cols.size();

		// Collection values = cols.values();
		Collection keys = cols.keySet();
		// Iterator valIt = values.iterator();
		Iterator keyIt = keys.iterator();
		for (int i = 0; i < size; i++) {
			String colName = keyIt.next().toString();
			Object colValue = cols.get(colName);

			sqlCols = sqlCols + colName;

			
			if (colValue == DBUtil.SYSDATE) {
				sqlVals = sqlVals + getDBLayer().getSystemDateConstant();
			} else if (colValue == DBUtil.SYSTEM_TIMESTAMP) {
				sqlVals = sqlVals + getDBLayer().getSystemDateTimeConstant();
			} else if (colValue == DBUtil.NULL_DATE) {
				sqlVals = sqlVals + "null";
			} else if (colValue == DBUtil.NULL_TIMESTAMP) {
				sqlVals = sqlVals + "null";
			} else if (colValue instanceof java.util.Date) { // this goes for
																// date or
																// timestamp
				sqlVals = sqlVals + "?";
				declareParameter(new SqlParameter(Types.DATE) );
				getParamValues().add(colValue);
				
			}else if (colValue == null) {
                sqlVals = sqlVals + "null";
            } else {
				sqlVals = sqlVals + "?";
				declareParameter(new SqlParameter(Types.VARCHAR));
				getParamValues().add(colValue);
			}

			if (i != (size - 1)) {
				sqlCols += ",";
				sqlVals += ",";
			}
		}
		sql = sql + sqlCols + ") VALUES (" + sqlVals + ")";
		setSql(sql);
		
		
		return;
	}

	public void buildUpdate() throws SQLException,DBException {
		
		String sql = "Update " + getDirector ().getFQTableName()
				+ " SET ";

		Map cols = getDirector ().fillDatastore(getDBObject ()).getLookUpTable();
		int size = cols.size();

		DBKey key = getDirector ().createKey(getDBObject());
		// make sure they don't update the whole table
		if (key.getKeyColumns().getLookUpTable().size() < 1) {
			throw new SQLException("Invalid key used for update");
		}

		// Collection values = cols.values();
		Collection keys = cols.keySet();
		// Iterator valIt = values.iterator();
		Iterator keyIt = keys.iterator();

		for (int i = 0; i < size; i++) {
			String colName = keyIt.next().toString();
			Object colValue = cols.get(colName);

			if (colValue == DBUtil.SYSDATE) {
				sql = sql + colName + " = "
						+ getDBLayer().getSystemDateConstant();
			} else if (colValue == DBUtil.NULL_DATE) {
				sql = sql + colName + " = null";
			} else if (colValue == DBUtil.NULL_TIMESTAMP) {
				sql = sql + colName + " = null";
			} else if (colValue == DBUtil.SYSTEM_TIMESTAMP) {
				sql = sql + colName + " = "
						+ getDBLayer().getSystemDateTimeConstant();
			} else if (colValue instanceof java.util.Date) { // this goes for
																// date or
																// timestamp
				sql = sql + colName + " = ? ";
				declareParameter(new SqlParameter(Types.DATE));
				getParamValues().add(colValue);
			} else if (colValue == null) {
			    sql = sql + colName + " = null ";
            } else {
				sql = sql + colName + " = ? ";
				declareParameter(new SqlParameter(Types.VARCHAR));
				getParamValues().add(colValue);
			}

			if (i != (size - 1)) {
				sql += ", ";
			}
		}
		sql = sql + " WHERE ";
		sql = sql + buildKeyParams(key);
		
		
		setSql(sql);
		return;

	}

	public String buildKeyParams(DBKey key){
		
		String sql = "";
		Map keyCols = key.getKeyColumns().getLookUpTable();
		int keyColSize = keyCols.size();

		Collection keyKeys = keyCols.keySet();
		Iterator keyKeyIt = keyKeys.iterator();
		
		for (int i = 0; i < keyColSize; i++) {
			String colName = keyKeyIt.next().toString();
			Object colValue = keyCols.get(colName);
			if (i == (keyColSize - 1)) {
				sql = sql + colName + " = ? ";
			} else {
				sql = sql + colName + " = ? AND ";
			}			
			if (colValue instanceof java.util.Date) {
				declareParameter(new SqlParameter(Types.DATE));
				getParamValues().add(colValue);
			} else {
				declareParameter(new SqlParameter(Types.VARCHAR));
				getParamValues().add(colValue.toString());
			}
			
		}
		
		return sql;
	}

	public Object getDBObject() {
		return dBObject;
	}

	public void setDBObject(Object object) {
		dBObject = object;
	}

	public List getParamValues() {
		if(paramValues == null){
			paramValues = new ArrayList();
		}
		return paramValues;
	}

	public void setParamValues(List paramValues) {
		this.paramValues = paramValues;
	}
/*
	public int update(){
		try{
			buildStatement();
			DBUtil.trace (getSql ());
			return super.update(getParamValues().toArray());
		}catch(Exception e){
			e.printStackTrace();
			return Integer.MIN_VALUE;
		}
	}
*/
	public DBSpecificLayer getDBLayer() {
		return dBLayer;
	}

	public void setDBLayer(DBSpecificLayer layer) {
		dBLayer = layer;
	}








    public DBDirector getDirector ()
    {
        return m_director;
    }



    public void setDirector (DBDirector director)
    {
        m_director = director;
    }
}
