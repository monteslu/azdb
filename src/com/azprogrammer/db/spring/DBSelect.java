package com.azprogrammer.db.spring;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.SqlParameter;
import org.springframework.jdbc.object.MappingSqlQuery;

import com.azprogrammer.db.DBDirector;
import com.azprogrammer.db.DBKey;
import com.azprogrammer.db.DBQuery;
import com.azprogrammer.db.DBRecord;
import com.azprogrammer.db.DBSelector;
import com.azprogrammer.db.DBStateAwareBean;
import com.azprogrammer.db.DBUtil;

public class DBSelect extends MappingSqlQuery implements RowCallbackHandler {

	
	protected DBDirector m_director = null;
	protected List paramValues = null;
	protected DBQuery dBQuery = null;
	protected List<Object> m_resultPage;
	protected int m_pageSize = 0;
	protected int m_pageNumber = 0;
	protected int m_currentRecordNumber = 0;
	protected DataSource m_localDatasouceHandle;

	public DBSelect(DataSource dataSource, DBDirector director){
        super();        
        setDataSource(dataSource);
        setDirector(director);
        setLocalDatasouceHandle (dataSource);
    }
	
	public DBSelect(DataSource dataSource, String sql,DBDirector director) {
		super(dataSource, sql);
		setDirector (director);
		setLocalDatasouceHandle (dataSource);
	}
	
	
	public String buildAllQueryParams(boolean includeOrderBy){
		String sql = "";
		
		if(getDBQuery() != null){
			List selectors = getDBQuery().getSelectors();
			if(selectors.size() == 0){
				return sql;
			}else{
				String paramsSQL = "";
				ArrayList sqlPieces = new ArrayList(selectors.size());				
				for (int i = 0; i < selectors.size(); i++) {
                   DBSelector select = (DBSelector) selectors.get (i);
                   String sqlPiece = buildSelectorParam(select);
                   if(!"".equals (sqlPiece)){
                       sqlPieces.add (sqlPiece);
                   }
                }
				for (int i = 0; i < sqlPieces.size(); i++) {
					paramsSQL += sqlPieces.get (i).toString ();
					if (i != (sqlPieces.size() - 1)) {
						paramsSQL += " AND ";
					}
				}
				if(!"".equals(paramsSQL.trim())){
					sql = sql + " WHERE " + paramsSQL;
				}
				
			}
			
			if(includeOrderBy){
    			String order = getDBQuery().getOrderClause();
    			if ((order != null) && (!order.trim().equals(""))) {
    				sql = sql + " " + order;
    			}
			}
			
		}
		
		return sql;
	}
	

	
	public String buildSelectorParam(DBSelector selector){
		String sql = "";
		String cn = selector.getColumnName();
		Object val = selector.getObjectValue();
		if ((cn == null)
			|| (val == null)
			|| (cn.trim().equals(""))
			|| ((val instanceof String) && ( ((String)val).trim().equals("")))) {
			return sql;
		}

		sql = selector.getColumnName().trim() + " ";
		boolean needsParamBound = true;
		switch (selector.getType()) {

			case DBSelector.TYPE_EQUAL :
				sql += "= ? ";
				break;

			case DBSelector.TYPE_LIKE :
				sql =
					" upper("
						+ selector.getColumnName().trim()
						+ ") like ? ";
				val = "%" + val.toString().toUpperCase() + "%";
				break;

			case DBSelector.TYPE_LIKE_CASE_SENSITIVE :
				sql = selector.getColumnName().trim() + " like ? ";
				val = "%" + val.toString() + "%";
				break;

			case DBSelector.TYPE_INEQUAL :
				sql += "<> ? ";
				break;

			case DBSelector.TYPE_GREATER_THAN :
				sql += "> ? ";
				break;

			case DBSelector.TYPE_GREATER_EQUAL :
				sql += ">= ? ";
				break;

			case DBSelector.TYPE_LESS_THAN :
				sql += "< ? ";
				break;

			case DBSelector.TYPE_LESS_EQUAL :
				sql += "<= ? ";
				break;

			case DBSelector.TYPE_IGNORE_CASE_EQUAL :
				sql =
					"upper("
						+ selector.getColumnName().trim()
						+ ") = ? ";
				val = val.toString().toUpperCase();
				break;

			case DBSelector.TYPE_LITERAL_DONT_PARSE :
				sql = val.toString();
				needsParamBound = false;
				break;

			case DBSelector.TYPE_IN_LIST :
				sql += "IN (?)";
				val = val.toString();
				break;

			default :
				sql += "= ? ";
				break;
		}
		
		if(needsParamBound){
			if(val instanceof java.util.Date){
				declareParameter(new SqlParameter(Types.DATE));
				getParamValues().add(val);
			}else{
				declareParameter(new SqlParameter(Types.VARCHAR));
				getParamValues().add(val.toString());
			}
		}
		
		return sql;
	}
	
	public List getAll(){
		String sql = buildFullQuery (true);
		setSql(sql);
		DBUtil.trace (sql);
		return execute(getParamValues().toArray());
		
	}
	
	public List<Object> getPage(int pageSize, int pageNumber){
	    if((pageSize < 1) || (pageNumber < 1)){
	        return new ArrayList <Object> ();
	    }
	    setPageNumber (pageNumber);
	    setPageSize (pageSize);
        String sql = buildFullQuery (true);
        setSql(sql);
        DBUtil.trace (sql);
        JdbcTemplate template = new JdbcTemplate (getLocalDatasouceHandle ());
        template.query (sql, getParamValues().toArray(), this);
        return getResultPage ();
        
    }
	
	public String buildFullQuery(boolean includeOrderBy){
	    return "SELECT "
        + getDirector ().getColumnSelectionSQL().trim()
        + " FROM "
        + getDirector().getFQTableName(getDBQuery())
        + buildAllQueryParams(includeOrderBy);
	}

	
	public Object findObject(DBKey key){
		String sql =
			"SELECT "
				+ getDirector().getColumnSelectionSQL().trim()
				+ " FROM "
				+ getDirector().getFQTableName(getDBQuery())
				+ " WHERE ";
		
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
		setSql(sql);
		Object obj = findObject(getParamValues().toArray());
		return obj;
	}
	protected Object mapRow(ResultSet rs, int rowNum) throws SQLException {
		try{
			DBRecord rec = getRecord(rs);
			Object dbo = getDirector().newObject();
			getDirector ().fillObject (rec, dbo);
			if(getDirector ().isDbAwareBeans ()){
			    ((DBStateAwareBean)dbo).setNew(false);
			}
			return dbo;
		}catch(Exception e){
			throw new SQLException(e.getMessage());
		}
	}
	

	public DBRecord getRecord(ResultSet rs) throws Exception {
		DBRecord rec = new DBRecord();
		ResultSetMetaData md = rs.getMetaData();
		//fill the record with current row assuming next() has already been called
		String columnName;
		Object fieldValue;
		for (int i = 0; i < md.getColumnCount(); i++) {
		    columnName = rs.getMetaData().getColumnLabel(i + 1).toUpperCase();
			
		    
			int type = md.getColumnType(i + 1);
			if ((type == Types.DATE)) {
				fieldValue = rs.getDate (i + 1);
				
			}else if ((type == Types.TIMESTAMP) || (type == Types.TIME)) {
			    fieldValue = rs.getTimestamp (i + 1);
            }
			else if ((type == Types.INTEGER) || (type == Types.SMALLINT) || (type == Types.TINYINT) ) {
                fieldValue = rs.getInt (i + 1);
            }
			else if ((type == Types.DOUBLE) || (type == Types.DECIMAL) || (type == Types.FLOAT)) {
                fieldValue = rs.getDouble (i + 1);
            }
			else if (type == Types.NUMERIC) {
				int scale = md.getScale(i + 1);
				if( scale != 0){
					fieldValue = rs.getDouble (i + 1);
				}else{
					fieldValue = rs.getInt (i + 1);
				}
                
            }
			else if (type == Types.BIGINT) {
                fieldValue = rs.getLong (i + 1);
            }
			else {
				fieldValue = rs.getString(i + 1);
			}
			if (fieldValue != null) {
			    rec.getLookUpTable().put(columnName ,fieldValue);
			}
			
		}

		return rec;
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

	public DBQuery getDBQuery() {
		return dBQuery;
	}

	public void setDBQuery(DBQuery query) {
		dBQuery = query;
	}





    public DBDirector getDirector ()
    {
        return m_director;
    }

    public void setDirector (DBDirector director)
    {
        m_director = director;
    }

    /* (non-Javadoc)
     * @see org.springframework.jdbc.core.RowCallbackHandler#processRow(java.sql.ResultSet)
     */
    public void processRow (ResultSet rs) throws SQLException
    {
        m_currentRecordNumber++;
        if((m_currentRecordNumber > (m_pageSize * (m_pageNumber -1))) && (m_currentRecordNumber < ((m_pageSize * m_pageNumber ) + 1))){
            getResultPage ().add (mapRow (rs, m_currentRecordNumber));
        }
        
    }

    public List<Object> getResultPage ()
    {
        if(m_resultPage == null){
            m_resultPage = new ArrayList <Object> ();
        }
        return m_resultPage;
    }



    public int getPageSize ()
    {
        return m_pageSize;
    }

    public void setPageSize (int pageSize)
    {
        m_pageSize = pageSize;
    }

    public int getPageNumber ()
    {
        return m_pageNumber;
    }

    public void setPageNumber (int pageNumber)
    {
        m_pageNumber = pageNumber;
    }

    public int getCurrentRecordNumber ()
    {
        return m_currentRecordNumber;
    }

    public void setCurrentRecordNumber (int currentRecordNumber)
    {
        m_currentRecordNumber = currentRecordNumber;
    }

    public DataSource getLocalDatasouceHandle ()
    {
        return m_localDatasouceHandle;
    }

    public void setLocalDatasouceHandle (DataSource localDatasouceHandle)
    {
        m_localDatasouceHandle = localDatasouceHandle;
    }

	
}
