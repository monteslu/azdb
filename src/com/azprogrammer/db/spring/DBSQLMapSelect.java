
package com.azprogrammer.db.spring;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.sql.DataSource;

import org.springframework.jdbc.object.MappingSqlQuery;

import com.azprogrammer.db.DBUtil;


public class DBSQLMapSelect extends MappingSqlQuery
{

    protected Object[] m_queryParameters;
    protected List<String> m_columnPropNames;

    public DBSQLMapSelect(DataSource datasource, String sql){
        super();
        setDataSource (datasource);
        setSql (sql);
    }
    
    public DBSQLMapSelect(DataSource datasource, String sql, Object[] parameters){
        super();
        setDataSource (datasource);
        setSql (sql);
        setQueryParameters (parameters);
    }
    
    public List getAll(){
        return execute(getQueryParameters ());        
    }
    
    /* (non-Javadoc)
     * @see org.springframework.jdbc.object.MappingSqlQuery#mapRow(java.sql.ResultSet, int)
     */
    @Override
    protected Object mapRow (ResultSet rs, int rowNum) throws SQLException
    {
        HashMap <String, Object> rec = new HashMap <String, Object> ();
        List <String> props = getColumnPropNames ();
        if(props.isEmpty ()){
            ResultSetMetaData md = rs.getMetaData();
            for (int i = 0; i < md.getColumnCount(); i++) {
                props.add (DBUtil.propNameFromCol (rs.getMetaData().getColumnLabel(i + 1).toUpperCase())) ;
            }
        }
        
        for (int i = 0; i < props.size (); i++) {
           rec.put (props.get (i),rs.getObject (i + 1));                      
        }

        return rec;
    }

    public Object[] getQueryParameters ()
    {
        return m_queryParameters;
    }

    public void setQueryParameters (Object[] queryParameters)
    {
        m_queryParameters = queryParameters;
    }

    public List <String> getColumnPropNames ()
    {
        if(m_columnPropNames == null){
            m_columnPropNames = new ArrayList <String> ();
        }
        return m_columnPropNames;
    }

    public void setColumnPropNames (List <String> columnPropNames)
    {
        m_columnPropNames = columnPropNames;
    }
}
