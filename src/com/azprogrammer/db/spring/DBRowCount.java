package com.azprogrammer.db.spring;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import javax.sql.DataSource;

import com.azprogrammer.db.DBDirector;
import com.azprogrammer.db.DBUtil;


public class DBRowCount extends DBSelect
{

    /**
     * @param dataSource
     * @param director
     */
    public DBRowCount (DataSource dataSource, DBDirector director)
    {
        super (dataSource, director);
    }
    
    public DBRowCount(DataSource dataSource, String sql,DBDirector director) {
        super(dataSource, sql,director);
    }
    
    public int getRecordCount(){
        String sql =
            "SELECT count(*) FROM "
            + getDirector().getFQTableName(getDBQuery())
            + buildAllQueryParams(false);
        setSql(sql);
        DBUtil.trace (sql);
        List list = execute(getParamValues().toArray());
        return ((Integer)list.get (0)).intValue ();
        
    }
    
    protected Object mapRow(ResultSet rs, int rowNum) throws SQLException {
        try{
            return new Integer(rs.getInt (1));
            
        }catch(Exception e){
            throw new SQLException(e.getMessage());
        }
    }

}
