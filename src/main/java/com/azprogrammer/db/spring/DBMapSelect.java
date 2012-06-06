
package com.azprogrammer.db.spring;

import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.DataSource;

import com.azprogrammer.db.DBDirector;
import com.azprogrammer.db.DBRecord;

public class DBMapSelect extends DBSelect
{
    public DBMapSelect(DataSource dataSource, DBDirector director){
        super(dataSource, director);
    }
    
    public DBMapSelect(DataSource dataSource,String sql,  DBDirector director){
        super(dataSource,sql, director);
    }
    
    protected Object mapRow(ResultSet rs, int rowNum) throws SQLException {
        try{
            DBRecord rec = getRecord(rs);
            return getDirector ().getRecordMap (rec);
        }catch(Exception e){
            throw new SQLException(e.getMessage());
        }
    }
}
