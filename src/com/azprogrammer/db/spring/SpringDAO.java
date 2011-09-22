package com.azprogrammer.db.spring;


import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.springframework.jdbc.core.support.JdbcDaoSupport;

import com.azprogrammer.db.DBDirector;
import com.azprogrammer.db.DBKey;
import com.azprogrammer.db.DBQuery;
import com.azprogrammer.db.DBSpecificLayer;
import com.azprogrammer.db.DBStateAwareBean;
import com.azprogrammer.db.DBUtil;
import com.azprogrammer.db.databases.MySQLLayer;


public class SpringDAO extends JdbcDaoSupport {
	
	private DBSpecificLayer dBLayer = MySQLLayer.getInstance();
	
	public int getRowCount(DBDirector director){
	    return getRowCount (director, null);
	}
	
	public int getRowCount(DBDirector director,DBQuery query){
        DBRowCount dbr = new DBRowCount (getDataSource (), director);
        dbr.setDBQuery (query);
        return dbr.getRecordCount ();
    }
	
	public List getAll(DBDirector director){
		return getAll(director,null);		
	}
	
	public List getAll(DBDirector director, int maxRows){
		return getAll(director,null,maxRows);		
	}
	
	public List getAll(DBDirector director, DBQuery query){
		DBSelect dbs = new DBSelect(getDataSource(),director);
		dbs.setDBQuery(query);
		List retVal = dbs.getAll ();
		return retVal;
	}
	
	public List getPage(DBDirector director, int pageSize, int  pageNumber){
        return getPage (director, null, pageSize, pageNumber);
    }
	
	public List getPage(DBDirector director, DBQuery query, int pageSize, int  pageNumber){
        DBSelect dbs = new DBSelect(getDataSource(),director);
        dbs.setDBQuery(query);
        List retVal = dbs.getPage (pageSize, pageNumber);
        return retVal;
    }
	
	public List getAll(DBDirector director, DBQuery query, int maxRows){
		DBSelect dbs = new DBSelect(getDataSource(),director);
		dbs.setDBQuery(query);
		dbs.setMaxRows(maxRows);
		List retVal = dbs.getAll ();
		return retVal;
	}
	
	public List<Map <String, Object>> getAllMaps(DBDirector director){
        return getAllMaps(director,null);       
    }
    
    public List<Map <String, Object>> getAllMaps(DBDirector director, int maxRows){
        return getAllMaps(director,null,maxRows);       
    }
    
    public List<Map <String, Object>> getAllMaps(DBDirector director, DBQuery query){
        DBMapSelect dbs = new DBMapSelect(getDataSource(),director);
        dbs.setDBQuery(query);
        List retVal = dbs.getAll ();
        return retVal;
    }
    
    public List<Map <String, Object>> getAllMaps(DBDirector director, DBQuery query, int maxRows){
        DBMapSelect dbs = new DBMapSelect(getDataSource(),director);
        dbs.setDBQuery(query);
        dbs.setMaxRows(maxRows);
        List retVal = dbs.getAll ();
        return retVal;
    }
    
    
    
    public List<Map <String, Object>> getMapsWithSQL(String sql){
        return getMapsWithSQL(sql, null, -1);       
    }
    
    public List<Map <String, Object>> getMapsWithSQL(String sql, int maxRows){
        return getMapsWithSQL(sql,null,maxRows);       
    }
    
    public List<Map <String, Object>> getMapsWithSQL(String sql, Object[] parameters){
        return getMapsWithSQL(sql,parameters,-1);        
    }
    
    public List<Map <String, Object>> getMapsWithSQL(String sql, Object[] parameters, int maxRows){
        DBSQLMapSelect dbs = new DBSQLMapSelect(getDataSource(),sql);
        if(parameters != null){
            dbs.setQueryParameters (parameters);
        }
        if(maxRows > -1){
            dbs.setMaxRows(maxRows);
        }
        return dbs.getAll ();       
    }
    
    
    public Map <String, Object> getMapsAndColsWithSQL(String sql){
        return getMapsAndColsWithSQL(sql, null, -1);       
    }
    
    public Map <String, Object> getMapsAndColsWithSQL(String sql, int maxRows){
        return getMapsAndColsWithSQL(sql,null,maxRows);       
    }
    
    public Map <String, Object> getMapsAndColsWithSQL(String sql, Object[] parameters){
        return getMapsAndColsWithSQL(sql,parameters,-1);        
    }
    
    public Map <String, Object> getMapsAndColsWithSQL(String sql, Object[] parameters, int maxRows){
        DBSQLMapSelect dbs = new DBSQLMapSelect(getDataSource(),sql);
        if(parameters != null){
            dbs.setQueryParameters (parameters);
        }
        if(maxRows > -1){
            dbs.setMaxRows(maxRows);
        }
        List<Map <String, Object>> rows = dbs.getAll ();
        Map<String, Object> retVal = new HashMap <String, Object>();
        retVal.put ("columns", dbs.getColumnPropNames ());
        retVal.put ("rows", rows);
        return retVal;
    }
    
    
    public List<Object> getObjectsWithSQL(Class <Object> beanClass, String sql){
        return getObjectsWithSQL(beanClass,sql, null,-1);       
    }
    
    public List<Object> getObjectsWithSQL(Class <Object> beanClass,String sql, int maxRows){
        return getObjectsWithSQL(beanClass,sql,null,maxRows);       
    }
    
    public List<Object> getObjectsWithSQL(Class <Object> beanClass,String sql, Object[] parameters){
        return getObjectsWithSQL(beanClass,sql,parameters,-1);        
    }
    
    public List<Object> getObjectsWithSQL(Class <Object> beanClass,String sql, Object[] parameters, int maxRows){
        DBSQLMapSelect dbs = new DBSQLMapSelect(getDataSource(),sql);
        Map <String, Method> propSetters = DBUtil.getPropertySetters (beanClass);
        List <Object> beans = new ArrayList <Object> ();
        
        if(parameters != null){
            dbs.setQueryParameters (parameters);
        }
        if(maxRows > -1){
            dbs.setMaxRows(maxRows);
        }
        List <Map<String, Object>> maps = dbs.getAll ();
                
        for (Iterator <Map <String, Object>> iterator = maps.iterator (); iterator.hasNext ();)
        {
            Map <String, Object> valuesMap = iterator.next ();
            try{
                Object bean = beanClass.newInstance ();
                DBUtil.setProperties (bean, valuesMap, propSetters);
                beans.add (bean);
            }catch(Exception beanE){
                throw new RuntimeException ("error setting bean properties " + beanE.getMessage ());
            }
        }        
        
        return beans; 
    }
    
    
	
	public int delete(Object dbo,DBDirector director) throws SQLException{
		try{
			DBUpdate dbu = new DBUpdate(getDataSource(),dbo,director);
			return dbu.delete();
		}catch(Exception e){
			throw new SQLException(e.getMessage());
		}
	}
	
	public void save(DBStateAwareBean dbo,DBDirector director) throws SQLException{
		try{
			
			if(dbo.isNew ()){
			    insert(dbo,director);
			}else{
			    update(dbo,director);
			}
			
		}catch(Exception e){
			throw new SQLException(e.getMessage());
		}
		
		
	}
	
	public void insert(Object dbo,DBDirector director) throws SQLException{
        try{
            
            DBUpdate dbu = new DBUpdate(getDataSource(),dbo,director);
            dbu.setDBLayer(getDBLayer());
            dbu.buildInsert ();
            dbu.update(dbu.getParamValues ().toArray ());           
            
        }catch(Exception e){
            throw new SQLException(e.getMessage());
        }
        
        
    }
	
	public void update(Object dbo,DBDirector director) throws SQLException{
        try{
            
            DBUpdate dbu = new DBUpdate(getDataSource(),dbo, director);
            dbu.setDBLayer(getDBLayer());
            dbu.buildUpdate ();
            DBUtil.trace(dbu.getSql());
            dbu.update(dbu.getParamValues ().toArray ());           
            
        }catch(Exception e){
        	e.printStackTrace(System.err);
        	throw new SQLException(e.getMessage());
            
        }
        
        
    }
	
	public Object findObject(DBKey key,DBDirector director){
		
		DBSelect dbs = new DBSelect(getDataSource(),director);
		return dbs.findObject(key);
	}
	
	public Object findMap(DBKey key,DBDirector director){
        
        DBMapSelect dbs = new DBMapSelect(getDataSource(),director);
        return dbs.findObject(key);
    }

	public DBSpecificLayer getDBLayer() {
		return dBLayer;
	}

	public void setDBLayer(DBSpecificLayer layer) {
		dBLayer = layer;
	}
		
	public int findSequenceNumber(DBDirector director) throws SQLException{
		return findSequenceNumber(director.getFQSequenceName());
	}
	
	public int findSequenceNumber(String sequenceName) throws SQLException{
		Connection con = null;
		try{
			con = getDataSource().getConnection();
			int retVal = getDBLayer().getSequenceNextVal(sequenceName, con);
			
			con.close();
			return retVal;
		}catch(Exception e){
			throw new SQLException(e.getMessage());
		}finally{
			if(con != null){
				try{
					con.close();
					}catch(Exception closeE){
						//NOP
					}
			}
		}
		
	}
	
}
