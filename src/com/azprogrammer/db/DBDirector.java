package com.azprogrammer.db;

import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;



/**
 * A DBDirector is used in conjunction with a DBFinder to create DBObjects from a database.<BR>
 * A DBDirector should be used and instatiated only as a singleton instance.<BR>
 * Creation date: (11/15/2000 11:05:22 AM)
 * @author: Luis Montes
 */

public abstract class DBDirector {
    
    protected String m_schema = null;
    protected String m_tableName = null;
    protected String m_viewName = null;
    protected String m_tablePrefix = null;
    protected String m_sequenceName = null;
    protected boolean m_dbAwareBeans = false;
    protected Map<String,Method> m_propertySetters;
    protected Map<String,Method> m_propertyGetters;
    protected Map<String,String> m_propertiesToColumns;
    protected List<String> m_ingorablePropsForInsert;
	
	/**
	 * DBDirector constructor.
	 */
	protected DBDirector() {
		super();
	}
	
	
	public abstract DBKey createKey(Object bean);
	
	
	/**
	 * 
	 *By default a query returns all columns in the table.
	 *  Override this if you want to specify columns. 
	 */
	public String getColumnSelectionSQL() {
		return "*";
	}

	/**
	*	Override this if you want to use a sequence
	*/
	public String getSequenceName() {
		return m_sequenceName;
	}

	public String getTableName(){
	    return m_tableName;
	}
	
	/**
	 * This may be overriden if you are querying from a view but writing to a table.
	 */
	public String getTableName(DBQuery query) {
		return getViewName();
	}
	
	public String getFQName(String objectName){
	    String prefix = DBUtil.clearNull (getTablePrefix ()).trim ();
	    
	    
	    if ( (!"".equals (DBUtil.clearNull (getSchema()).trim () )) && (!"".equals (DBUtil.clearNull (objectName).trim () )) ){
	        return getSchema () + "." + prefix + objectName;
	    }else{
	        return prefix + objectName;
	    }
	}
	
	/**
     * Maps the properties of this object to the columns that need to be persisted.
     *
     */
    public DBRecord fillDatastore(Object bean) throws DBException{
        Map propsToCols = getPropertiesToColumns ();        
        if(propsToCols == null){
            throw new DBException("Missing property to column map");
        }
        List<String> ignorables = getIngorablePropsForInsert ();
        Map getters = getPropertyGetters ();
        DBRecord rec = new DBRecord();
        
        try{       
            for (Iterator i = getters.keySet().iterator(); i.hasNext();) {
                String prop = (String) i.next();                
                Method method = (Method) getters.get(prop);
                if(!isIgnorableInsertProp (prop)){
                    //System.out.println("prop:" + prop);
                    String colName = (String)propsToCols.get (prop);
                    if(colName != null){
                        rec.putField(colName,method.invoke (bean));
                    }
                }
            } 
        }catch(Exception e){
            throw new DBException("Unable to retrieve property values from bean. " + e.getMessage ());
        }
        return rec;
    }
    
    
    /**
     * Convenience method to effeciently convert a bean into a map using cached property getter methods.
     * The bean should be only of the type handled by this director.
     *
     */
    public Map<String, Object> objectToMap(Object bean) throws Exception{
        Map<String, Object> map = new HashMap <String, Object> ();
        Map getters = getPropertyGetters ();
        try{       
            for (Iterator i = getters.keySet().iterator(); i.hasNext();) {
                String prop = (String) i.next();                
                Method method = (Method) getters.get(prop);
                map.put(prop,method.invoke (bean));
            } 
        }catch(Exception e){
            throw new Exception("Unable to create map from bean: " + e.getMessage ());
        }
        return map;
    }
    
    protected boolean isIgnorableInsertProp(String propName){
        List<String> ignorables = getIngorablePropsForInsert (); 
        if(ignorables == null){
            return false;
        }
        for (Iterator iterator = ignorables.iterator (); iterator.hasNext ();)
        {
            String igPropName = (String) iterator.next ();
            if(propName.equals (igPropName)){
                return true;
            }            
        }
        return false;
    }


    /**
     * Fill the properties of this object with the columns from the database record.
     *
     */
    public void fillObject(DBRecord record, Object bean) throws DBException{
        
        Map<String,String> propsToCols = getPropertiesToColumns ();
        Map<String, Method> setters = getPropertySetters ();
        if(propsToCols != null){
            try{               
                for (Iterator i = propsToCols.keySet().iterator(); i.hasNext();) {
                    String prop = (String) i.next();
                    String colName = (String) propsToCols.get (prop);
                    Method method = (Method) setters.get(prop);
                    Object value = record.getObjectField (colName);
                    if(method != null){
                        method.invoke (bean, value);
                    }
                } 
            }catch(Exception e){
                throw new DBException("Unable to set property values from bean. " + e.getMessage ());
            } 
        }else{
            try{       
                for (Iterator i = record.getLookUpTable ().keySet().iterator(); i.hasNext();) {
                    String colName = (String) i.next();
                    String prop = DBUtil.propNameFromCol (colName);                    
                    Method method = (Method) setters.get(prop);
                    Object value = record.getObjectField (colName);
                    if(method != null){
                        method.invoke (bean, value );
                    }
                } 
            }catch(Exception e){
                throw new DBException("Unable to set property values from bean with no mapping. " + e.getMessage ());
            }
        }

    }

    
    /**
     * Fill the properties of this object with the columns from the database record.
     *
     */
    public Map<String, Object> getRecordMap(DBRecord record) throws DBException{
        Map<String, Object> map = new HashMap <String, Object> ();
        Map<String,String> propsToCols = getPropertiesToColumns ();
        if(propsToCols != null){
            try{               
                for (Iterator i = propsToCols.keySet().iterator(); i.hasNext();) {
                    String prop = (String) i.next();
                    String colName = (String) propsToCols.get (prop);
                    map.put (prop, record.getObjectField (colName));
                } 
            }catch(Exception e){
                throw new DBException("Unable to set property values from bean. " + e.getMessage ());
            } 
        }else{
            try{       
                for (Iterator i = record.getLookUpTable ().keySet().iterator(); i.hasNext();) {
                    String colName = (String) i.next();
                    String prop = DBUtil.propNameFromCol (colName);                    
                    map.put (prop, record.getObjectField (colName));                    
                } 
            }catch(Exception e){
                throw new DBException("Unable to set property values from bean with no mapping. " + e.getMessage ());
            }
        }
        
        return map;

    }
    
	
	public String getFQTableName(){
	    return getFQName (getTableName());
	}
	
	public String getFQTableName(DBQuery query){
        return getFQName (getTableName(query));
    }
	
	public String getFQSequenceName(){
	    return getFQName (getSequenceName());
	}
	
	
	public abstract Object newObject();

    public String getSchema ()
    {
        return m_schema;
    }

    public void setSchema (String schemaName)
    {
        m_schema = schemaName;
    }

    public String getTablePrefix ()
    {
        return m_tablePrefix;
    }

    public void setTablePrefix (String aTablePrefix)
    {
        m_tablePrefix = aTablePrefix;
    }
    
    public boolean getReturnAutoIncrementId(){
        return false;
    }


    public Map <String, Method> getPropertySetters ()
    {
        if(m_propertySetters == null){
            try{
                HashMap <String, Method> temp = new HashMap <String, Method>();
                PropertyDescriptor[] props = Introspector.getBeanInfo(newObject ().getClass()).getPropertyDescriptors();
                for (int i = 0; i < props.length; i++)
                {
                    temp.put (props[i].getName (), props[i].getWriteMethod ());
                }
                m_propertySetters = temp;
            }catch(Exception e){
                System.out.println("Error with introspection on bean class for prop setters: " + e.getMessage ());
            }
            
        }
        return m_propertySetters;
    }


    public void setPropertySetters (HashMap <String, Method> propertySetters)
    {
        m_propertySetters = propertySetters;
    }


    public Map <String, Method> getPropertyGetters ()
    {
        if(m_propertyGetters == null){
            try{
                HashMap <String, Method> temp = new HashMap <String, Method>();
                PropertyDescriptor[] props = Introspector.getBeanInfo(newObject ().getClass()).getPropertyDescriptors();
                for (int i = 0; i < props.length; i++)
                {
                    temp.put (props[i].getName (), props[i].getReadMethod ());                    
                }
                temp.remove ("class");
                m_propertyGetters = temp;
            }catch(Exception e){
                System.out.println("Error with introspection on bean class for prop getters: " + e.getMessage ());
            }
        }
        return m_propertyGetters;
    }


    public void setPropertyGetters (HashMap <String, Method> propertyGetters)
    {
        m_propertyGetters = propertyGetters;
    }


    public boolean isDbAwareBeans ()
    {
        return m_dbAwareBeans;
    }


    public void setDbAwareBeans (boolean dbAwareBeans)
    {
        m_dbAwareBeans = dbAwareBeans;
    }


    public Map <String, String> getPropertiesToColumns ()
    {
        return m_propertiesToColumns;
    }


    public void setPropertiesToColumns (Map <String, String> propertiesToColumns)
    {
        m_propertiesToColumns = propertiesToColumns;
    }


    public List <String> getIngorablePropsForInsert ()
    {
        return m_ingorablePropsForInsert;
    }


    public void setIngorablePropsForInsert (List <String> ingorablePropsForInsert)
    {
        m_ingorablePropsForInsert = ingorablePropsForInsert;
    }


    public void setTableName (String tableName)
    {
        m_tableName = tableName;
    }


    public String getViewName ()
    {
        if(m_viewName == null){
            return getTableName ();
        }
        return m_viewName;
    }


    public void setViewName (String viewName)
    {
        m_viewName = viewName;
    }

     public String getColumnForProperty(String propName){
         if(getPropertiesToColumns () == null){
             return null;
         }else{
             return getPropertiesToColumns ().get (propName);
         }
     }


    public void setSequenceName (String sequenceName)
    {
        m_sequenceName = sequenceName;
    }


}
