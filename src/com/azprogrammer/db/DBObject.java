package com.azprogrammer.db;

import java.beans.*;

/**
 * A DBObject is a JavaBean that represents any logical entity that can be persisted as a record in a database.<BR>
 * Creation date: (11/15/2000 11:05:22 AM)
 * @author: Luis Montes
 */

public abstract class DBObject
    extends PropertyChanger
    implements PropertyChangeListener {

    boolean new_ = true;
    private boolean fieldBeanModified = false;

    /**
     * DBObject constructor.
     */
    public DBObject() {
        super();
        setNew(true);
        addPropertyChangeListener(this);
    }
    
    /**
     * Gets the beanModified property (boolean) value.
     * @return The beanModified property value.
     * @see #setBeanModified
     */
    protected boolean isBeanModified() {
        return fieldBeanModified;
    }
    public boolean isNew() {
        return new_;
    }
    public void propertyChange(PropertyChangeEvent e) {
        Object newValue = e.getNewValue();
        Object oldValue = e.getOldValue();

        if (((oldValue != null) && (!oldValue.equals(newValue)))
            || ((newValue != null) && (!newValue.equals(oldValue)))) {
            setBeanModified(true);
        }

    }

    /**
     * Sets the beanModified property (boolean) value.
     * @param beanModified The new value for the property.
     * @see #getBeanModified
     */
    public void setBeanModified(boolean beanModified) {
        fieldBeanModified = beanModified;
    }
    public void setNew(boolean b) {
        new_ = b;
    }
}
