package com.azprogrammer.db;

/**
 * Property changer provides support for PropertyChange Events.<BR>
 * Creation date: (11/17/2000 9:54:40 AM)
 * @author: Luis Montes
 */

public class PropertyChanger {
	protected transient java.beans.PropertyChangeSupport propertyChange;
	/**
	 * PropertyChanger constructor comment.
	 */
	public PropertyChanger() {
		super();
	}
	/**
	 * The addPropertyChangeListener method was generated to support the propertyChange field.
	 */
	public synchronized void addPropertyChangeListener(
		java.beans.PropertyChangeListener listener) {
		getPropertyChange().addPropertyChangeListener(listener);
	}
	/**
	 * The addPropertyChangeListener method was generated to support the propertyChange field.
	 */
	public synchronized void addPropertyChangeListener(
		java.lang.String propertyName,
		java.beans.PropertyChangeListener listener) {
		getPropertyChange().addPropertyChangeListener(propertyName, listener);
	}
	/**
	 * The firePropertyChange method was generated to support the propertyChange field.
	 */
	public void firePropertyChange(java.beans.PropertyChangeEvent evt) {
		getPropertyChange().firePropertyChange(evt);
	}
	/**
	 * The firePropertyChange method was generated to support the propertyChange field.
	 */
	public void firePropertyChange(
		java.lang.String propertyName,
		int oldValue,
		int newValue) {
		getPropertyChange().firePropertyChange(
			propertyName,
			oldValue,
			newValue);
	}
	/**
	 * The firePropertyChange method was generated to support the propertyChange field.
	 */
	public void firePropertyChange(
		java.lang.String propertyName,
		java.lang.Object oldValue,
		java.lang.Object newValue) {
		getPropertyChange().firePropertyChange(
			propertyName,
			oldValue,
			newValue);
	}
	/**
	 * The firePropertyChange method was generated to support the propertyChange field.
	 */
	public void firePropertyChange(
		java.lang.String propertyName,
		boolean oldValue,
		boolean newValue) {
		getPropertyChange().firePropertyChange(
			propertyName,
			oldValue,
			newValue);
	}
	/**
	 * Accessor for the propertyChange field.
	 */
	protected java.beans.PropertyChangeSupport getPropertyChange() {
		if (propertyChange == null) {
			propertyChange = new java.beans.PropertyChangeSupport(this);
		};
		return propertyChange;
	}
	/**
	 * The hasListeners method was generated to support the propertyChange field.
	 */
	public synchronized boolean hasListeners(java.lang.String propertyName) {
		return getPropertyChange().hasListeners(propertyName);
	}
	/**
	 * The removePropertyChangeListener method was generated to support the propertyChange field.
	 */
	public synchronized void removePropertyChangeListener(
		java.beans.PropertyChangeListener listener) {
		getPropertyChange().removePropertyChangeListener(listener);
	}
	/**
	 * The removePropertyChangeListener method was generated to support the propertyChange field.
	 */
	public synchronized void removePropertyChangeListener(
		java.lang.String propertyName,
		java.beans.PropertyChangeListener listener) {
		getPropertyChange().removePropertyChangeListener(
			propertyName,
			listener);
	}
}