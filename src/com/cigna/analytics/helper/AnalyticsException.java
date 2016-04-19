package com.cigna.analytics.helper;

/**
 * Description: Exception class for Analytics
 * 
 * File: AnalyticsException.java
 * 
 * File type: Class
 * 
 * @version: 1.0
 * 
 * @author: Rupendra Peddacama
 * 
 * Change History:
 * 
 */

public class AnalyticsException extends BaseException{

	/**
	* Determines if a de-serialized file is compatible with this class.
	*
	* Maintainers must change this value if and only if the new version
	* of this class is not compatible with old versions. See Sun docs
	* for <a href=http://java.sun.com/products/jdk/1.1/docs/guide
	* /serialization/spec/version.doc.html> details. </a>
	*
	* Not necessary to include in first version of the class, but
	* included here as a reminder of its importance.
	*/
	private static final long serialVersionUID = 7526471155622776147L;
	private String message=null;

    
    /**
     * Constructs an <code>ESearchException</code> with the specified
     * message
     *
     * @param   message The message
     */
    public AnalyticsException(String message) {
    	
		super(AnalyticsProperties.getMessage(message));
		this.message = message;
    }

    /**
     * Constructs an ESearchException with the message and throwable
     * @param message
     * @param throwable
     */
    public AnalyticsException(String message, Throwable throwable){
    	super(AnalyticsProperties.getMessage(message), throwable);
    	this.message=message;
    }

    /**
     * Returns error code for the exception
     * @return String errorcode
     */
    public String getErrorCode(){
    	return this.message;
    }
    
}
