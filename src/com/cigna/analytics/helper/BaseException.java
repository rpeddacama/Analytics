package com.cigna.analytics.helper;

import java.io.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *   Description: This is the base exception class
 *
 *   File: BaseException.java
 *
 *   File type: Class
 *
 *   @version: 1.0
 *
 *   @author: Rupendra Peddacama
 *
 *   Change History: 
 *
 *   @see java.lang.Exception
 *
 */

public class BaseException extends Exception {

    private static Log log =
        LogFactory.getLog(BaseException.class);

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
    
    /**
     * Constructs an <code>BaseException</code> with the specified
     * message and indicating if the exception should be logged
     *
     * @param   message The message
     */
    public BaseException(String message) {

		super(message);

		// log the exception
		logException();
    }

    /**
     * Constructs a BaseException with the specified message and Throwable
     * @param message
     * @param throwable
     */
    public BaseException(String message, Throwable throwable){
    	super(message,throwable);
    }
    
	/**
     * This over rides the printStackTrace method.  It sends the
     * stack trace info to the logger.
     *
     */
    public void printStackTrace() {
		logException();
    }


	/**
     * Logs the exception to the ESearch Logger.
     *
     */
    public void logException() {

		log.error(this.toString(), this);

    }

	/**
     * This gets a String representing a stack trace
     * 
     * @param e The exception to get the stack trace 
     * 
     * @return a string representing the stack trace
     */
    public static String getStackTrace(Exception e) {

        // Get the stack trace into a byte array
        ByteArrayOutputStream bao = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(bao);
       	e.printStackTrace(ps);

		return bao.toString();

    }

}
