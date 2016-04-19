package com.cigna.analytics.helper;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.io.*;
import javax.naming.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *   Description: This class loads the properties files for Analytics application
 *
 *   File: AnalyticsProperties.java
 *
 *   File type: Class
 *
 *   @version: 1.0
 *
 *   @author: Rupendra Peddacama
 *
 *   Change History: 
 *
 */

public class AnalyticsProperties {
	
	private static Properties errorProp = new Properties(); 
	private static Properties searchProp = new Properties();
	private static java.util.Map<String,Properties> propMap = new java.util.HashMap<String,Properties>();
    private static Log log = LogFactory.getLog(AnalyticsProperties.class);
    private static Context ctx;
    static {
		
		try{
			
			ctx = new InitialContext();
			
			ctx =(Context) new InitialContext().lookup("java:comp/env");

			searchProp.setProperty(SearchConstants.DEFAULT_SERVERHOST, (String) ctx.lookup(SearchConstants.DEFAULT_SERVERHOST));
			searchProp.setProperty(SearchConstants.DEFAULT_SERVERPORT, (String) ctx.lookup(SearchConstants.DEFAULT_SERVERPORT));

		}catch(NamingException ne){
			log.error("Error in loading search properties from context : "+ne.getMessage());
			InputStream is1 = AnalyticsProperties.class.getClassLoader().getResourceAsStream("search.properties");
			try{
				searchProp.load(is1);
			}catch(IOException ioe){
				log.error("Error in loading search properties file : "+ioe.getMessage());
				throw new RuntimeException("Error in loading search properties file : "+ioe.getMessage());
			}
			//throw new RuntimeException("Error in loading search properties : "+ne.getMessage());
		}
		InputStream is2=AnalyticsProperties.class.getClassLoader().getResourceAsStream("error.properties");
		try{
			errorProp.load(is2);
		}catch(IOException ioe){
			log.error("Error in loading error properties file : "+ioe.getMessage());
			throw new RuntimeException("Error in loading error properties file : "+ioe.getMessage());
		}		
		propMap.put("ERROR",errorProp);
		propMap.put("SEARCH", searchProp);
    }
	
    /**
     * Returns error message using the passed error code
     * @param code
     * @return error message for the input code
     */
	public static String getMessage(String code){
		
		if(code==null || code.equals("")){
			log.error("Error code specified is null or empty");
			throw new RuntimeException("Error code specified is null or empty");
		}

		String errorMessage="";
		errorMessage= errorProp.getProperty(code);
		if(!errorMessage.equals(""))
			return errorMessage;
		else{
			log.error("Error code specified is not found in error.properties file : "+code);
			throw new RuntimeException("Error code specified is not found in error.properties file : "+code);
		}
			
	}

	/**
	 * Returns a map of error messages given the error codes
	 * @param errorCodes
	 */
	public static Map<String, String> getErrorMessages(List<String> errorCodes){
		if(errorCodes == null || errorCodes.isEmpty())
			return (new HashMap<String, String>());
		Map<String, String> msgs = new HashMap<String, String>();
		for(int i=0;i<errorCodes.size();i++){
			if(errorCodes.get(i)!=null && !errorCodes.get(i).equals(""))
				msgs.put(errorCodes.get(i), getMessage((String)errorCodes.get(i)));
		}
		return msgs;
	}
	
	/**
	 * Returns the property value for the input name
	 * @param name
	 * @param context
	 * @return property value
	 */
	public static String getProperty(String context, String name){
		
		String propertyValue="";
		propertyValue= ((Properties)propMap.get(context)).getProperty(name);
		if(!propertyValue.equals(""))
			return propertyValue;
		else{
			log.error("Property specified is not found in search.properties file : "+name);
			throw new RuntimeException("Property specified is not found in search.properties file : "+name);
		}
	}
	
}
