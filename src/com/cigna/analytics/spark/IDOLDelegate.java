package com.cigna.analytics.spark;


import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;

import com.autonomy.aci.AciAction;
import com.autonomy.aci.AciConnection;
import com.autonomy.aci.AciConnectionDetails;
import com.autonomy.aci.AciResponse;
import com.autonomy.aci.exceptions.*;
import com.cigna.analytics.helper.AnalyticsException;


public class IDOLDelegate implements VoidFunction<JavaRDD<String>>
{

	private static final long serialVersionUID = 1L;
	private AciConnectionDetails aciConnDetails = null;
	
	private static Log log = LogFactory.getLog(IDOLDelegate.class);

	@Override
	public void call(JavaRDD<String> rdd)
			throws Exception {
		aciConnDetails = getConnDetails("cilidolv0002.sys.cigna.com",18000);
		String query="";
		int count=0;
		
		Map<String,Map<Integer,Integer>> stats = new HashMap<String, Map<Integer, Integer>>();

		if(!rdd.isEmpty()){
			count = rdd.collect().size();
			if(count>10)
				count=10;
			
			for(int i=0;i<count;i++){
				query=rdd.collect().get(i);
				query=query.replaceAll("/a=query", "query");
				query=query.replaceAll("&maxresults=", "&maxresults=5&");
				query=query.replaceAll("&printfields=LOCATIONID,MONGODOCID", "&printfields=NAME");
				query=query.replaceAll("&responseformat=json", "&responseformat=xml");
				
//				System.out.println(query);
				AciResponse acir = new AciResponse();
				int rank=0;
				String collName="", provName="";
				
				AciAction acia = new AciAction(query);
				acir = submitAciAction(acia,aciConnDetails);
				
				if(acir.checkForSuccess()){

					AciResponse results = acir.findFirstOccurrence("autn:hit");
					while(results!=null){

						rank++;
						if (results.getName().equals("autn:hit")) {
							
							AciResponse aProvName = results.findFirstEnclosedOccurrence("NAME");
							if(aProvName != null)
								provName = aProvName.getValue();

							AciResponse aCollName = results.findFirstEnclosedOccurrence("autn:database");
							if(aCollName != null)
								collName = aCollName.getValue();
							
							if(collName.toUpperCase().indexOf("HCP_PRVDR_P")!= -1)
								provName = "PHY @ "+provName;
							else if(collName.toUpperCase().indexOf("HCP_PRVDR_D")!= -1)
								provName = "DEN @ "+provName;
							else if(collName.toUpperCase().indexOf("HCP_PRVDR_R")!= -1)
								provName = "PHA @ "+provName;					
							else if(collName.toUpperCase().indexOf("HCP_PRVDR_F")!= -1)
								provName = "HOS @ "+provName;
							else if(collName.toUpperCase().indexOf("HCP_PRVDR_O")!= -1)
								provName = "FAC @ "+provName;
							
							if(stats.containsKey(provName)){
								Map<Integer, Integer> value = (Map<Integer, Integer>) stats.get(provName);
								if(value.containsKey(rank))
								{
									int rankCount = (int) value.get(rank);
									value.put(rank, rankCount++);
								}
								else
									value.put(rank, 1);
							}
							else{
								
								Map<Integer, Integer> value = new HashMap<Integer, Integer>();
								value.put(rank, 1);
								stats.put(provName, value);
							}

						}
						results = results.next();
					}//while looping the results
					
				} //if for checking search results success

			}
			
			
			PublishToKafka pub = new PublishToKafka();
			pub.publish(stats);
			
			/*
			for (Entry<String, Map<Integer, Integer>> entry : stats.entrySet()) {
			    System.out.println(entry.getKey());
			    for (Entry<Integer, Integer> entry1 : entry.getValue().entrySet()) {
			    	System.out.println("$("+entry1.getKey()+ "," + entry1.getValue()+")");
			    }
			}
			*/

		}
	}

	
	/**
	 * This method creates the connection object using hostname and port for IDOL
	 * @param sHost
	 * @param iPort
	 * @return
	 */
	public AciConnectionDetails getConnDetails(String sHost, int iPort)	{
		
		AciConnectionDetails connDetails = new AciConnectionDetails();
		connDetails.setHost(sHost);
		connDetails.setPort(iPort);
		connDetails.setTimeout(150000);
		connDetails.setRetries(0);
		return connDetails;
	}
	
	/**
	 * This method submits the action to IDOL server and gets the response object
	 * @param aciAction
	 * @param connDetails
	 * @return
	 */
	public AciResponse submitAciAction(AciAction aciAction, 
			AciConnectionDetails connDetails) throws AnalyticsException {
		
		AciResponse acir = new AciResponse();
		try	{
			AciConnection aciConn = new AciConnection();
			aciConn = new AciConnection(connDetails);
			acir = aciConn.aciActionExecute(aciAction);
		}catch(BadParameterException bpe) {
			throw new AnalyticsException("ESL0021",bpe);
		}catch(ForbiddenActionException fae) {
			throw new AnalyticsException("ESL0022",fae);
		}catch(MissingParameterException mpe) {
			throw new AnalyticsException("ESL0023",mpe);
		}catch(ServerNotAvailableException snae) {
			throw new AnalyticsException("ESL0024",snae);
		}catch(AciException exp) {
			throw new AnalyticsException("ESL0025",exp);
		}
		//catch(UnsupportedEncodingException uee){
		catch(Exception uee){
			throw new AnalyticsException("ESL0007",uee);
		}
		return acir;
	}

}
