package com.cigna.analytics.spark;

import java.util.*;
import java.util.Map.Entry;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;


public class PublishToKafka {

	private Properties kafkaProps = new Properties();
	private KafkaProducer<String, String> producer;

	public static void main(String[] args){

		PublishToKafka ptk = new PublishToKafka();

		Map<String,Map<Integer,Integer>> stats = new HashMap<String, Map<Integer, Integer>>();
		Map<Integer,Integer> counts = new HashMap<Integer,Integer>();
		counts.put(1, 1);
		counts.put(30, 1);
		counts.put(4, 2);

		stats.put("PHY @ Robert Smith, MD", counts);
		stats.put("PHY @ Kyra Lynn, MD", counts);

		ptk.publish(stats);
	}

	public void publish(Map<String,Map<Integer,Integer>> stats){

		kafkaProps.put("bootstrap.servers", "cilidolp0007.sys.cigna.com:9092,cilidolp0007.sys.cigna.com:9093,cilidolp0007.sys.cigna.com:9094");

		// This is mandatory, even though we don't send keys
		kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		kafkaProps.put("acks", "1");
		kafkaProps.put("partitioner.class", "com.cigna.analytics.helper.ProviderPartitioner");

		// how many times to retry when produce request fails?
		kafkaProps.put("retries", "3");
		kafkaProps.put("linger.ms", 5);

		producer = new KafkaProducer<String,String>(kafkaProps);
		Map<String,String> phyNames = new HashMap<String,String>();
		Map<String,String> denNames = new HashMap<String,String>();
		Map<String,String> phaNames = new HashMap<String,String>();
		Map<String,String> hosNames = new HashMap<String,String>();
		Map<String,String> facNames = new HashMap<String,String>();
		

		for (Entry<String, Map<Integer, Integer>> entry : stats.entrySet()) {

			String key=entry.getKey();
			String value="";
			if(key.substring(0,3).equals("PHY")){
				for (Entry<Integer, Integer> entry1 : entry.getValue().entrySet()) {
					value+="\""+entry1.getKey()+"$"+entry1.getValue()+"\",";
				}
				phyNames.put(key.substring(6,key.length()), value.substring(0, value.length()));
			}
			if(key.substring(0,3).equals("DEN")){
				for (Entry<Integer, Integer> entry1 : entry.getValue().entrySet()) {
					value+="\""+entry1.getKey()+"$"+entry1.getValue()+"\",";
				}
				denNames.put(key.substring(6,key.length()), value.substring(0, value.length()));
			}
			if(key.substring(0,3).equals("PHA")){
				for (Entry<Integer, Integer> entry1 : entry.getValue().entrySet()) {
					value+="\""+entry1.getKey()+"$"+entry1.getValue()+"\",";
				}
				phaNames.put(key.substring(6,key.length()), value.substring(0, value.length()));
			}
			if(key.substring(0,3).equals("HOS")){
				for (Entry<Integer, Integer> entry1 : entry.getValue().entrySet()) {
					value+="\""+entry1.getKey()+"$"+entry1.getValue()+"\",";
				}
				hosNames.put(key.substring(6,key.length()), value.substring(0, value.length()));
			}
			if(key.substring(0,3).equals("FAC")){
				for (Entry<Integer, Integer> entry1 : entry.getValue().entrySet()) {
					value+="\""+entry1.getKey()+"$"+entry1.getValue()+"\",";
				}
				facNames.put(key.substring(6,key.length()), value.substring(0, value.length()));
			}
		}

		produce("PHY",phyNames);
		produce("DEN",denNames);
		produce("PHA",phaNames);
		produce("HOS",hosNames);
		produce("FAC",facNames);

		producer.close();


	}
	
	
	private void produce(String type, Map<String,String> names){
		
		if(names.size()>0){
			String payLoad="";
			String value = "{"+
					"\"_id\":\""+System.currentTimeMillis()+"\","+
					"\"type\":\""+type+"\","+
					"\"rankings\":[";
	
			for(Entry<String, String> load : names.entrySet()){
				payLoad+="{\"name\":\""+load.getKey()+"\",\"positionCounts\":["+load.getValue().substring(0, load.getValue().length()-1)+"]},";
			}
	
			value+=payLoad.substring(0, payLoad.length()-1)+"]}";
	//		System.out.println(value);
	
			ProducerRecord<String,String> record = new ProducerRecord<String,String>("replicated-idol-topic", type+System.currentTimeMillis(), value);
			producer.send(record,
					new Callback() {
				public void onCompletion(RecordMetadata metadata, Exception e) {
					if(e != null)
						e.printStackTrace();
					System.out.println("The offset of the record we just sent is: " + metadata.offset());
				}
			});
		}
	}


}
