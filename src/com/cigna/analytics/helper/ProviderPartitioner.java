package com.cigna.analytics.helper;


import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

public class ProviderPartitioner implements Partitioner {

	public ProviderPartitioner (VerifiableProperties props) {

	}

	public int partition(Object key, int a_numPartitions) {
		int partition = 0;
		String stringKey = (String) key;
		partition = Integer.parseInt( stringKey.substring(0,3)) % a_numPartitions;
		return partition;
	}

}
