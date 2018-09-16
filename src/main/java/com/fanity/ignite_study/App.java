package com.fanity.ignite_study;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteCallable;
import org.junit.Test;



/**
 * Hello world!
 *
 */
public class App {
	public static void main(String[] args) {
		testDB();
	}
	
	@Test
	public void testA(){
		try (Ignite ignite = Ignition.start("D:/soft/apache-ignite-fabric-2.6.0-bin/examples/config/example-ignite.xml")) {
			Collection<IgniteCallable<Integer>> calls = new ArrayList<>();

			// Iterate through all the words in the sentence and create Callable
			// jobs.
			for (final String word : "Count characters using callable".split(" ")) {
				calls.add(new IgniteCallable<Integer>() {
					@Override
					public Integer call() throws Exception {
						return word.length();
					}
				});
			}

			// Execute collection of Callables on the grid.
			Collection<Integer> res = ignite.compute().call(calls);

			int sum = 0;

			// Add up individual word lengths received from remote nodes.
			for (int len : res)
				sum += len;

			System.out.println(">>> Total number of characters in the phrase is '" + sum + "'.");
		}
	}

	@Test
	public void testB(){
		//Ignition.start("D:/soft/apache-ignite-fabric-2.6.0-bin/examples/config/example-ignite.xml")
	    try (Ignite ignite = Ignition.start("D:/soft/apache-ignite-fabric-2.6.0-bin/examples/config/example-ignite.xml")) {
	      // Put values in cache.
	      IgniteCache<Integer, String> cache = ignite.getOrCreateCache("myCache");
	      cache.put(1, "Hello");
	      cache.put(2, "World!");
	      // Get values from cache
	      // Broadcast 'Hello World' on all the nodes in the cluster.
	      ignite.compute().broadcast(()->System.out.println(cache.get(1) + " " + cache.get(2)));
	    }
		
    }
	
	public static void testDB(){
		try {
			// Register JDBC driver.
			Class.forName("org.apache.ignite.IgniteJdbcDriver");
			// Opening a connection in the streaming mode and time based flushing set.
			//Connection conn = DriverManager.getConnection("jdbc:ignite:cfg://streaming=true:streamingFlushFrequency=1000@file:///etc/config/ignite-jdbc.xml");
			Connection conn = DriverManager.getConnection("jdbc:ignite:thin://192.168.1.104");
			System.out.println(conn);
			PreparedStatement stmt = conn.prepareStatement(
			  "INSERT INTO Person(_key, name, age) VALUES(CAST(? as BIGINT), ?, ?)");
			// Adding the data.
			for (int i = 1; i < 100; i++) {
			      // Inserting a Person object with a Long key.
			      stmt.setInt(1, i);
			      stmt.setString(2, "John Smith");
			      stmt.setInt(3, 25);
			  
			      stmt.execute();
			}
			conn.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
//	public static void testC(){
//		Ignite ignite = Ignition.start();
//
//    	long cityId = 2; // Id for Denver
//
//    	// Sending the logic to a cluster node that stores Denver and its residents.
//    	ignite.compute().affinityRun("SQL_PUBLIC_CITY", cityId, new IgniteRunnable() {
//    	  
//    	  @IgniteInstanceResource
//    	  Ignite ignite;
//    	  
//    	  @Override
//    	  public void run() {
//    	    // Getting an access to Persons cache.
//    	    IgniteCache<BinaryObject, BinaryObject> people = ignite.cache(
//    	        "Person").withKeepBinary();
//    	 
//    	    ScanQuery<BinaryObject, BinaryObject> query = 
//    	        new ScanQuery <BinaryObject, BinaryObject>();
//    	 
//    	    try (QueryCursor<Cache.Entry<BinaryObject, BinaryObject>> cursor =
//    	           people.query(query)) {
//    	      
//    	      // Iteration over the local cluster node data using the scan query.
//    	      for (Cache.Entry<BinaryObject, BinaryObject> entry : cursor) {
//    	        BinaryObject personKey = entry.getKey();
//    	 
//    	        // Picking Denver residents only only.
//    	        if (personKey.<Long>field("CITY_ID") == cityId) {
//    	            person = entry.getValue();
//    	            // Sending the warning message to the person.
//    	        }
//    	      }
//    	    }
//    	  }
//    	}
//	}

}
