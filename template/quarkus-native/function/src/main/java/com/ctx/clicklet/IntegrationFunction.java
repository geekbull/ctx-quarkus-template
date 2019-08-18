package com.ctx.clicklet;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.Consumes;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;



import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import redis.clients.jedis.Jedis;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.Instant;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

@Path("/")
public class IntegrationFunction {

	@GET
	@Path("/health")
	@Produces(MediaType.APPLICATION_JSON)
	public String health() {
		System.out.println("[ctx.log.info] Health Check called");
		String ctxId = UUID.randomUUID().toString();
		ObjectNode node = JsonNodeFactory.instance.objectNode(); // initializing
		node.put("ctx_id", ctxId);
		node.put("status", "clicklet_healthy");

		// Return JSON response
		return node.toString();

	}

	// Every end point should have a starting time and ending time to calculate
	// execution time
	// Long the execution time.
	// Should always carry a ctx-id in header. Generate one if you don't find one
	@POST
	@Path("/function")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public String functionpost(InputStream incomingData) {
		// Function Name - Either Read from environment variable or hardcoded

		String functionName = "";
		if (System.getenv("function_name") == null) {
			functionName = "restAPIServer";
		} else {
			functionName = System.getenv("function_name");
		}
		// Start Time
		Instant sinstant = Instant.now();
		long startTime = sinstant.toEpochMilli();

		String nextFunction = "";
		// Check if a ctx-id is passed in the header, otherwise generate one
		// Generate ctxID as UUID
		String ctxId = UUID.randomUUID().toString();

		// Read the incoming data and Build a String
		StringBuilder jsonStringBuilder = new StringBuilder();
		try {
			BufferedReader in = new BufferedReader(new InputStreamReader(incomingData));
			String line = null;
			while ((line = in.readLine()) != null) {
				jsonStringBuilder.append(line);
			}
		} catch (Exception e) {
			System.out.println("[ctx.log.error] Error Parsing: - ");
		}

		System.out.println("[ctx.log.info] Post Data Received: " + jsonStringBuilder.toString());

		// Check for next function. If next function is not available, send it to
		// deadend
		if (System.getenv("next_function") == null) {
			nextFunction = "deadend";
		}else {
			nextFunction = System.getenv("next_function");
		}

		//
		//
		//

		// Body of your function goes here

		//
		//
		//

		// Parse the string and create object, add ctx and store
		ObjectNode node = JsonNodeFactory.instance.objectNode(); // initializing
		node.put("ctx_id", ctxId);
		node.put("next_function", nextFunction);
		double estimatedTime = estimatedTime(startTime);
		node.put("exe_time_ms", estimatedTime); // In Milli Seconds

		// Get EndTime instant
		Instant einstant = Instant.now();
		long endTime = einstant.toEpochMilli();

		// Build a JSON to store in Redis

		ObjectMapper mapper = new ObjectMapper();
		JsonNode rootNode = mapper.createObjectNode();

		JsonNode childNode1 = mapper.createObjectNode();
		((ObjectNode) childNode1).put("in", jsonStringBuilder.toString());
		((ObjectNode) childNode1).put("out", jsonStringBuilder.toString());
		((ObjectNode) childNode1).put("in_ts", startTime);
		((ObjectNode) childNode1).put("out_ts", endTime);

		((ObjectNode) rootNode).set(functionName, childNode1);

		String jsonString = "";
		try {
			jsonString = mapper.writeValueAsString(rootNode);
		} catch (Exception e) {
			System.out.println("[ctx.log.err] Cannot create JSON for Redis store");
		}

		try {
			writeToRedis(ctxId, jsonString);
			// Test function for checking JSON structure
			// parse(nodeToStore.toString());
		} catch (Exception e) {
			System.out.println("[ctx.log.error] Write to Redis Failed");
		}
		// Post Data to the next function
		System.out.println("[ctx.log.info] Calling Next Function: " + nextFunction);
		restClient(ctxId, nextFunction, jsonStringBuilder.toString());
		// Return JSON response
		return node.toString();
	}

	// Call this dead end route when next function is not available in environment
	// variables
	@POST
	@Path("/deadend")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public String deadend(@Context HttpHeaders httpHeaders, InputStream incomingData) {
		String functionName = "deadend";
		Instant instant = Instant.now();
		long startTime = instant.toEpochMilli();
		// Read ctxId from Header
		String ctxId = httpHeaders.getRequestHeader("x-ctx-id").get(0);
		// Read the incoming data and Build a String
		StringBuilder jsonStringBuilder = new StringBuilder();
		try {
			BufferedReader in = new BufferedReader(new InputStreamReader(incomingData));
			String line = null;
			while ((line = in.readLine()) != null) {
				jsonStringBuilder.append(line);
			}
		} catch (Exception e) {
			System.out.println("[ctx.log.error] Error Parsing: - ");
		}

		// Parse the string and create object, add ctx and store
		ObjectNode node = JsonNodeFactory.instance.objectNode(); // initializing
		node.put("ctx_id", ctxId);
		node.put("next_function", "deadend");
		long estimatedTime = estimatedTime(startTime);
		node.put("exe_time_ms", estimatedTime); // In Micro Seconds
		System.out.println("[ctx.log.err] Dead end reached as next function not available");
		String valFromRedis = "";
		try {
			valFromRedis = getKeyFromRedis(ctxId);
		} catch (Exception e) {
			System.out.println("[ctx.log.err] ctxId not found");
		}

		// Before Returning this function. Update Redis value for this ctxId
		// Get Value of ctxID
		// Append this value
		// Build a JSON to store in Redis
		// Get EndTime instant
		Instant einstant = Instant.now();
		long endTime = einstant.toEpochMilli();

		// ====================================
		ObjectMapper mapper = new ObjectMapper();
		JsonNode rootNode = mapper.createObjectNode();
		JsonNode objectFromRedis = null;
		try {
			objectFromRedis = mapper.readTree(valFromRedis);
		} catch (Exception e) {

		}

		JsonNode childNode1 = mapper.createObjectNode();
		((ObjectNode) childNode1).put("in", jsonStringBuilder.toString());

		((ObjectNode) childNode1).put("in_ts", startTime);
		((ObjectNode) childNode1).put("out_ts", endTime);

		((ObjectNode) objectFromRedis).set(functionName, childNode1);

		String jsonString = "";
		try {
			jsonString = mapper.writeValueAsString(objectFromRedis);
		} catch (Exception e) {
			System.out.println("[ctx.log.error] Failed to create JSON");
		}

		// ===========================-=========
		try {
			writeToRedis(ctxId, jsonString);
			// Test function for checking JSON structure
			// parse(nodeToStore.toString());
		} catch (Exception e) {
			System.out.println("[ctx.log.error] Write to Redis Failed");
		}

		return node.toString();
	}

	// Get Estimated time for execution
	private Long estimatedTime(Long startTime) {
		Instant instant = Instant.now();
		long estTime = instant.toEpochMilli() - startTime;
		return estTime;
	}

	// Parse the JSONjedis2.set(txnId, "Some va");
	public void parse(String json) throws Exception {
		JsonFactory factory = new JsonFactory();

		ObjectMapper mapper = new ObjectMapper(factory);
		JsonNode rootNode = mapper.readTree(json);

		Iterator<Map.Entry<String, JsonNode>> fieldsIterator = rootNode.fields();
		while (fieldsIterator.hasNext()) {

			Map.Entry<String, JsonNode> field = fieldsIterator.next();
			System.out.println("[ctx.log.info] Key: " + field.getKey() + "\tValue:" + field.getValue());
		}
	}

	// To Inject arbitrary key and value into JSON
	public String injectIntoJson(String json, String key, String val) throws Exception {
		JsonFactory factory = new JsonFactory();

		ObjectMapper mapper = new ObjectMapper(factory);
		JsonNode rootNode = mapper.readTree(json);

		// Add or inject Another Node
		// This should be used to insert payload into Redis

		((ObjectNode) rootNode).put(key, val);

		// ObjectNode addedNode = ((ObjectNode) rootNode).putObject(key);
		// addedNode
		// .put("ikey1", "ival1")
		// .put("ikey2", "ival2")
		// .put("ikey3", "ival3");
		System.out.println(rootNode.toString());
		return rootNode.toString();

	}

	// Use this method to send data to next function or any external url
	// Read the target url from environment vaiable
	public void restClient(String ctxId, String nextFunction, String incomingData) {
		System.setProperty(ClientBuilder.JAXRS_DEFAULT_CLIENT_BUILDER_PROPERTY, "org.jboss.resteasy.client.jaxrs.ResteasyClientBuilder");
		String functionUrl = "";
		int responseCode;
		if (System.getenv("function_url") == null) {
			functionUrl = "http://localhost:8000";
		} else {
			functionUrl = System.getenv("function_url");
		}
		try {
			String url = functionUrl + "/" + nextFunction.toString().trim();
			URL obj = new URL(url);
			HttpURLConnection conn = (HttpURLConnection)obj.openConnection();
			conn.setRequestMethod("POST");
			conn.setRequestProperty("Content-Type","application/json");
			conn.setRequestProperty("x-ctx-id", ctxId);
			conn.setDoOutput(true);
			DataOutputStream dr = new DataOutputStream(conn.getOutputStream());
			dr.writeBytes(incomingData);
			dr.flush();
			dr.close();
			responseCode = conn.getResponseCode();
			/*
			 * //Client client = ClientBuilder.newBuilder().build(); Client client =
			 * ClientBuilder.newClient(); //ResteasyClient client = new
			 * ResteasyClientBuilder().build(); // Get the base URL from environment
			 * variables and build the next function url WebTarget target =
			 * client.target(functionUrl + "/" + nextFunction.toString().trim()); //
			 * Response response = target.request().get(); Response response =
			 * target.request().header("x-ctx-id", ctxId) .post(Entity.entity(incomingData,
			 * "application/json")); String value = response.readEntity(String.class); if
			 * (responseCode != 200 || responseCode !=202) { throw new
			 * RuntimeException("[ctx.log.err] Failed : HTTP error code : " +
			 * response.getStatus()); }
			 * System.out.println("[ctx.log.info] Response from downstream: " + value);
			 * 
			 * response.close();
			 */

		} catch (Exception e) {

			e.printStackTrace();

		}
	}

	// Use this Method to store the incoming requests and outgoing responses with
	// corresponding ctxIds
	// Read redis configuration from environment variables
	public void writeToRedis(String txnId, String value) throws Exception {
		String redisHost = "";
		Integer redisPort = 6379;
		String redisPassword = "";
		if (System.getenv("redis_host") == null) {
			redisHost = "localhost";
		} else {
			redisHost = System.getenv("redis_host");
		}
		
		if (System.getenv("redis_port") == null) {
			redisPort = Integer.parseInt("6379");
		} else {
			redisPassword= System.getenv("redis_port");
		}
		
		if (System.getenv("redis_password") == null) {
			redisPassword = "redispassword";
		} else {
			redisPassword = System.getenv("redis_password");
		}

		//System.out.println("[ctx.log.info] Redis Host : " + redisHost);
		//System.out.println("[ctx.log.info] Redis Port : " + redisPort);
		//System.out.println("[ctx.log.info] Redis Password : " + redisPassword);
		//String ctxId = txnId;
		Jedis jedis = new Jedis(redisHost);
		jedis.auth(redisPassword);

		// Build the JSON based on type and store
		// {"ctx_id" : {
		// "function1" : {"in": { requestBody},
		// "out" : { response Body },
		// "in_ts": 1231231232,
		// "out_ts": 123123123},
		// "function1" : {"in": { requestBody},
		// "out" : { response Body },
		// "in_ts": 1231231232,
		// "out_ts": 123123123},
		// }}

		jedis.set(txnId, value);
		jedis.close();

	}

	public String getKeyFromRedis(String txnId) throws Exception {
		String redisHost = "";
		Integer redisPort = 6379;
		String redisPassword = "";
		if (System.getenv("redis_host") == null) {
			redisHost = "localhost";
		} else {
			redisHost = System.getenv("redis_host");
		}
		
		if (System.getenv("redis_port") == null) {
			redisPort = Integer.parseInt("6379");
		} else {
			redisPassword= System.getenv("redis_port");
		}
		
		if (System.getenv("redis_password") == null) {
			redisPassword = "redispassword";
		} else {
			redisPassword = System.getenv("redis_password");
		}

		//System.out.println("[ctx.log.info] Redis Host : " + redisHost);
		//System.out.println("[ctx.log.info] Redis Port : " + redisPort);
		//System.out.println("[ctx.log.info] Redis Password : " + redisPassword);
		String ctxId = txnId;
		Jedis jedis2 = new Jedis(redisHost);
		jedis2.auth(redisPassword);

		System.out.println("[ctx.log.info] Redis Queried for :" + txnId);
		String keyVal = null;

		try {
			keyVal = jedis2.get(txnId.trim());
		} catch (Exception e) {
			System.out.println("[ctx.log.error] Connection to Redis Failed");
		}

		jedis2.close();
		return keyVal;
	}

}
