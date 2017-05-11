package org.apache.kafka.connect.utils;

import static org.junit.Assert.assertTrue;

import java.util.Map;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.junit.Before;
import org.junit.Test;

public class SchemaUtilsTest {

	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void shouldSupportTimestamp() {
		Schema schema = SchemaBuilder.struct().field("timestamp", Timestamp.SCHEMA);
		Struct struct = new Struct( schema );
		java.util.Date timestamp = new java.util.Date();
		struct.put("timestamp", new java.util.Date());
		
		Map<String,Object> jsonMap = SchemaUtils.toJsonMap(struct);
		assertTrue(jsonMap.get("timestamp") instanceof java.util.Date);
	}
	@Test
	public void shouldSupportDate() {
		Schema schema = SchemaBuilder.struct().field("date", Date.SCHEMA);
		Struct struct = new Struct( schema );
		java.util.Date date = new java.util.Date();
		struct.put("date", new java.util.Date());
		
		Map<String,Object> jsonMap = SchemaUtils.toJsonMap(struct);
		assertTrue(jsonMap.get("date") instanceof java.util.Date);
	}
	@Test
	public void shouldSupportTime() {
		Schema schema = SchemaBuilder.struct().field("time", Time.SCHEMA);
		Struct struct = new Struct( schema );
		java.util.Date date = new java.util.Date();
		struct.put("time", new java.util.Date());
		
		Map<String,Object> jsonMap = SchemaUtils.toJsonMap(struct);
		assertTrue(jsonMap.get("time") instanceof java.util.Date);
	}
}
