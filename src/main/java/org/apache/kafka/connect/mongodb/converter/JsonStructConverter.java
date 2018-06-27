package org.apache.kafka.connect.mongodb.converter;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.bson.BsonTimestamp;
import org.bson.Document;

/**
 * Struct converter who stores mongodb document as Json String.
 * 
 * @author André Ignacio
 */
public class JsonStructConverter implements StructConverter {

	@Override
	public Struct toStruct(Document document, Schema schema) {
		final Struct messageStruct = new Struct(schema);
		final BsonTimestamp bsonTimestamp = (BsonTimestamp) document.get("ts");
		final Integer seconds = bsonTimestamp.getTime();
		final Integer order = bsonTimestamp.getInc();
		messageStruct.put("timestamp", seconds);
		messageStruct.put("order", order);
		messageStruct.put("operation", document.get("op"));
		messageStruct.put("database", document.get("ns"));
		
		final Document modifiedDocument = (Document) document.get("o");
		
		if(modifiedDocument.get("_id") == null) {
		    modifiedDocument.put("_id", ((Document) document.get("o2")).get("_id"));
		}
		
		// modifiedDocument.computeIfAbsent("_id", k -> {return ((Document) document.get("o2")).get("_id");} );

		messageStruct.put("object", modifiedDocument.toJson());

		return messageStruct;
	}

}
