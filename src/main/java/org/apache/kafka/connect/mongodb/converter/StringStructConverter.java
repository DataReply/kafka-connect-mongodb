package org.apache.kafka.connect.mongodb.converter;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.bson.BsonTimestamp;
import org.bson.Document;

/**
 * Default struct converter. This converter store mongodb document with .toString().
 * 
 * @author André Ignacio
 */
public class StringStructConverter implements StructConverter {

	@Override
	public Struct toStruct(Document document, Schema schema) {
        Struct messageStruct = new Struct(schema);
        BsonTimestamp bsonTimestamp = (BsonTimestamp) document.get("ts");
        Integer seconds = bsonTimestamp.getTime();
        Integer order = bsonTimestamp.getInc();
        messageStruct.put("timestamp", seconds);
        messageStruct.put("order", order);
        messageStruct.put("operation", document.get("op"));
        messageStruct.put("database", document.get("ns"));
        messageStruct.put("object", document.get("o").toString());
        
        return messageStruct;
	}

}
