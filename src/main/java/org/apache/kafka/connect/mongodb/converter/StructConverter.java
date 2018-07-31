package org.apache.kafka.connect.mongodb.converter;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.bson.Document;

/**
 * Converter a document in a Struct.
 *
 * @author André Ignacio
 */
public interface StructConverter {

	Struct toStruct(Document document, Schema schema, Boolean customSchema);
}
