package org.apache.kafka.connect.mongodb.converter;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.utils.ConverterUtils;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Default struct converter. This converter store mongodb document with .toString().
 *
 * @author André Ignacio
 */
public class StringStructConverter implements StructConverter {

  private final static Logger log = LoggerFactory.getLogger(StringStructConverter.class);

  @Override
  public Struct toStruct(Document document, Schema schema, Boolean getCustomSchema) {
    Struct messageStruct = null;
    if(getCustomSchema){
      Schema customSchema = ConverterUtils.createDynamicSchema((Map<String, Object>)document.get("o"), true);
      messageStruct = ConverterUtils.createDynamicStruct(customSchema, (Map<String, Object>)document.get("o"), true);
      if(customSchema == null || messageStruct == null){
        return getDefault(document, schema);
      }
    }
    else{
      messageStruct = getDefault(document, schema);
    }

    return messageStruct;
  }

  private Struct getDefault(Document document, Schema schema){
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
