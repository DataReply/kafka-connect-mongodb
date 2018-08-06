package org.apache.kafka.connect.utils;

import com.mongodb.MongoClient;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.bson.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class ConverterUtils {

  private final static Logger log = LoggerFactory.getLogger(ConverterUtils.class);

  public static Struct createDynamicStruct(Schema customSchema, Map<String, Object> doc, Boolean skipId) {
    Struct customStruct = new Struct(customSchema);
    Iterator<String> itKeys = doc.keySet().iterator();
    while(itKeys.hasNext()){
      String key = itKeys.next();
      Object value = doc.get(key);
      log.debug("key_Struct: " + key);
      log.debug("value_Struct: " + doc.get(key));
      Document generic = new Document(key, value);
      BsonDocument bsonDocument =
        generic.toBsonDocument(
          BsonDocument.class, MongoClient.getDefaultCodecRegistry());
      BsonValue bsonValue = bsonDocument.get(key);

      try {
        //skip "_id"
        if (key.equals("_id") && skipId) {
          continue;
        }

        //value is an array
        else if (bsonValue.isArray()) {
          BsonArray bsonArray = bsonValue.asArray();
          //empty array|isString|isInt32|isInt64|isDouble|isBoolean
          if (bsonArray.size() == 0
            || ( !bsonArray.get(0).isDocument() && !bsonArray.get(0).isDateTime())) {
            customStruct.put(key, value);
          }

          //array of Documents TODO is not working yet
          else if (bsonArray.get(0).isDocument()) {
//              List objectList = (List) value;
//              Schema elementSchema = createDynamicSchema((Map<String, Object>) objectList.get(0));
//              List<Struct> structList = new ArrayList<>();
//              for(Object object : objectList){
//                structList.add(createDynamicStruct(elementSchema, (Map<String, Object>)object));
//              }
//              customStruct.put(key, structList);
            List<String> docStrList = new ArrayList<>();
            for(BsonValue element: bsonArray){
              docStrList.add(element.asDocument().toJson());
            }
            customStruct.put(key, docStrList);
          }

          //array of Dates
          else {
            List<String> dateStrList = new ArrayList<>();
            for(BsonValue bsonDate: bsonArray){
              long bsonDateLong = bsonDate.asDateTime().getValue();
              LocalDateTime date = LocalDateTime.ofInstant(Instant.ofEpochMilli(bsonDateLong),
                ZoneId.of("Z"));
              DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
              dateStrList.add(date.format(formatter));
            }
            customStruct.put(key, dateStrList);
          }
        }

        //value isDocument
        else if (bsonValue.isDocument() && !bsonValue.isObjectId()) {
          customStruct.put(key, createDynamicStruct(
            customSchema.field(key).schema(), (Map<String, Object>) value, skipId));
        }

        //value isDateTime
        else if (bsonValue.isDateTime()) {
          long bsonDateLong = bsonValue.asDateTime().getValue();
          LocalDateTime date = LocalDateTime.ofInstant(Instant.ofEpochMilli(bsonDateLong),
            ZoneId.of("Z"));
          DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
          customStruct.put(key, date.format(formatter));
        }

        //value is undefined
        else if(value instanceof BsonUndefined){
          log.debug("key: {} is undefined", key);
          continue;
        }

        //value is undefined
        else if(bsonValue.isNull()){
          log.debug("key: {} is null", key);
          continue;
        }

        //value isString|isInt64|isInt32|isDouble|isBoolean
        else {
          customStruct.put(key, value);
        }
      }
      catch (Exception e){
        e.printStackTrace();
        log.error("Error while getting dynamicStruct: " + e.getMessage());
        return null;
      }
    }
    return customStruct;
  }

  public static Schema createDynamicSchema(Map<String, Object> doc, Boolean skipId){

    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    Iterator<String> itKeys = doc.keySet().iterator();
    while(itKeys.hasNext()){
      String key = itKeys.next();
      Object value = doc.get(key);
      Document generic = new Document(key, value);
      BsonDocument bsonDocument =
        generic.toBsonDocument(
          BsonDocument.class, MongoClient.getDefaultCodecRegistry());
      BsonValue bsonValue = bsonDocument.get(key);

      //skip "_id"
      if(key.equals("_id") && skipId){
        continue;
      }

      //value isString|isDateTime
      else if(bsonValue.isString() || bsonValue.isDateTime()){
        schemaBuilder.field(key, Schema.STRING_SCHEMA);
      }

      //value is an array
      else if(bsonValue.isArray()){
        BsonArray bsonArray = bsonValue.asArray();
        //it is an array of string or the array is empty
        schemaBuilder.field(key, getSchemaFromList(key, value));
      }

      //value isDocument
      else if(bsonValue.isDocument()){
        log.debug("key: {} isDocument", key);
        schemaBuilder.field(key, createDynamicSchema((Map<String, Object>) value, skipId));
      }

      //value isInt64
      else if(bsonValue.isInt64()){
        log.debug("key: {} isInt64", key);
        schemaBuilder.field(key, Schema.INT64_SCHEMA);
      }

      //value isInt32
      else if(bsonValue.isInt32()){
        log.debug("key: {} isInt32", key);
        schemaBuilder.field(key, Schema.INT32_SCHEMA);
      }

      //value isDouble
      else if(bsonValue.isDouble()){
        schemaBuilder.field(key, Schema.FLOAT64_SCHEMA);
      }

      //value isBoolean
      else if(bsonValue.isBoolean()){
        schemaBuilder.field(key, Schema.BOOLEAN_SCHEMA);
      }

      //value is undefined
      else if(value instanceof BsonUndefined){
        log.debug("key: {} is undefined", key);
        continue;
      }

      //value is undefined
      else if(bsonValue.isNull()){
        log.debug("key: {} is null", key);
        continue;
      }

      else{
        log.error("schema option not implemented");
        return null;
      }
    }
    return schemaBuilder.build();
  }

  private static Schema getSchemaFromList(String key, Object value){
    Document generic = new Document(key, value);
    BsonDocument bsonDocument =
      generic.toBsonDocument(
        BsonDocument.class, MongoClient.getDefaultCodecRegistry());
    BsonArray array = bsonDocument.get(key).asArray();

    if(array.size() == 0
      || array.get(0).isString() || array.get(0).isDateTime()){
      return SchemaBuilder.array(Schema.STRING_SCHEMA).schema();
    }

    //array of Int32
    else if(array.get(0).isInt32()){
      return SchemaBuilder.array(Schema.INT32_SCHEMA).schema();
    }

    //array of Int64
    else if(array.get(0).isInt64()){
      return SchemaBuilder.array(Schema.INT64_SCHEMA).schema();
    }

    //array of isDouble
    else if(array.get(0).isDouble()){
      return SchemaBuilder.array(Schema.FLOAT64_SCHEMA).schema();
    }

    //array of isBoolean
    else if(array.get(0).isBoolean()){
      return SchemaBuilder.array(Schema.BOOLEAN_SCHEMA).schema();
    }

    //array of isDocument TODO not working yet
    else if(array.get(0).isDocument()){
//      List objectList = (List)value;
//      log.debug("objectList(0): {}", objectList.get(0));
//      Schema elementSchema = createDynamicSchema((Map<String, Object>) objectList.get(0));
//      return SchemaBuilder.array(elementSchema).build();
      return SchemaBuilder.array(Schema.STRING_SCHEMA).build();
    }
    else {
      log.error("array schema option not implemented");
      return null;
    }
  }
}