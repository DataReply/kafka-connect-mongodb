package org.apache.kafka.connect.utils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;

/**
 * @author Andrea Patelli
 */
public class SchemaUtils {
    public static Map<String, Object> toJsonMap(Struct struct) {
        Map<String, Object> jsonMap = new HashMap<String, Object>(0);
        List<Field> fields = struct.schema().fields();
        for (Field field : fields) {
            String fieldName = field.name();
            Schema.Type fieldType = field.schema().type();
            String schemaName=field.schema().name();
            switch (fieldType) {
                case STRING:
                    jsonMap.put(fieldName, struct.getString(fieldName));
                    break;
                case INT32:
                	if (Date.LOGICAL_NAME.equals(schemaName) 
                			|| Time.LOGICAL_NAME.equals(schemaName)) {
                		jsonMap.put(fieldName, (java.util.Date) struct.get(fieldName));
                	} else {
                		jsonMap.put(fieldName, struct.getInt32(fieldName));
                	}
                    break;
                case INT16:
                    jsonMap.put(fieldName, struct.getInt16(fieldName));
                    break;
                case INT64:
                	if (Timestamp.LOGICAL_NAME.equals(schemaName)) {
                		jsonMap.put(fieldName, (java.util.Date) struct.get(fieldName));
                	} else {
                		jsonMap.put(fieldName, struct.getInt64(fieldName));
                	}
                    break;
                case FLOAT32:
                    jsonMap.put(fieldName, struct.getFloat32(fieldName));
                    break;
                case STRUCT:
                    jsonMap.put(fieldName, toJsonMap(struct.getStruct(fieldName)));
                    break;
            }
        }
        return jsonMap;
    }
}
