import com.mongodb.CursorType;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import org.bson.BSON;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.BSONTimestamp;

import java.util.ArrayList;

/**
 * Created by a.patelli on 15/02/2016.
 */
public class Main {
    public static void main(String[] args) {
        MongoClient mongoClient = new MongoClient("localhost", 27017);

        MongoDatabase db = mongoClient.getDatabase("local");

        MongoCollection<Document> oplog = db.getCollection("oplog.rs");

        String t = "14557179622";
        Long timestamp = Long.parseLong(t);
        Integer order = new Long(timestamp % 10).intValue();
        timestamp = timestamp / 10;

         Integer finalTimestamp = timestamp.intValue();
         Integer finalOrder = order;

//        Document query = new Document(
//                "$and",
//                new ArrayList<Document>() {{
//                    add(
//                            new Document(
//                                    "fromMigrate",
//                                    new Document("$exists", false)
//                            )
//                    );
//                    add(
//                            new Document(
//                                    "$or",
//                                    new ArrayList<Document>() {{
//                                        add(new Document("op", "i"));
//                                        add(new Document("op", "u"));
//                                        add(new Document("op", "d"));
//                                    }}
//                            )
//                    );
//                    add(
//                            new Document(
//                                    "ts",
//                                    new Document(
//                                            "$gt",
//                                            new BSONTimestamp(finalTimestamp,finalOrder)
//                                    )
//                            )
//                    );
//                }}
//        );

        Bson query = Filters.and(
                Filters.exists("fromMigrate", false),
                Filters.gt("ts", new BSONTimestamp(finalTimestamp, finalOrder)),
                Filters.or(
                        Filters.eq("op", "i"),
                        Filters.eq("op", "u"),
                        Filters.eq("op", "d")
                    )
                );

        Document fields = new Document();
        fields.put("ts", 1);
        fields.put("op", 1);
        fields.put("ns", 1);
        fields.put("o", 1);

        FindIterable<Document> iterable = oplog
                .find(query)
                .sort(new Document("$natural", 1))
                .projection(Projections.include("ts", "op", "ns", "o"))
                .cursorType(CursorType.TailableAwait);

//        iterable.forEach(new Block<Document>() {
//            @Override
//            public void apply(Document document) {
//                System.out.println(document);
//            }
//        });

        for (Document d : iterable) {
            String op = (String) d.get("op");
            String ns = (String) d.get("ns");
            Document o = (Document) d.get("o");

            if (op.equals("i")) {
                System.out.println("INSERT");
                System.out.println("db: " + ns);
                System.out.println("object: " + o);
            } else if (op.equals("u")) {
                System.out.println("UPDATE");
                System.out.println("db: " + ns);
                System.out.println("object: " + o);
            } else if (op.equals("d")) {
                System.out.println("DELETE");
                System.out.println("db: " + ns);
                System.out.println("object: " + o);
            }
            System.out.println(d.get("ts"));
            System.out.println(d);
        }
    }
}
