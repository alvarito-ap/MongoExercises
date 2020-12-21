package edu.comillas.midb;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Projections.excludeId;
import static com.mongodb.client.model.Projections.fields;
import static com.mongodb.client.model.Projections.include;

public class Practica {

    public static void main (String [] args){
        MongoClient client = new MongoClient("127.0.0.1", 27017);
        MongoDatabase db = client.getDatabase("Empleo");

        MongoCollection<Document> collection = db.getCollection("Solicitudes");


        // Pregunta 1
        MongoCursor<Document> cursor = collection.find(
                or(
                        and(new Document("Provincia", "Barcelona"), gte("Dtes_Empleo_mujer_edad_>=45", 100) ),
                        and(eq("Provincia", "Bizkaia"), gte("Dtes_Empleo_mujer_edad_>=45", 7), lte("Dtes_Empleo_mujer_edad_>=45", 10))
                )
        ).projection(fields(include("Provincia", "Municipio", "Dtes_Empleo_mujer_edad_>=45"), excludeId())).iterator();

        try {
            while (cursor.hasNext()){
                System.out.println(cursor.next().toJson());
            }
        }finally {
            cursor.close();
        }

        //Pregunta 2
        List<Bson> query = new ArrayList<>();

        query.add(Aggregates.group("$Provincia", Accumulators.sum("total_Dtes_Empleo_Prov", "$total_Dtes_Empleo")));

        query.add(Aggregates.match(gt("total_Dtes_Empleo_Prov", 1700000)));

        query.add(Aggregates.sort(Sorts.descending("total_Dtes_Empleo_Prov")));

        cursor = collection.aggregate(query).iterator();

        try {
            while (cursor.hasNext()){
                System.out.println(cursor.next().toJson());
            }
        }finally {
            cursor.close();
        }

        //Pregunta 3
        query.clear();

        query.add(Aggregates.match(and(eq("Provincia", "Huelva"), eq("Código_mes", 200703))));

        query.add(Aggregates.group("$Municipio", Accumulators.sum("total_Dtes_Empleo_Municipio", "$total_Dtes_Empleo"),
                Accumulators.sum("total_Dtes_Empleo_Municipio_Servicios", "$Dtes_Empleo_Servicios")));

        Document divide = new Document("$divide", Arrays.asList("$total_Dtes_Empleo_Municipio_Servicios", "$total_Dtes_Empleo_Municipio"));
        Document multiply = new Document("$multiply", Arrays.asList("$Porcentaje", 100));

        query.add(Aggregates.project(Projections.fields( Projections.include("Municipio", "total_Dtes_Empleo_Municipio_Servicios", "total_Dtes_Empleo_Municipio"),
                Projections.computed("Porcentaje", divide)
        )));

        query.add(Aggregates.project(Projections.fields( Projections.include("Municipio", "total_Dtes_Empleo_Municipio_Servicios", "total_Dtes_Empleo_Municipio"),
                Projections.computed("Porcentaje", multiply)
        )));

        query.add(Aggregates.sort(Sorts.descending("Porcentaje")));

        query.add(Aggregates.limit(1));

        cursor = collection.aggregate(query).iterator();

        try {
            while (cursor.hasNext()){
                System.out.println(cursor.next().toJson());
            }
        }finally {
            cursor.close();
        }

        client.close();
    }
}
