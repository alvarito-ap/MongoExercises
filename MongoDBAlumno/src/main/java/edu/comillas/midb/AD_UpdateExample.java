package edu.comillas.midb;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;

public class AD_UpdateExample {
    public static void main(String[] args){
        //TODO Crear un objeto cliente de mongo
        MongoClient mongo = new MongoClient("127.0.0.1", 27017);
        //TODO Obtener la base de datos "Empleo"
        MongoDatabase db1 = mongo.getDatabase("Empleo");

        //TODO obtener la coleccion "Solicitudes"
        //Se define la coleccion sobre la que se va a trabajar ."Solicitudes"
        String collectionName = "Solicitudes";
        //Se obtiene la colleccion de documentos
        MongoCollection<Document> coll = db1.getCollection(collectionName);

        //Se crea un documento con la clausula set
        Document up = new Document("$set", new Document("Provincia","Castelvania"));

        //Se aplican los cambios a aquellos registros con "codigo de CA" igual a 9 y "Codigo_Municipio" igual a 8072 y el mes sea noviembre del 2006
        UpdateResult updateResult = coll.updateMany(and(eq("Código_de_CA",9),eq("Codigo_Municipio",8072),eq("Código_mes",200711)), up);

        //TODO visualizar los cambios por consola


        //TODO cerrar la conexión con la base de datos

    }
}
