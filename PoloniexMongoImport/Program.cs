using System;
//using System.Collections.Generic;
using System.IO;
using CsvHelper;
using MongoDB.Bson;

namespace PoloniexMongoImport {
    class MainClass {
        public static void Main(string[] args) {
            Console.WriteLine("importing csv file..");

            using (StreamReader reader = new StreamReader("/Users/daxxog/Downloads/tradeHistory.csv"))

            using (var csv = new CsvReader(reader)) {
                csv.Read();
                csv.ReadHeader();

                while (csv.Read()) {
                    BsonDocument bsonRecord = new BsonDocument();

                    foreach(String header in csv.Context.HeaderRecord) {
                        CsvFlowBson(header, csv, bsonRecord);
                    }

                    Console.WriteLine(bsonRecord.ToString()); //todo: insert record into db instead of printing to console
                }
            }
        }

        /* 
         * CsvFlowBson(String field, CsvReader csv, BsonDocument bson)
         * Pour a CsvReader field into a BSON document
         * (replaces any existing element with the same name or adds a new element if an element with the same name is not found)
         */
        public static void CsvFlowBson(String field, CsvReader csv, BsonDocument bson) {
            bson.SetElement(GetFieldElement(csv, field));
        }

        /* 
         * GetFieldElement(CsvReader csv, String field) -> BsonElement
         * Wrapper for csv.GetField and new BsonElement
         */
        public static BsonElement GetFieldElement(CsvReader csv, String field) {
            return new BsonElement(field, BsonValue.Create(csv.GetField(field)));
        }
    }
}
