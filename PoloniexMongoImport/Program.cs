using System;
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

                    Console.WriteLine(PoloniexDateStampToBytes(bsonRecord.GetValue("Date").ToString()));

                    //Console.WriteLine(bsonRecord.ToString()); //todo: upsert record into db instead of printing to console
                }
            }
        }

        /* 
         * public static byte[] PoloniexDateStampToBytes(String dateStamp)
         * Poloniex specific boolean logic which checks for a valid String based timestamp
         *                                 111111111
         *                       0123456789012345678
         * Strings in the format YYYY-MM-DD hh:mm:ss will return true
         */
        public static bool PoloniexDateStampValid(String dateStamp) {
            return dateStamp.Length == 19 &&                      //check length is 19 chars, then
                   Char.IsNumber(dateStamp, 0) &&                 //check every char in every position of the String (positions 0 - 18, total 19 chars)
                   Char.IsNumber(dateStamp, 1) &&
                   Char.IsNumber(dateStamp, 2) &&
                   Char.IsNumber(dateStamp, 3) &&                 //checking for exact numbers
                        dateStamp.Substring(4,  1).Equals("-") && //checking for exact chars
                   Char.IsNumber(dateStamp, 5) &&
                   Char.IsNumber(dateStamp, 6) &&
                        dateStamp.Substring(7,  1).Equals("-") &&
                   Char.IsNumber(dateStamp, 8) &&
                   Char.IsNumber(dateStamp, 9) &&
                        dateStamp.Substring(10, 1).Equals(" ") &&
                   Char.IsNumber(dateStamp, 11) &&
                   Char.IsNumber(dateStamp, 12) &&
                        dateStamp.Substring(13, 1).Equals(":") &&
                   Char.IsNumber(dateStamp, 14) &&
                   Char.IsNumber(dateStamp, 15) &&
                        dateStamp.Substring(16, 1).Equals(":") &&
                   Char.IsNumber(dateStamp, 17) &&
                   Char.IsNumber(dateStamp, 18);
        }

        /* 
         * public static byte[] PoloniexDateStampToBytes(String dateStamp)
         * Poloniex specific code which converts a String based timestamp to a 4 byte array which Mongodb can use for the ObjectID
         */
        public static byte[] PoloniexDateStampToBytes(String dateStamp) {
            if(PoloniexDateStampValid(dateStamp)) { //check for valid dateStamp
                //Console.WriteLine(dateStamp);
                return new byte[] { 0x01, 0x01, 0x01, 0x01 }; //todo, logic to create byte array from information contained in the String
            } else {
                Console.WriteLine("found bad dateStamp: " + dateStamp);
                return new byte[] { 0x00, 0x00, 0x00, 0x00 };
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
