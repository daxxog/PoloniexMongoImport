using System;
using System.IO;
using System.Security.Cryptography;
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

                ulong csvLine = 0;
                while (csv.Read()) {
                    csvLine++;
                    BsonDocument bsonRecord = new BsonDocument();

                    //fill the BsonDocument with all the values from the current line in the CSV file
                    foreach(String header in csv.Context.HeaderRecord) {
                        CsvFlowBson(header, csv, bsonRecord);
                    }

                    //create a static ObjectID based on the "Date" col
                    byte[] dateBytes = PoloniexDateStampToBytes(bsonRecord.GetValue("Date").ToString());
                    byte[] dateHash = MD5.Create().ComputeHash(dateBytes);
                    ObjectId idStatic = new ObjectId(MergeBytes12(dateHash, dateBytes, csvLine));

                    //set the "_id" to our newly generated ObjectId
                    bsonRecord.SetElement(new BsonElement("_id", idStatic));
                    
                    Console.WriteLine(bsonRecord.ToString()); //todo: upsert record into db instead of printing to console
                }
            }
        }

        /* 
         * public static bool PoloniexDateStampValid(String dateStamp)
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
            var bytes4 = new byte[] { 0x00, 0x00, 0x00, 0x00 }; //4 bytes we return

            if(PoloniexDateStampValid(dateStamp)) { //check for valid dateStamp
                //Parse each digit in the dateStamp
                var y0 = Convert.ToInt16(dateStamp.Substring(0,  1)) * 1000; //millennia
                var y1 = Convert.ToInt16(dateStamp.Substring(1,  1)) * 100; //century
                var y2 = Convert.ToInt16(dateStamp.Substring(2,  1)) * 10; //decade
                var y3 = Convert.ToInt16(dateStamp.Substring(3,  1)); //year
                var year = y0 + y1 + y2 + y3; //full year (4 digits)

                var m5 = Convert.ToInt16(dateStamp.Substring(5,  1)) * 10; //month (tens)
                var m6 = Convert.ToInt16(dateStamp.Substring(6,  1)); //month (ones)
                var month = m5 + m6; //month (1-12) of the year

                var d8 = Convert.ToInt16(dateStamp.Substring(8,  1)) * 10; //day (tens)
                var d9 = Convert.ToInt16(dateStamp.Substring(9,  1)); //day (ones)
                var day = d8 + d9; //day of the month

                var h11 = Convert.ToInt16(dateStamp.Substring(11,  1)) * 10; //hour (tens)
                var h12 = Convert.ToInt16(dateStamp.Substring(12,  1)); //hour (ones)
                var hour = h11 + h12; //hour of the day

                var m14 = Convert.ToInt16(dateStamp.Substring(14,  1)) * 10; //minute (tens)
                var m15 = Convert.ToInt16(dateStamp.Substring(15,  1)); //minute (ones)
                var minute = m14 + m15; //minute of the hour

                var s17 = Convert.ToInt16(dateStamp.Substring(17,  1)) * 10; //second (tens)
                var s18 = Convert.ToInt16(dateStamp.Substring(18,  1)); //second (ones)
                var second = s17 + s18; //second of the minute

                var dt = new DateTime(year, month, day, hour, minute, second, DateTimeKind.Utc); //create DateTime object using the parsed data
                var oid = new ObjectId(dt, 0, 0, 0); //fake ObjectId just to do the binary conversion
                var bytes12 = oid.ToByteArray(); //12 byte ObjectId, we only want the first 4 bytes

                //copy the first four bytes for our return value
                bytes4[0] = bytes12[0];
                bytes4[1] = bytes12[1];
                bytes4[2] = bytes12[2];
                bytes4[3] = bytes12[3];

                return bytes4;
            } else {
                Console.WriteLine("found bad dateStamp: " + dateStamp);
                return bytes4;
            }
        }

        /*
         * public static byte[] MergeBytes12(byte[] _bytes12, byte[] bytes4, ulong int8)
         * Create 12 byte merged data from: (ObjectID compatible)
         * _bytes12 = 12 byte "random" data returned value may contain some of this data.
         * bytes4 = 4 byte header. Will always be the first 4 bytes of the returned data.
         * int8 = 64 bit unsigned integer which is casted into an 8 byte array.
         *        Zero values from the resulting array are replaced with data from _bytes12.
         */
        public static byte[] MergeBytes12(byte[] _bytes12, byte[] bytes4, ulong int8) {
            byte[] bytes8 = BitConverter.GetBytes(int8); //64 bit unsigned integer converted to byte array (8 bytes)
            byte[] bytes12 = new byte[12]; //new byte array which will hold the return value

            if(BitConverter.IsLittleEndian) { //we always want our 8 byte array to be BigEndian, as in the MongoDB spec
                Array.Reverse(bytes8);
            }

            //copy and merge _bytes12 with non zeros of byte casted int8 into bytes12 (return value)
            for (int i = 0; i <= 11; i++) {
                bytes12[i] = _bytes12[i]; //copy the first 12 bytes from _bytes12 to our newly created bytes12

                if(i > 3) {
                    if(bytes8[i - 4] != 0) { //we can use this data if its not a zero
                        bytes12[i] = bytes8[i - 4];
                    }
                }
            }

            //overwrite the front of the 12 byte array with our 4 byte header
            bytes12[0] = bytes4[0];
            bytes12[1] = bytes4[1];
            bytes12[2] = bytes4[2];
            bytes12[3] = bytes4[3];

            //return the resulting data
            return bytes12;
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
