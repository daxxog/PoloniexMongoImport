using System;
using System.Collections.Generic;
using System.IO;
using CsvHelper;

namespace PoloniexMongoImport {
    class MainClass {
        public static void Main(string[] args) {
            Console.WriteLine("importing csv file..");

            using (StreamReader reader = new StreamReader("/Users/daxxog/Downloads/tradeHistory.csv"))

            using (var csv = new CsvReader(reader)) {
                var records = new List<PoloniexRowData>();
                csv.Read();
                csv.ReadHeader();

                while (csv.Read()) {
                    var record = new PoloniexRowData {
                        Date = csv.GetField("Date"),
                        Market = csv.GetField("Market"),
                        Category = csv.GetField("Category"),
                        Type = csv.GetField("Type"),
                        Price = csv.GetField("Price"),
                        Amount = csv.GetField("Amount"),
                        Total = csv.GetField("Total"),
                        Fee = csv.GetField("Fee"),
                        OrderNumber = csv.GetField("Order Number"),
                        BaseTotalLessFee = csv.GetField("Base Total Less Fee"),
                        QuoteTotalLessFee = csv.GetField("Quote Total Less Fee")
                    };

                    records.Add(record);
                }
            }
        }
    }

    public class PoloniexRowData {
        /* Structure:
         * Date,Market,Category,Type,Price,Amount,Total,Fee,Order Number,Base Total Less Fee,Quote Total Less Fee
         * -{ Date }-
         * -{ Market }-
         * -{ Category }-
         * -{ Type }-
         * -{ Price }-
         * -{ Amount }-
         * -{ Total }-
         * -{ Fee }-
         * -{ OrderNumber }-
         * -{ BaseTotalLessFee }-
         * -{ QuoteTotalLessFee }-
         */

        public string Date { get; set; }
        public string Market { get; set; }
        public string Category { get; set; }
        public string Type { get; set; }
        public string Price { get; set; }
        public string Amount { get; set; }
        public string Total { get; set; }
        public string Fee { get; set; }
        public string OrderNumber { get; set; }
        public string BaseTotalLessFee { get; set; }
        public string QuoteTotalLessFee { get; set; }
    }
}
