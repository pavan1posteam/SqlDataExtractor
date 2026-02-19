using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using Npgsql;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlDataExtractor
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.Title = "SQL Data Extractor";
            DataExtractor.TransformData();
        }
        #region PSQL DATA EXTRACTOR       
        //static void Main(string[] args)
        //{
        //    string connectionString = "Host=127.0.0.1;Port= 5432;Database=POS;User id= pos; password=Wendel1";
        //    //connectionString = "Server=AAJ-POS\\PCAMERICA;Database=lmart;Userid=sa;Password=pcAmer1ca;";
        //    string outputFolder = @"C:\Bottlecapps\New folder\Data";

        //    // Create the output directory if it doesn't exist
        //    if (!Directory.Exists(outputFolder))
        //    {
        //        Directory.CreateDirectory(outputFolder);
        //    }

        //    try
        //    {
        //        using (NpgsqlConnection connection = new NpgsqlConnection(connectionString))
        //        {
        //            connection.Open();
        //            Console.WriteLine("Connected successfully");

        //            // Get the schema for all tables
        //            DataTable tables = connection.GetSchema("Tables");
        //            int tableCount = tables.Rows.Count;

        //            Console.WriteLine($"Found {tableCount} tables. Starting export...");

        //            // Initialize a counter for progress tracking
        //            int exportedTables = 0;

        //            foreach (DataRow row in tables.Rows)
        //            {
        //                string schemaName = row["TABLE_SCHEMA"].ToString();
        //                string tableName = row["TABLE_NAME"].ToString();
        //                Console.WriteLine($"Exporting table: {schemaName}.{tableName}");

        //                try
        //                {
        //                    // Query to fetch data from the table with schema
        //                    string query = $"SELECT * FROM \"{schemaName}\".\"{tableName}\"";
        //                    using (NpgsqlCommand command = new NpgsqlCommand(query, connection))
        //                    using (NpgsqlDataReader reader = command.ExecuteReader())
        //                    {
        //                        // Create CSV file for the table
        //                        string filePath = Path.Combine(outputFolder, $"{schemaName}_{tableName}.csv");
        //                        using (StreamWriter writer = new StreamWriter(filePath))
        //                        {
        //                            // Write header row
        //                            for (int i = 0; i < reader.FieldCount; i++)
        //                            {
        //                                writer.Write(reader.GetName(i));
        //                                if (i < reader.FieldCount - 1)
        //                                    writer.Write(",");
        //                            }
        //                            writer.WriteLine();

        //                            // Write data rows
        //                            while (reader.Read())
        //                            {
        //                                for (int i = 0; i < reader.FieldCount; i++)
        //                                {
        //                                    writer.Write(reader[i].ToString().Replace(",", " ")); // Replace commas in data to avoid CSV issues
        //                                    if (i < reader.FieldCount - 1)
        //                                        writer.Write(",");
        //                                }
        //                                writer.WriteLine();
        //                            }
        //                        }
        //                    }

        //                    exportedTables++;
        //                    Console.WriteLine($"Table {schemaName}.{tableName} exported ({exportedTables}/{tableCount})");
        //                }
        //                catch (Exception tableEx)
        //                {
        //                    // Log specific table errors and continue
        //                    Console.WriteLine($"Error exporting table {schemaName}.{tableName}: {tableEx.Message}");
        //                }
        //            }

        //            Console.WriteLine($"All {exportedTables} tables exported successfully.");
        //        }
        //    }
        //    catch (Exception ex)
        //    {
        //        Console.WriteLine($"Error: {ex.Message}");
        //    }

        //    Console.ReadLine();
        //}
        #endregion
        #region SQL DATA EXTRACTOR
        //static void Main(string[] args)
        //{
        //    Console.Title = "SQL Data Extractor";
        //    DataExtractor data = new DataExtractor();
        //    data.TransformData();
        //    return;
        //    //string connectionString = "Server=WSRM-SQL;Database=WSRC;Trusted_Connection=True;";
        //    //string connectionString = "Server=WSRM-SQL;Database=WSRC;Userid=SA;Password=Tri4400t3ad1m$;Trusted_Connection=True;";
        //    string connectionString = "Data Source=WSRM-SQL;Initial Catalog=WSRC;User Id=SA;Password=Tri4400t3ad1m$;";//WSRM (NCR)
        //    //connectionString = "Server=WIN-QTGKUDUTNAG;Database=Cosmo2016;Trusted_Connection=True;";//CosmoTech POS
        //    //connectionString = "Server=SERVER;Database=belmonte5000;Trusted_Connection=True;";//Microsoft Diamond Scan
        //    connectionString = "Server=DESKTOP-RTEM937\\SQLEXPRESS;Database=1stpos;Trusted_Connection=True;";//1st POS
        //    //connectionString = "Server=POS-3\\CAPSERVER;Database=CAPDATA4;Trusted_Connection=True;";//CAP POS
        //    //connectionString = "Server=SERVER-PC\\TIGERPOS;Database=POSDB;Trusted_Connection=True;";//TIGER POS
        //    //connectionString = "Server=EVSERVER22\\SQLEXPRESS;Database=EVANS22;Trusted_Connection=True;";//Microsoft RMS
        //    //connectionString = "Server=Pyramid2\\CAPSERVER;Database=CAPDATA1;Trusted_Connection=True;";//POSNATION CAP
        //    //connectionString = "Server=FRIENDSHIP01\\RETAILERBV;Database=rsi;User Id=sa;Password=PoSR8s0i6;";//THE RETAILER BUSSINESS VIEW
        //    //connectionString = "Server=SERVER65;Database=drlnew;Trusted_Connection=True;";//POS Positive//SERVER001901\SQLEXPRESS
        //    //connectionString = "Server=SERVER001901\\SQLEXPRESS;Database=STORESQL;Trusted_Connection=True;";//SMS
        //    //connectionString = "Data Source = localhost\\PCAMERICA; Initial Catalog = cresqlabc; User Id = sa; Password = pcAmer1ca; ";//WineExpress store
        //    connectionString = "Server=EWS072025;Database=EdmondWineShop;User Id=sa;Password=8634?(edm)~!;";
        //    string outputFolder = @"C:\Bottlecapps\New folder\Data";
           
        //    // Create the output directory if it doesn't exist
        //    if (!Directory.Exists(outputFolder))
        //    {
        //        Directory.CreateDirectory(outputFolder);
        //    }

        //    try
        //    {
        //        using (SqlConnection connection = new SqlConnection(connectionString))
        //        {
        //            connection.Open();
        //            Console.WriteLine("Connected successfully");

        //            // Get the schema for all tables
        //            DataTable tables = connection.GetSchema("Tables");
        //            int tableCount = tables.Rows.Count;

        //            Console.WriteLine($"Found {tableCount} tables. Starting export...");

        //            // Initialize a counter for progress tracking
        //            int exportedTables = 0;

        //            foreach (DataRow row in tables.Rows)
        //            {
        //                string schemaName = row["TABLE_SCHEMA"].ToString();
        //                string tableName = row["TABLE_NAME"].ToString();
        //                Console.WriteLine($"Exporting table: {schemaName}.{tableName}");
                        
        //                /*if (tableName == "Inventory" || tableName == "Departments") { }
        //                else { continue; }*/
        //                try
        //                {
        //                    // Query to fetch data from the table with schema
        //                    string query = $"SELECT * FROM [{schemaName}].[{tableName}];";
        //                    using (SqlCommand command = new SqlCommand(query, connection))
        //                    using (SqlDataReader reader = command.ExecuteReader())
        //                    {
        //                        // Create CSV file for the table
        //                        string filePath = Path.Combine(outputFolder, $"{schemaName}_{tableName}.csv");
        //                        using (StreamWriter writer = new StreamWriter(filePath))
        //                        {
        //                            // Write header row
        //                            for (int i = 0; i < reader.FieldCount; i++)
        //                            {
        //                                writer.Write(reader.GetName(i));
        //                                if (i < reader.FieldCount - 1)
        //                                    writer.Write(",");
        //                            }
        //                            writer.WriteLine();

        //                            // Write data rows
        //                            while (reader.Read())
        //                            {
        //                                for (int i = 0; i < reader.FieldCount; i++)
        //                                {
        //                                    writer.Write(reader[i].ToString().Replace(",", " ")); // Replace commas in data to avoid CSV issues
        //                                    if (i < reader.FieldCount - 1)
        //                                        writer.Write(",");
        //                                }
        //                                writer.WriteLine();
        //                            }
        //                        }
        //                    }
        //                    exportedTables++;
        //                    Console.WriteLine($"Table {schemaName}.{tableName} exported ({exportedTables}/{tableCount})");
        //                }
        //                catch (Exception tableEx)
        //                {
        //                    // Log specific table errors and continue
        //                    Console.WriteLine($"Error exporting table {schemaName}.{tableName}: {tableEx.Message}");
        //                }
        //            }

        //            Console.WriteLine($"All {exportedTables} tables exported successfully.");
        //        }
        //    }
        //    catch (Exception ex)
        //    {
        //        Console.WriteLine($"Error: {ex.Message}");
        //    }

        //    Console.ReadLine();
        //}
        #endregion
    }
}
