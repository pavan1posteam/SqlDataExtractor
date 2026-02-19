using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.IO.Compression;
using Npgsql;


namespace SqlDataExtractor
{
    public class DataExtractor
    {
        public static int count = 0;
        public static void TransformData()
        {
            string outputFolder = @"C:\Bottlecapps\New folder\Data";
            if (Directory.Exists(outputFolder))
            {
                Directory.Delete(outputFolder, true);
            }
            Console.WriteLine("Select DataBase Type: \n 1. SQL Server \n 2. PostgreSQL ");
            var selection = Console.ReadLine();
            if(selection == "1")// SQL Server
            {
                Console.WriteLine("Enter Server Name: ");
                var srName = Console.ReadLine();
                Console.WriteLine("Enter DataBase Name: ");
                var dbName = Console.ReadLine();
                Console.WriteLine("Windows Authentication(Y/N): ");
                var authType = Console.ReadLine();
                string connectionString = "";
                if (authType.Equals("Y", StringComparison.OrdinalIgnoreCase))
                {
                    connectionString = $"Data Source={srName};Initial Catalog={dbName};Trusted_Connection=True;";
                }
                else if (authType.Equals("N", StringComparison.OrdinalIgnoreCase))
                {
                    Console.WriteLine("Enter User Name: ");
                    var urName = Console.ReadLine();
                    Console.WriteLine("Enter DB Password Name: ");
                    var pdName = Console.ReadLine();
                    connectionString = $"Server={srName};Database={dbName};User Id={urName};Password={pdName};";
                }
                else
                {
                    Console.WriteLine("Invalid Selection.");
                    Thread.Sleep(2000);
                    return;
                }
                Console.WriteLine($"Connecting to SQL Server {srName} ...");
                Directory.CreateDirectory(outputFolder);
                try
                {
                    using (SqlConnection connection = new SqlConnection(connectionString))
                    {
                        connection.Open();
                        Console.WriteLine("Connected Successfully");
                        DataTable tables = connection.GetSchema("Tables");
                        int tableCount = tables.Rows.Count;

                        Console.WriteLine($"Found {tableCount} tables. Starting export...");
                        int exportedTables = 0;

                        foreach (DataRow row in tables.Rows)
                        {
                            string schemaName = row["TABLE_SCHEMA"].ToString();
                            string tableName = row["TABLE_NAME"].ToString();
                            Console.WriteLine($"Exporting table: {schemaName}.{tableName}");
                            try
                            {
                                string query = $"SELECT * FROM [{schemaName}].[{tableName}];";
                                using (SqlCommand command = new SqlCommand(query, connection))
                                using (SqlDataReader reader = command.ExecuteReader())
                                {
                                    string filePath = Path.Combine(outputFolder, $"{schemaName}_{tableName}.csv");
                                    using (StreamWriter writer = new StreamWriter(filePath))
                                    {
                                        for (int i = 0; i < reader.FieldCount; i++)
                                        {
                                            writer.Write(reader.GetName(i));
                                            if (i < reader.FieldCount - 1)
                                                writer.Write(",");
                                        }
                                        writer.WriteLine();
                                        while (reader.Read())
                                        {
                                            for (int i = 0; i < reader.FieldCount; i++)
                                            {
                                                writer.Write(reader[i].ToString().Replace(",", " "));
                                                if (i < reader.FieldCount - 1)
                                                    writer.Write(",");
                                            }
                                            writer.WriteLine();
                                        }
                                    }
                                }
                                exportedTables++;
                                Console.WriteLine($"Table {schemaName}.{tableName} exported ({exportedTables}/{tableCount})");
                            }
                            catch (Exception tableEx)
                            {
                                Console.WriteLine($"Error exporting table {schemaName}.{tableName}: {tableEx.Message}");
                            }
                        }
                        Console.WriteLine($"All {exportedTables} tables Exported Successfully.");
                    }
                    string zipPath = @"C:\Bottlecapps\New folder\Data.zip";
                    if (File.Exists(zipPath))
                    {
                        File.Delete(zipPath);
                    }
                    ZipFile.CreateFromDirectory(outputFolder, zipPath);
                    Directory.Delete(outputFolder, true);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error: {ex.Message}");
                }
                Thread.Sleep(4000);
                return;
            }
            else if (selection == "2")// PostgreSQL
            {
                Console.WriteLine("Enter Server Name: ");
                var srName = Console.ReadLine();
                Console.WriteLine("Enter DataBase Name: ");
                var dbName = Console.ReadLine();
                Console.WriteLine("Enter User Name: ");
                var urName = Console.ReadLine();
                Console.WriteLine("Enter DB Password Name: ");
                var pdName = Console.ReadLine();
                string connectionString = $"Host={srName};Database={dbName};User id= {urName}; password={pdName}";//Port= 5432
                Directory.CreateDirectory(outputFolder);

                try
                {
                    using (NpgsqlConnection connection = new NpgsqlConnection(connectionString))
                    {
                        connection.Open();
                        Console.WriteLine("Connected successfully");
                        DataTable tables = connection.GetSchema("Tables");
                        int tableCount = tables.Rows.Count;

                        Console.WriteLine($"Found {tableCount} tables. Starting export...");
                        int exportedTables = 0;

                        foreach (DataRow row in tables.Rows)
                        {
                            string schemaName = row["TABLE_SCHEMA"].ToString();
                            string tableName = row["TABLE_NAME"].ToString();
                            Console.WriteLine($"Exporting table: {schemaName}.{tableName}");

                            try
                            {
                                string query = $"SELECT * FROM \"{schemaName}\".\"{tableName}\"";
                                using (NpgsqlCommand command = new NpgsqlCommand(query, connection))
                                using (NpgsqlDataReader reader = command.ExecuteReader())
                                {
                                    string filePath = Path.Combine(outputFolder, $"{schemaName}_{tableName}.csv");
                                    using (StreamWriter writer = new StreamWriter(filePath))
                                    {
                                        for (int i = 0; i < reader.FieldCount; i++)
                                        {
                                            writer.Write(reader.GetName(i));
                                            if (i < reader.FieldCount - 1)
                                                writer.Write(",");
                                        }
                                        writer.WriteLine();
                                        while (reader.Read())
                                        {
                                            for (int i = 0; i < reader.FieldCount; i++)
                                            {
                                                writer.Write(reader[i].ToString().Replace(",", " ")); 
                                                if (i < reader.FieldCount - 1)
                                                    writer.Write(",");
                                            }
                                            writer.WriteLine();
                                        }
                                    }
                                }
                                exportedTables++;
                                Console.WriteLine($"Table {schemaName}.{tableName} exported ({exportedTables}/{tableCount})");
                            }
                            catch (Exception tableEx)
                            {
                                Console.WriteLine($"Error exporting table {schemaName}.{tableName}: {tableEx.Message}");
                            }
                        }
                        Console.WriteLine($"All {exportedTables} tables exported successfully.");
                        string zipPath = @"C:\Bottlecapps\New folder\Data.zip";
                        if (File.Exists(zipPath))
                        {
                            File.Delete(zipPath);
                        }
                        ZipFile.CreateFromDirectory(outputFolder, zipPath);
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error: {ex.Message}");
                }
                Thread.Sleep(4000);
                return;
            }
            else
            {
                count++;
                Console.WriteLine("Invalid selection. Please select either 1 or 2.");
                if(count < 3)
                {
                    Thread.Sleep(2000);
                    TransformData();
                }
                else
                {
                    Console.WriteLine("Too many Invalid Attempts....");
                    Thread.Sleep(2000);
                    return;
                }
            }
        }
    }
}
