﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Data.OracleClient;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
//using Oracle.ManagedDataAccess.Client;

namespace OracleDataMigrator
{
    internal class Program
    {
        //VARCHAR2,DATE,NUMBER,TIMESTAMP(6)
        static void Main()
        {
            string oracleconnectionString = "Data Source=(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=10.10.9.119)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=hsbck)));User Id=hsm86adm;Password=hsm86adm";
            string sqlServerConnectionString = "Server=Sai-sql08-dev;Database=TEST;Integrated Security = true;";

            using (OracleConnection oracleConnection = new OracleConnection(oracleconnectionString))
            {
                oracleConnection.Open();
                using (OracleCommand oracleCommand = oracleConnection.CreateCommand())
                {
                    //oracleCommand.CommandText = "SELECT TEXT FROM ALL_VIEWS Where VIEW_NAME = 'V_F_PDI'";
                    //oracleCommand.CommandText = "SELECT object_name FROM all_objects where owner = 'HSM86ADM' and object_type = 'PACKAGE'";
                    oracleCommand.CommandText = "SELECT * FROM ALL_SOURCE WHERE OWNER = 'HSM86ADM' AND NAME = 'COIL_PKG'";
                    var result = (string)oracleCommand.ExecuteScalar();
                    using (OracleDataReader oracleReader = oracleCommand.ExecuteReader())
                    {
                        DataTable dt = new DataTable();
                        dt.Load(oracleReader);
                    }
                }
            }

            /*
             * Call to Get Oracle Table Definition By Table Name */
            //GetOracleTableDefinition(oracleconnectionString, "COILER_MV");


            /*
            DeleteDataFromSqlServer(sqlServerConnectionString, "SCADA_DATA_XFER");
            GetDataFromOracleAndInsertIntoMSQL(oracleconnectionString, sqlServerConnectionString, "SCADA_DATA_XFER");
            */

            GetTablesFromOracleAndInsertIntoMSQL(oracleconnectionString, sqlServerConnectionString);
            GetProceduresFromOracleAndInsertIntoMSQL(oracleconnectionString, sqlServerConnectionString);
            GetViewsFromOracleAndInsertIntoMSQL(oracleconnectionString, sqlServerConnectionString);
            GetFunctionsFromOracleAndInsertIntoMSQL(oracleconnectionString, sqlServerConnectionString);
            GetTriggersFromOracleAndInsertIntoMSQL(oracleconnectionString, sqlServerConnectionString);
            GetIndexesFromOracleAndInsertIntoMSQL(oracleconnectionString, sqlServerConnectionString);
            GetPackagesFromOracleAndInsertIntoMSQL(oracleconnectionString, sqlServerConnectionString);
            GetTypesFromOracleAndInsertIntoMSQL(oracleconnectionString, sqlServerConnectionString);
            GetSynonymsFromOracleAndInsertIntoMSQL(oracleconnectionString, sqlServerConnectionString);


            GetTablesFromOracleAndCreateIntoMSQL(oracleconnectionString, sqlServerConnectionString);
            Console.ReadLine();
        }

        static void GetTablesFromOracleAndInsertIntoMSQL(string oracleconnectionString, string sqlServerConnectionString)
        {
            try
            {
                Console.WriteLine("Going to get all the 'TABLE' from HSM86ADM Oracle 9i DB...");
                // SQL query to retrieve table names
                string sql = "SELECT table_name FROM all_tables where owner = 'HSM86ADM'";

                // Create a connection to the Oracle database
                using (OracleConnection oracleConnection = new OracleConnection(oracleconnectionString))
                {
                    try
                    {
                        // Open the connection
                        oracleConnection.Open();

                        // Create a command to execute the SQL query
                        using (OracleCommand oracleCommand = oracleConnection.CreateCommand())
                        {
                            oracleCommand.CommandText = sql;

                            // Execute the command and retrieve a data reader
                            using (OracleDataReader oracleReader = oracleCommand.ExecuteReader())
                            {
                                var oracle9iTableNameForTables = "Oracle9iTables";
                                ExecuteMSSQLQuery(sqlServerConnectionString, $"DROP TABLE {oracle9iTableNameForTables}");
                                if (!CheckIfTableExistsInMSSQL(sqlServerConnectionString, oracle9iTableNameForTables))
                                {
                                    ExecuteMSSQLQuery(sqlServerConnectionString, $"Create Table {oracle9iTableNameForTables} ([ID] [int] IDENTITY(1,1) NOT NULL,TableName [VARCHAR](255) NULL)");
                                }

                                DeleteDataFromSqlServer(sqlServerConnectionString, "Oracle9iTables");

                                // Iterate through the result set and print table names
                                while (oracleReader.Read())
                                {
                                    string tableName = oracleReader.GetString(0);
                                    ExecuteMSSQLQuery(sqlServerConnectionString, $"Insert into {oracle9iTableNameForTables} (TableName) Values('{tableName}')");
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        // Handle any exceptions
                        Console.WriteLine("Error: " + ex.Message);
                    }
                    Console.WriteLine("Getting all the 'TABLE' from HSM86ADM Oracle 9i DB has been finished.\n");
                }
            }
            catch (Exception ex)
            {
                Debug.Print(ex.Message);
            }
        }

        static void GetProceduresFromOracleAndInsertIntoMSQL(string oracleconnectionString, string sqlServerConnectionString)
        {
            try
            {
                Console.WriteLine("Going to get all the 'PROCEDURE' from HSM86ADM Oracle 9i DB...");
                // SQL query to retrieve table names
                string sql = "SELECT object_name FROM all_objects where owner = 'HSM86ADM' and object_type = 'PROCEDURE'";

                // Create a connection to the Oracle database
                using (OracleConnection oracleConnection = new OracleConnection(oracleconnectionString))
                {
                    try
                    {
                        // Open the connection
                        oracleConnection.Open();

                        // Create a command to execute the SQL query
                        using (OracleCommand oracleCommand = oracleConnection.CreateCommand())
                        {
                            oracleCommand.CommandText = sql;

                            // Execute the command and retrieve a data reader
                            using (OracleDataReader oracleReader = oracleCommand.ExecuteReader())
                            {
                                var oracle9iTableNameForProcedures = "Oracle9iProcedures";
                                ExecuteMSSQLQuery(sqlServerConnectionString, $"DROP TABLE {oracle9iTableNameForProcedures}");
                                if (!CheckIfTableExistsInMSSQL(sqlServerConnectionString, oracle9iTableNameForProcedures))
                                {
                                    ExecuteMSSQLQuery(sqlServerConnectionString, $"Create Table {oracle9iTableNameForProcedures} ([ID] [int] IDENTITY(1,1) NOT NULL,ProcedureName [VARCHAR](255) NULL)");
                                }
                                
                                DeleteDataFromSqlServer(sqlServerConnectionString, oracle9iTableNameForProcedures);

                                // Iterate through the result set and print table names
                                while (oracleReader.Read())
                                {
                                    string procedureName = oracleReader.GetString(0);
                                    ExecuteMSSQLQuery(sqlServerConnectionString, $"Insert into {oracle9iTableNameForProcedures} (ProcedureName) Values('{procedureName}')");
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        // Handle any exceptions
                        Console.WriteLine("Error: " + ex.Message);
                    }
                    Console.WriteLine("Getting all the 'PROCEDURE' from HSM86ADM Oracle 9i DB has been finished.\n");
                }
            }
            catch (Exception ex)
            {
                Debug.Print(ex.Message);
            }
        }

        static void GetViewsFromOracleAndInsertIntoMSQL(string oracleconnectionString, string sqlServerConnectionString)
        {
            try
            {
                Console.WriteLine("Going to get all the 'VIEW' from HSM86ADM Oracle 9i DB...");
                // SQL query to retrieve table names
                string sql = "SELECT object_name FROM all_objects where owner = 'HSM86ADM' and object_type = 'VIEW'";

                // Create a connection to the Oracle database
                using (OracleConnection oracleConnection = new OracleConnection(oracleconnectionString))
                {
                    try
                    {
                        // Open the connection
                        oracleConnection.Open();

                        // Create a command to execute the SQL query
                        using (OracleCommand oracleCommand = oracleConnection.CreateCommand())
                        {
                            oracleCommand.CommandText = sql;

                            // Execute the command and retrieve a data reader
                            using (OracleDataReader oracleReader = oracleCommand.ExecuteReader())
                            {
                                var oracle9iTableNameForViews = "Oracle9iViews";
                                ExecuteMSSQLQuery(sqlServerConnectionString, $"DROP TABLE {oracle9iTableNameForViews}");
                                if (!CheckIfTableExistsInMSSQL(sqlServerConnectionString, oracle9iTableNameForViews))
                                {
                                    ExecuteMSSQLQuery(sqlServerConnectionString, $"Create Table {oracle9iTableNameForViews} ([ID] [int] IDENTITY(1,1) NOT NULL,ViewName [VARCHAR](255) NULL)");
                                }

                                DeleteDataFromSqlServer(sqlServerConnectionString, oracle9iTableNameForViews);

                                // Iterate through the result set and print table names
                                while (oracleReader.Read())
                                {
                                    string viewName = oracleReader.GetString(0);
                                    ExecuteMSSQLQuery(sqlServerConnectionString, $"Insert into {oracle9iTableNameForViews} (ViewName) Values('{viewName}')");
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        // Handle any exceptions
                        Console.WriteLine("Error: " + ex.Message);
                    }
                    Console.WriteLine("Getting all the 'VIEW' from HSM86ADM Oracle 9i DB has been finished.\n");
                }
            }
            catch (Exception ex)
            {
                Debug.Print(ex.Message);
            }
        }

        static void GetFunctionsFromOracleAndInsertIntoMSQL(string oracleconnectionString, string sqlServerConnectionString)
        {
            try
            {
                Console.WriteLine("Going to get all the 'FUNCTION' from HSM86ADM Oracle 9i DB...");
                // SQL query to retrieve table names
                string sql = "SELECT object_name FROM all_objects where owner = 'HSM86ADM' and object_type = 'FUNCTION'";

                // Create a connection to the Oracle database
                using (OracleConnection oracleConnection = new OracleConnection(oracleconnectionString))
                {
                    try
                    {
                        // Open the connection
                        oracleConnection.Open();

                        // Create a command to execute the SQL query
                        using (OracleCommand oracleCommand = oracleConnection.CreateCommand())
                        {
                            oracleCommand.CommandText = sql;

                            // Execute the command and retrieve a data reader
                            using (OracleDataReader oracleReader = oracleCommand.ExecuteReader())
                            {
                                var oracle9iTableNameForFunctions = "Oracle9iFunctions";
                                ExecuteMSSQLQuery(sqlServerConnectionString, $"DROP TABLE {oracle9iTableNameForFunctions}");
                                if (!CheckIfTableExistsInMSSQL(sqlServerConnectionString, oracle9iTableNameForFunctions))
                                {
                                    ExecuteMSSQLQuery(sqlServerConnectionString, $"Create Table {oracle9iTableNameForFunctions} ([ID] [int] IDENTITY(1,1) NOT NULL,FunctionName [VARCHAR](255) NULL)");
                                }

                                DeleteDataFromSqlServer(sqlServerConnectionString, oracle9iTableNameForFunctions);

                                // Iterate through the result set and print table names
                                while (oracleReader.Read())
                                {
                                    string functionName = oracleReader.GetString(0);
                                    ExecuteMSSQLQuery(sqlServerConnectionString, $"Insert into {oracle9iTableNameForFunctions} (FunctionName) Values('{functionName}')");
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        // Handle any exceptions
                        Console.WriteLine("Error: " + ex.Message);
                    }
                    Console.WriteLine("Getting all the 'FUNCTION' from HSM86ADM Oracle 9i DB has been finished.\n");
                }
            }
            catch (Exception ex)
            {
                Debug.Print(ex.Message);
            }
        }

        static void GetTriggersFromOracleAndInsertIntoMSQL(string oracleconnectionString, string sqlServerConnectionString)
        {
            try
            {
                Console.WriteLine("Going to get all the 'TRIGGER' from HSM86ADM Oracle 9i DB...");
                // SQL query to retrieve table names
                string sql = "SELECT object_name FROM all_objects where owner = 'HSM86ADM' and object_type = 'TRIGGER'";

                // Create a connection to the Oracle database
                using (OracleConnection oracleConnection = new OracleConnection(oracleconnectionString))
                {
                    try
                    {
                        // Open the connection
                        oracleConnection.Open();

                        // Create a command to execute the SQL query
                        using (OracleCommand oracleCommand = oracleConnection.CreateCommand())
                        {
                            oracleCommand.CommandText = sql;

                            // Execute the command and retrieve a data reader
                            using (OracleDataReader oracleReader = oracleCommand.ExecuteReader())
                            {
                                var oracle9iTableNameForTriggers = "Oracle9iTriggers";
                                ExecuteMSSQLQuery(sqlServerConnectionString, $"DROP TABLE {oracle9iTableNameForTriggers}");
                                if (!CheckIfTableExistsInMSSQL(sqlServerConnectionString, oracle9iTableNameForTriggers))
                                {
                                    ExecuteMSSQLQuery(sqlServerConnectionString, $"Create Table {oracle9iTableNameForTriggers} ([ID] [int] IDENTITY(1,1) NOT NULL,TriggerName [VARCHAR](255) NULL)");
                                }

                                DeleteDataFromSqlServer(sqlServerConnectionString, oracle9iTableNameForTriggers);

                                // Iterate through the result set and print table names
                                while (oracleReader.Read())
                                {
                                    string triggerName = oracleReader.GetString(0);
                                    ExecuteMSSQLQuery(sqlServerConnectionString, $"Insert into {oracle9iTableNameForTriggers} (TriggerName) Values('{triggerName}')");
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        // Handle any exceptions
                        Console.WriteLine("Error: " + ex.Message);
                    }
                    Console.WriteLine("Getting all the 'TRIGGER' from HSM86ADM Oracle 9i DB has been finished.\n");
                }

            }
            catch (Exception ex)
            {
                Debug.Print(ex.Message);
            }
        }

        static void GetIndexesFromOracleAndInsertIntoMSQL(string oracleconnectionString, string sqlServerConnectionString)
        {
            try
            {
                Console.WriteLine("Going to get all the 'INDEX' from HSM86ADM Oracle 9i DB...");
                // SQL query to retrieve table names
                string sql = "SELECT object_name FROM all_objects where owner = 'HSM86ADM' and object_type = 'INDEX'";

                // Create a connection to the Oracle database
                using (OracleConnection oracleConnection = new OracleConnection(oracleconnectionString))
                {
                    try
                    {
                        // Open the connection
                        oracleConnection.Open();

                        // Create a command to execute the SQL query
                        using (OracleCommand oracleCommand = oracleConnection.CreateCommand())
                        {
                            oracleCommand.CommandText = sql;

                            // Execute the command and retrieve a data reader
                            using (OracleDataReader oracleReader = oracleCommand.ExecuteReader())
                            {
                                var oracle9iTableNameForIndexes = "Oracle9iIndexes";
                                ExecuteMSSQLQuery(sqlServerConnectionString, $"DROP TABLE {oracle9iTableNameForIndexes}");
                                if (!CheckIfTableExistsInMSSQL(sqlServerConnectionString, oracle9iTableNameForIndexes))
                                {
                                    ExecuteMSSQLQuery(sqlServerConnectionString, $"Create Table {oracle9iTableNameForIndexes} ([ID] [int] IDENTITY(1,1) NOT NULL,IndexName [VARCHAR](255) NULL)");
                                }

                                DeleteDataFromSqlServer(sqlServerConnectionString, oracle9iTableNameForIndexes);

                                // Iterate through the result set and print table names
                                while (oracleReader.Read())
                                {
                                    string indexName = oracleReader.GetString(0);
                                    ExecuteMSSQLQuery(sqlServerConnectionString, $"Insert into {oracle9iTableNameForIndexes} (IndexName) Values('{indexName}')");
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        // Handle any exceptions
                        Console.WriteLine("Error: " + ex.Message);
                    }
                    Console.WriteLine("Getting all the 'INDEX' from HSM86ADM Oracle 9i DB has been finished.\n");
                }
            }
            catch (Exception ex)
            {
                Debug.Print(ex.Message);
            }
        }

        static void GetPackagesFromOracleAndInsertIntoMSQL(string oracleconnectionString, string sqlServerConnectionString)
        {
            try
            {
                Console.WriteLine("Going to get all the 'PACKAGE' from HSM86ADM Oracle 9i DB...");
                // SQL query to retrieve table names
                string sql = "SELECT object_name FROM all_objects where owner = 'HSM86ADM' and object_type = 'PACKAGE'";

                // Create a connection to the Oracle database
                using (OracleConnection oracleConnection = new OracleConnection(oracleconnectionString))
                {
                    try
                    {
                        // Open the connection
                        oracleConnection.Open();

                        // Create a command to execute the SQL query
                        using (OracleCommand oracleCommand = oracleConnection.CreateCommand())
                        {
                            oracleCommand.CommandText = sql;

                            // Execute the command and retrieve a data reader
                            using (OracleDataReader oracleReader = oracleCommand.ExecuteReader())
                            {
                                var oracle9iTableNameForPackages = "Oracle9iPackages";
                                ExecuteMSSQLQuery(sqlServerConnectionString, $"DROP TABLE {oracle9iTableNameForPackages}");
                                if (!CheckIfTableExistsInMSSQL(sqlServerConnectionString, oracle9iTableNameForPackages))
                                {
                                    ExecuteMSSQLQuery(sqlServerConnectionString, $"Create Table {oracle9iTableNameForPackages} ([ID] [int] IDENTITY(1,1) NOT NULL,PackageName [VARCHAR](255) NULL)");
                                }

                                DeleteDataFromSqlServer(sqlServerConnectionString, oracle9iTableNameForPackages);

                                // Iterate through the result set and print table names
                                while (oracleReader.Read())
                                {
                                    string packageName = oracleReader.GetString(0);
                                    ExecuteMSSQLQuery(sqlServerConnectionString, $"Insert into {oracle9iTableNameForPackages} (PackageName) Values('{packageName}')");
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        // Handle any exceptions
                        Console.WriteLine("Error: " + ex.Message);
                    }
                    Console.WriteLine("Getting all the 'PACKAGE' from HSM86ADM Oracle 9i DB has been finished.\n");
                }
            }
            catch (Exception ex)
            {
                Debug.Print(ex.Message);
            }
        }

        static void GetTypesFromOracleAndInsertIntoMSQL(string oracleconnectionString, string sqlServerConnectionString)
        {
            try
            {
                Console.WriteLine("Going to get all the 'TYPE' from HSM86ADM Oracle 9i DB...");
                // SQL query to retrieve table names
                string sql = "SELECT object_name FROM all_objects where owner = 'HSM86ADM' and object_type = 'TYPE'";

                // Create a connection to the Oracle database
                using (OracleConnection oracleConnection = new OracleConnection(oracleconnectionString))
                {
                    try
                    {
                        // Open the connection
                        oracleConnection.Open();

                        // Create a command to execute the SQL query
                        using (OracleCommand oracleCommand = oracleConnection.CreateCommand())
                        {
                            oracleCommand.CommandText = sql;

                            // Execute the command and retrieve a data reader
                            using (OracleDataReader oracleReader = oracleCommand.ExecuteReader())
                            {
                                var oracle9iTableNameForTypes = "Oracle9iTypes";
                                ExecuteMSSQLQuery(sqlServerConnectionString, $"DROP TABLE {oracle9iTableNameForTypes}");
                                if (!CheckIfTableExistsInMSSQL(sqlServerConnectionString, oracle9iTableNameForTypes))
                                {
                                    ExecuteMSSQLQuery(sqlServerConnectionString, $"Create Table {oracle9iTableNameForTypes} ([ID] [int] IDENTITY(1,1) NOT NULL,TypeName [VARCHAR](255) NULL)");
                                }

                                DeleteDataFromSqlServer(sqlServerConnectionString, oracle9iTableNameForTypes);

                                // Iterate through the result set and print table names
                                while (oracleReader.Read())
                                {
                                    string typeName = oracleReader.GetString(0);
                                    ExecuteMSSQLQuery(sqlServerConnectionString, $"Insert into {oracle9iTableNameForTypes} (TypeName) Values('{typeName}')");
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        // Handle any exceptions
                        Console.WriteLine("Error: " + ex.Message);
                    }
                    Console.WriteLine("Getting all the 'TYPE' from HSM86ADM Oracle 9i DB has been finished.\n");
                }
            }
            catch (Exception ex)
            {
                Debug.Print(ex.Message);
            }
        }

        static void GetSynonymsFromOracleAndInsertIntoMSQL(string oracleconnectionString, string sqlServerConnectionString)
        {
            try
            {
                Console.WriteLine("Going to get all the 'SYNONYMS' from HSM86ADM Oracle 9i DB...");
                // SQL query to retrieve table names
                string sql = "SELECT synonym_name, table_owner, table_name FROM all_synonyms where owner = 'HSM86ADM'";

                // Create a connection to the Oracle database
                using (OracleConnection oracleConnection = new OracleConnection(oracleconnectionString))
                {
                    try
                    {
                        // Open the connection
                        oracleConnection.Open();

                        // Create a command to execute the SQL query
                        using (OracleCommand oracleCommand = oracleConnection.CreateCommand())
                        {
                            oracleCommand.CommandText = sql;

                            // Execute the command and retrieve a data reader
                            using (OracleDataReader oracleReader = oracleCommand.ExecuteReader())
                            {
                                var oracle9iTableNameForSynonyms = "Oracle9iSynonyms";
                                ExecuteMSSQLQuery(sqlServerConnectionString, $"DROP TABLE {oracle9iTableNameForSynonyms}");
                                if (!CheckIfTableExistsInMSSQL(sqlServerConnectionString, oracle9iTableNameForSynonyms))
                                {
                                    ExecuteMSSQLQuery(sqlServerConnectionString, $"Create Table {oracle9iTableNameForSynonyms} ([ID] [int] IDENTITY(1,1) NOT NULL,SynonymName [VARCHAR](255) NULL,TableOwner [VARCHAR](255) NULL,TableName [VARCHAR](255) NULL)");
                                }

                                DeleteDataFromSqlServer(sqlServerConnectionString, oracle9iTableNameForSynonyms);

                                // Iterate through the result set and print table names
                                while (oracleReader.Read())
                                {
                                    string synonymName = oracleReader.GetString(0);
                                    string tableOwner = oracleReader.GetString(1);
                                    string tableName = oracleReader.GetString(2);
                                    ExecuteMSSQLQuery(sqlServerConnectionString, $"Insert into {oracle9iTableNameForSynonyms} (SynonymName, TableOwner, TableName) Values('{synonymName}','{tableOwner}','{tableName}')");
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        // Handle any exceptions
                        Console.WriteLine("Error: " + ex.Message);
                    }
                    Console.WriteLine("Getting all the 'SYNONYMS' from HSM86ADM Oracle 9i DB has been finished.\n");
                }
            }
            catch (Exception ex)
            {
                Debug.Print(ex.Message);
            }
        }

        static void DeleteDataFromSqlServer(string connectionString, string tableName)
        {
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                connection.Open();

                string deleteQuery = $"DELETE FROM {tableName}";

                using (SqlCommand command = new SqlCommand(deleteQuery, connection))
                {
                    command.ExecuteNonQuery();
                }
            }
        }

        static void ExecuteMSSQLQuery(string connectionString, string insertQuery)
        {
            try
            {
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    connection.Open();
                    using (SqlCommand command = new SqlCommand(insertQuery, connection))
                    {
                        command.ExecuteNonQuery();
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        static void GetOracleTableDefinition(string oracleconnectionString, string tableName)
        {
            using (OracleConnection oracleConnection = new OracleConnection(oracleconnectionString))
            {
                try
                {
                    // Open the connection
                    oracleConnection.Open();

                    // Create a command to execute the SQL query
                    using (OracleCommand oracleCommand = oracleConnection.CreateCommand())
                    {
                        oracleCommand.CommandText = $"Select column_name, data_type, data_length, data_precision, nullable from user_tab_columns where table_name = '{tableName}'";

                        // Execute the command and retrieve a data reader
                        using (OracleDataReader oracleReader = oracleCommand.ExecuteReader())
                        {
                            while (oracleReader.Read())
                            {
                                string columnName = oracleReader.GetString(0);
                                string datatype = oracleReader.GetString(1);
                                string dataLength = oracleReader.GetInt32(2).ToString();
                                string dataPrecision = oracleReader.GetValue(3).ToString();
                                string nullable = oracleReader.GetValue(4).ToString();
                                Console.WriteLine($"Column Name :- {columnName}, " +
                                    $"Data Type :- {datatype}, " +
                                    $"Data Length :- {dataLength}," +
                                    $" Data Precision :- {dataPrecision}" +
                                    $" Nullable :- {nullable}");
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    // Handle any exceptions
                    Console.WriteLine("Error: " + ex.Message);
                }
            }
        }

        static bool CheckIfTableExistsInMSSQL(string connectionString, string tableName)
        {
            var result = false;
            try
            {
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    connection.Open();

                    string deleteQuery = $"SELECt COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{tableName}'";

                    using (SqlCommand command = new SqlCommand(deleteQuery, connection))
                    {
                        int count = Convert.ToInt32(command.ExecuteScalar());
                        if (count > 0)
                        {
                            result = true;
                        }
                    }
                }
            }
            catch (Exception ex) { Debug.Print(ex.Message); }
            return result;
        }

        static void GetDataFromOracleAndInsertIntoMSQL(string oracleconnectionString, string sqlServerConnectionString, string tableName, Dictionary<string, string> columnWiseDataType)
        {
            try
            {
                if (CheckIfTableExistsInMSSQL(sqlServerConnectionString, tableName))
                {
                    //GetData from Oracle
                    using (OracleConnection oracleConnection = new OracleConnection(oracleconnectionString))
                    {
                        try
                        {
                            // Open the connection
                            oracleConnection.Open();

                            // Create a command to execute the SQL query
                            using (OracleCommand oracleCommand = oracleConnection.CreateCommand())
                            {
                                oracleCommand.CommandText = $"Select * from {tableName}";

                                // Execute the command and retrieve a data reader
                                using (OracleDataReader oracleReader = oracleCommand.ExecuteReader())
                                {

                                    var numberOfRecordsInserted = 0;
                                    while (oracleReader.Read())
                                    {

                                        var columnNames = new string[oracleReader.FieldCount];
                                        var columnValues = new string[oracleReader.FieldCount];

                                        for (var index = 0; index < oracleReader.FieldCount; index++)
                                        {
                                            try
                                            {
                                                columnNames[index] = oracleReader.GetName(index);
                                                if (columnWiseDataType[columnNames[index]].StartsWith("numeric") &&
                                                   string.IsNullOrEmpty(oracleReader.GetValue(index)?.ToString()))
                                                {
                                                    columnValues[index] = $"TRY_CONVERT({columnWiseDataType[columnNames[index]]},'0')";
                                                }
                                                else if (columnWiseDataType[columnNames[index]].StartsWith("numeric"))
                                                {
                                                    columnValues[index] = $"COALESCE(TRY_CONVERT({columnWiseDataType[columnNames[index]]}," +
                                                        $"'{oracleReader.GetValue(index)}'), 0)";
                                                }
                                                else if (columnWiseDataType[columnNames[index]].StartsWith("varchar"))
                                                {
                                                    var varcharLength = columnWiseDataType[columnNames[index]].Substring(columnWiseDataType[columnNames[index]].IndexOf('(') + 1, columnWiseDataType[columnNames[index]].IndexOf(')') -
                                                        columnWiseDataType[columnNames[index]].IndexOf('(') - 1);
                                                    if (Convert.ToInt32(varcharLength) < oracleReader.GetValue(index)?.ToString().Length)
                                                    {
                                                        columnValues[index] = $"TRY_CONVERT({columnWiseDataType[columnNames[index]]},LEFT('{oracleReader.GetValue(index)}',{varcharLength}))";
                                                    }
                                                    else
                                                    {
                                                        columnValues[index] = $"TRY_CONVERT({columnWiseDataType[columnNames[index]]},'{oracleReader.GetValue(index)?.ToString().Replace("'", "''")}')";
                                                    }

                                                }
                                                else
                                                {
                                                    columnValues[index] = $"TRY_CONVERT({columnWiseDataType[columnNames[index]]},'{oracleReader.GetValue(index)}')";
                                                }
                                            }
                                            catch (Exception ex)
                                            {
                                                //To Manage Oracle Overflow Error
                                                columnNames[index] = oracleReader.GetName(index);
                                                columnValues[index] = $"TRY_CONVERT({columnWiseDataType[columnNames[index]]},'{oracleReader.GetOracleNumber(index)}')";
                                            }

                                        }
                                        //Generate MS-SQL Insert Query
                                        string msqlInsertQuery = $"INSERT INTO {tableName} ({string.Join(",", columnNames)}) VALUES" +
                                                $"({string.Join(",", columnValues)})";
                                        ExecuteMSSQLQuery(sqlServerConnectionString, msqlInsertQuery);
                                        numberOfRecordsInserted++;


                                    }
                                    Console.WriteLine($"Number of records inserted from oracle into MSSQL are {numberOfRecordsInserted}");
                                    Console.WriteLine(string.Empty);
                                }
                            }

                        }
                        catch (Exception ex)
                        {
                            // Handle any exceptions
                            Console.WriteLine("Error: " + ex.Message);
                        }

                    }
                }
                else
                {
                    Console.WriteLine($"Table with name {tableName} doesn't exists in MS-SQL.");
                }

            }
            catch (Exception ex) { Debug.Print(ex.Message); }
        }

        static void GetTablesFromOracleAndCreateIntoMSQL(string oracleconnectionString, string sqlServerConnectionString)
        {
            try
            {
                // SQL query to retrieve table names
                string sql = "SELECT table_name FROM all_tables where owner = 'HSM86ADM'";

                // Create a connection to the Oracle database
                using (OracleConnection oracleConnection = new OracleConnection(oracleconnectionString))
                {
                    try
                    {
                        // Open the connection
                        oracleConnection.Open();

                        // Create a command to execute the SQL query
                        using (OracleCommand oracleCommand = oracleConnection.CreateCommand())
                        {
                            oracleCommand.CommandText = sql;

                            // Execute the command and retrieve a data reader
                            using (OracleDataReader oracleReader = oracleCommand.ExecuteReader())
                            {
                                // Iterate through the result set and print table names
                                var numberOfTablesCreated = 0;
                                while (oracleReader.Read())
                                {
                                    string tableName = oracleReader.GetString(0);
                                    ExecuteMSSQLQuery(sqlServerConnectionString, $"DROP TABLE {tableName}");
                                    if (!CheckIfTableExistsInMSSQL(sqlServerConnectionString, tableName))
                                    {
                                        var (mssqlCreateTableQuery, columnWiseDataType) = GetMSSQLEquivalentTableCreationQueryFromOracle(oracleconnectionString, tableName);
                                        if (!string.IsNullOrEmpty(mssqlCreateTableQuery) &&
                                            columnWiseDataType.Any())
                                        {
                                            CreateDataIntoSqlServer(sqlServerConnectionString, mssqlCreateTableQuery);
                                            Console.WriteLine($"MSSQL Table has been created with name '{tableName}'");

                                            GetDataFromOracleAndInsertIntoMSQL(oracleconnectionString, sqlServerConnectionString, tableName, columnWiseDataType);
                                            Console.WriteLine($"Total number of MSSQL tables processed are '{numberOfTablesCreated}'");
                                            numberOfTablesCreated++;
                                        }
                                    }
                                }
                                Console.WriteLine($"Total number of MSSQL tables created are '{numberOfTablesCreated}'");
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        // Handle any exceptions
                        Console.WriteLine("Error: " + ex.Message);
                    }
                }
            }
            catch (Exception ex)
            {
                Debug.Print(ex.Message);
            }
        }

        static (string, Dictionary<string, string>) GetMSSQLEquivalentTableCreationQueryFromOracle(string oracleconnectionString, string tableName)
        {
            var result = string.Empty;
            var columnWiseDataType = new Dictionary<string, string>();
            try
            {
                using (OracleConnection oracleConnection = new OracleConnection(oracleconnectionString))
                {
                    try
                    {
                        // Open the connection
                        oracleConnection.Open();

                        // Create a command to execute the SQL query
                        using (OracleCommand oracleCommand = oracleConnection.CreateCommand())
                        {
                            oracleCommand.CommandText = $"Select column_name, data_type, data_length, data_precision, nullable from user_tab_columns where table_name = '{tableName}'";

                            // Execute the command and retrieve a data reader
                            using (OracleDataReader oracleReader = oracleCommand.ExecuteReader())
                            {
                                //Generate MSSQL Equivalent Table Creation Query
                                string createTableQuery = string.Empty;
                                createTableQuery = $"CREATE TABLE {tableName} (";

                                while (oracleReader.Read())
                                {
                                    string columnName = oracleReader.GetString(0);
                                    string datatype = oracleReader.GetString(1);
                                    string dataLength = oracleReader.GetInt32(2).ToString();
                                    string dataPrecision = oracleReader.GetValue(3).ToString();
                                    string nullable = oracleReader.GetValue(4).ToString();

                                    var mssqlEquivalentDataType = string.Empty;
                                    switch (datatype)
                                    {
                                        case "VARCHAR2":
                                            mssqlEquivalentDataType = $"varchar({dataLength})";
                                            columnWiseDataType.Add(columnName, mssqlEquivalentDataType);
                                            break;
                                        case "NUMBER":
                                            mssqlEquivalentDataType = string.IsNullOrEmpty(dataPrecision) ?
                                                $"numeric({dataLength}) DEFAULT 0" :
                                                $"numeric({dataLength},{dataPrecision}) DEFAULT 0";
                                            columnWiseDataType.Add(columnName, string.IsNullOrEmpty(dataPrecision) ? $"numeric({dataLength})" :
                                                $"numeric({dataLength},{dataPrecision})");
                                            break;
                                        case "DATE":
                                            mssqlEquivalentDataType = $"date";
                                            columnWiseDataType.Add(columnName, mssqlEquivalentDataType);
                                            break;
                                        case "TIMESTAMP(6)":
                                            mssqlEquivalentDataType = $"datetime";
                                            columnWiseDataType.Add(columnName, mssqlEquivalentDataType);
                                            break;
                                    }

                                    var mssqlEquivalentNullable = string.Empty;
                                    mssqlEquivalentNullable = nullable.Equals("N") ? "NULL" : "NOT NULL";
                                    createTableQuery += $"{columnName} {mssqlEquivalentDataType} {mssqlEquivalentNullable},";
                                }
                                createTableQuery = createTableQuery.TrimEnd(',', ' ');
                                createTableQuery += $");";

                                result = createTableQuery;
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        // Handle any exceptions
                        Console.WriteLine("Error: " + ex.Message);
                    }
                }
            }
            catch (Exception ex)
            {
                Debug.Print(ex.Message);
            }
            return (result, columnWiseDataType);
        }

        static void CreateDataIntoSqlServer(string connectionString, string createTableQuery)
        {
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                connection.Open();
                using (SqlCommand command = new SqlCommand(createTableQuery, connection))
                {
                    command.ExecuteNonQuery();
                }
            }
        }
    }
}
