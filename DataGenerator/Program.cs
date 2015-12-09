using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;

namespace DataGenerator
{
    class Program
    {
        static void Main(string[] args)
        {
            TableFileResult result = GetTablesToPopulate();

            foreach (Table table in result.Tables)
            {
                GenerateInsertScripts(table);
            }

            if (result.Errors.Any())
            {
                HandleTopLevelError(AppendLines(result.Errors), false);
            }
            else if (!result.Tables.Any())
            {
                HandleTopLevelError("No tables to compare!");
            }

            //TODO: Add silent mode
            //Console.WriteLine("Press enter to exit:");
            //Console.Read();
        }

        #region Methods

        private static void GenerateInsertScripts(Table table)
        {
            string queryText = "SELECT c.name, c.system_type_id, c.max_length, c.precision, c.scale, c.is_nullable" +
                               " FROM sys.columns c" +
                               " INNER JOIN sys.tables t" +
                               " ON c.object_id = t.object_id" +
                               " INNER JOIN sys.schemas s" +
                               " ON t.schema_id = s.schema_id" +
                               $" WHERE s.name = '{table.SchemaName}'" +
                               $" AND t.name = '{table.TableName}'" +
                               " AND is_identity = 0" +
                               " AND is_computed = 0";

            DataTable dt = GetDataTable(queryText);
            List<Column> columns = dt.Rows.Cast<DataRow>().Select(dr => new Column(dr)).ToList();

            List<string> fileLines = new List<string>();

            Random rand = new Random();
            for (int i = 0; i < 10; i++)
            {
                fileLines.AddRange(GetInsertScript(columns, table, rand));
            }

            string fileName = $"{table.SchemaName}_{table.TableName}";
            string fileContents = AppendLines(fileLines);

            WriteToFile(fileName, fileContents);
        }

        private static List<string> GetInsertScript(List<Column> columns, Table table, Random rand)
        {
            List<string> scriptLines = new List<string>();

            if (columns.Count > 1)
            {
                scriptLines.Add($"INSERT INTO {table.SchemaName}.{table.TableName}");
                scriptLines.Add("(");
                scriptLines.Add($"{Tab}{columns.First().ColumnName}");
                scriptLines.AddRange(columns.Skip(1).Select(column => $"{Tab}, {column.ColumnName}"));
                scriptLines.Add(")");
                scriptLines.Add("SELECT");

                scriptLines.Add($"{Tab}{columns[0].ColumnName} = {GetColumnValue(columns[1], rand)}");
                scriptLines.AddRange(columns.Skip(1).Select(column => $"{Tab}, {column.ColumnName} = {GetColumnValue(column, rand)}"));
                scriptLines.Add("");
            }

            return scriptLines;
        }

        private static string GetColumnValue(Column column, Random rand)
        {
            string output = "TODO";
            bool needQuotes = true;

            if (column.SystemTypeID == (int)DataType.Bit)
            {
                output = rand.Next(0, 2).ToString();
                needQuotes = false;
            }
            else if (column.SystemTypeID == (int)DataType.Integer)
            {
                output = rand.Next(1, 10).ToString();
                needQuotes = false;
            }
            else if (column.SystemTypeID == (int)DataType.Date || column.SystemTypeID == (int)DataType.Datetime)
            {
                int minuteOffset = 0;

                if (column.ColumnName != "DateCreated" && column.ColumnName != "DateModified")
                {
                    minuteOffset = rand.Next(0, 100000);
                }

                output = DateTime.Now.AddMinutes(-minuteOffset).ToString();
            }
            else if (column.SystemTypeID == (int)DataType.Varchar || column.SystemTypeID == (int)DataType.Character)
            {
                output = GetStringByColumnName(column.ColumnName, rand);
            }

            if (needQuotes)
            {
                output = $"'{output}'";
            }

            return output;
        }

        private static string GetStringByColumnName(string columnName, Random rand)
        {
            string output;

            columnName = columnName.ToLower();

            if (columnName == "usercreated" || columnName == "usermodified")
            {
                output = "admin";
            }
            else if (columnName.Contains("firstname"))
            {
                output = FirstNames[rand.Next(0, FirstNames.Count)];
            }
            else if (columnName.Contains("lastname"))
            {
                output = LastNames[rand.Next(0, LastNames.Count)];
            }
            else if (columnName.Contains("address1"))
            {
                output = $"{rand.Next(100, 999)} {Streets[rand.Next(0, Streets.Count)]} St.";
            }
            else if (columnName.Contains("address2"))
            {
                int randomNumber = rand.Next(0, 3);
                output = randomNumber == 0 ? "" : $"Suite {randomNumber}";
            }
            else if (columnName.EndsWith("city"))
            {
                output = "Oklahoma City";
            }
            else if (columnName.EndsWith("state"))
            {
                output = "OK";
            }
            else if (columnName.Contains("zipcode"))
            {
                output = rand.Next(73100, 73199).ToString();
            }
            else if (columnName.Contains("zip4"))
            {
                output = rand.Next(1000, 9999).ToString();
            }
            else if (columnName.EndsWith("phone"))
            {
                //TODO: Add symbols if the column is long enough
                //output = $"(405) {rand.Next(100, 999)}-{rand.Next(1000, 9999)}";
                output = $"405{rand.Next(1000000, 9999999)}";
            }
            else if (columnName.Contains("phoneext"))
            {
                output = "";
            }
            else if (columnName.Contains("email"))
            {
                output = $"{LoremIpsumWords[rand.Next(0, LoremIpsumWords.Count)]}{rand.Next(100, 999)}@email.email";
            }
            else
            {
                output = LoremIpsumWords[rand.Next(0, LoremIpsumWords.Count)];
            }

            return output;
        }

        private static TableFileResult GetTablesToPopulate()
        {
            const string fileName = "TablesToPopulate.supersecret";

            List<string> lines = GetFileLines(fileName);
            const char period = '.';

            List<Table> tables = lines.Select(line => line.Split(period))
                                                .Where(parts => parts.Length == Enum.GetValues(typeof(TablePart)).Length)
                                                .Select(parts => new Table(parts[(int)TablePart.SchemaName],
                                                                        parts[(int)TablePart.TableName]))
                                                .ToList();

            List<string> errorMessages = GetFileErrors(lines, period, Enum.GetValues(typeof(TablePart)).Length, "schema/table format");

            if (errorMessages.Any())
            {
                //TODO: Write error messages to file
                Console.WriteLine("Error: Invalid schema/table format in TablesToPopulate file.");
            }

            TableFileResult result = new TableFileResult(tables, errorMessages);

            return result;
        }

        private static List<string> GetFileLines(string fileName)
        {
            List<string> fileLines = new List<string>();

            const char backSlash = '\\';
            DirectoryInfo directoryInfo = new DirectoryInfo($"{CurrentDirectory}{backSlash}{Folder.Inputs}");

            if (directoryInfo.Exists)
            {
                FileInfo file = directoryInfo.GetFiles(fileName).FirstOrDefault();

                if (file == null)
                {
                    Console.WriteLine($"File does not exist: {directoryInfo.FullName}{backSlash}{fileName}");
                }
                else
                {
                    fileLines = File.ReadAllLines(file.FullName)
                                            .Where(line => !string.IsNullOrWhiteSpace(line)
                                                            && !line.StartsWith("--")
                                                            && !line.StartsWith("//")
                                                            && !line.StartsWith("'"))
                                            .ToList();
                }
            }
            else
            {
                Console.WriteLine($"Directory does not exist: {directoryInfo.FullName}");
            }

            return fileLines;
        }

        private static List<string> GetFileErrors(List<string> fileLines, char separator, int length, string description)
        {
            List<string> errorMessages = fileLines.Where(line => line.Split(separator).Length != length)
                                                    .Select(invalidLine => $"Invalid {description}: {invalidLine}")
                                                    .ToList();

            return errorMessages;
        }

        private static string AppendLines(IEnumerable<string> input)
        {
            return input.Aggregate(new StringBuilder(), (current, next) => current.AppendLine(next)).ToString();
        }

        private static void HandleTopLevelError(string errorMessage, bool writeToConsole = true)
        {
            if (writeToConsole)
            {
                Console.WriteLine(errorMessage);
            }

            WriteToFile("Error", errorMessage);
        }

        private static void WriteToFile(string fileName, string fileContents)
        {
            const char backSlash = '\\';
            string directory = $"{CurrentDirectory}{backSlash}{Folder.Outputs}";

            //TODO: Create subdirectory for each schema

            if (!Directory.Exists(directory))
            {
                Directory.CreateDirectory(directory);
            }

            string filePath = $"{directory}{backSlash}{fileName}.sql";

            if (File.Exists(filePath))
            {
                File.Delete(filePath);
            }

            using (StreamWriter sw = File.CreateText(filePath))
            {
                sw.Write(fileContents);
            }

            Console.WriteLine($"Wrote file to {filePath}");
        }

        private static DataTable GetDataTable(string queryText)
        {
            SqlConnection conn = new SqlConnection
            {
                ConnectionString = ConfigurationManager.ConnectionStrings["SQLDBConnection"].ToString()
            };

            SqlDataAdapter sda = new SqlDataAdapter(queryText, conn);
            DataTable dt = new DataTable();

            try
            {
                conn.Open();
                sda.Fill(dt);
            }
            catch (Exception ex)
            {
                //TODO: Write error messages to file
                Console.WriteLine(ex.Message);
            }
            finally
            {
                conn.Close();
            }

            return dt;
        }

        #endregion

        #region Properties

        private static string CurrentDirectory => Directory.GetCurrentDirectory();  //bin\Debug

        private static string Tab => new string(' ', 4);

        private static List<string> FirstNames => new List<string>
        {
            "James",
            "John",
            "Robert",
            "Michael",
            "William",
            "Mary",
            "Patricia",
            "Linda",
            "Barbara",
            "Elizabeth"
        };

        private static List<string> LastNames => new List<string>
        {
            "Smith",
            "Johnson",
            "Williams",
            "Jones",
            "Brown",
            "Davis",
            "Miller",
            "Wilson",
            "Moore",
            "Taylor"
        };

        private static List<string> Streets => new List<string>
        {
            "First",
            "Second",
            "Third",
            "Fourth",
            "Fifth",
            "Park",
            "Main",
            "Oak",
            "Pine",
            "Maple"
        };

        private static List<string> LoremIpsumWords => new List<string>
        {
            "adipiscing",
            "aliqua",
            "aliquip",
            "amet",
            "anim",
            "aute",
            "cillum",
            "commodo",
            "consectetur",
            "consequat",
            "culpa",
            "cupidatat",
            "deserunt",
            "dolor",
            "dolore",
            "duis",
            "eiusmod",
            "elit",
            "enim",
            "esse",
            "est",
            "excepteur",
            "exercitation",
            "fugiat",
            "incididunt",
            "ipsum",
            "irure",
            "labore",
            "laboris",
            "laborum",
            "lorem",
            "magna",
            "minim",
            "mollit",
            "nisi",
            "non",
            "nostrud",
            "nulla",
            "occaecat",
            "officia",
            "pariatur",
            "proident",
            "qui",
            "quis",
            "reprehenderit",
            "sed",
            "sint",
            "sit",
            "sunt",
            "tempor",
            "ullamco",
            "velit",
            "veniam",
            "voluptate"
        };

        #endregion

        #region Classes

        private class Table
        {
            public string SchemaName { get; }
            public string TableName { get; }

            public Table(string schemaName, string tableName)
            {
                SchemaName = schemaName.Trim();
                TableName = tableName.Trim();
            }
        }

        private class TableFileResult
        {
            public List<Table> Tables { get; }
            public List<string> Errors { get; }

            public TableFileResult(List<Table> tables, List<string> errors)
            {
                Tables = tables;
                Errors = errors;
            }
        }

        private class Column
        {
            public string ColumnName { get; }
            public int SystemTypeID { get; }
            public int MaxLength { get; }
            public int Precision { get; }
            public int Scale { get; }
            public bool Nullable { get; }

            public Column(DataRow dr)
            {
                ColumnName = dr["name"].ToString();
                SystemTypeID = int.Parse(dr["system_type_id"].ToString());
                MaxLength = int.Parse(dr["max_length"].ToString());
                Precision = int.Parse(dr["precision"].ToString());
                Scale = int.Parse(dr["scale"].ToString());
                Nullable = bool.Parse(dr["is_nullable"].ToString());
            }
        }

        #endregion

        #region Enums

        private enum TablePart
        {
            SchemaName,
            TableName
        }

        private enum Folder
        {
            Inputs,
            Outputs
        }

        private enum DataType
        {
            Image = 34,
            Uniqueidentifier = 36,
            Date = 40,
            Datetimeoffset = 43,
            Tinyint = 48,
            Smallint = 52,
            Integer = 56,
            Datetime = 61,
            Floating = 62,
            Sql_variant = 98,
            Bit = 104,
            Dec = 106,
            Numeric = 108,
            Bigint = 127,
            Varbinary = 165,
            Varchar = 167,
            Binary = 173,
            Character = 175,
            Nvarchar = 231,
            Sysname = 231,
            Nchar = 239,
            Xml = 241
        }

        #endregion

    }
}
