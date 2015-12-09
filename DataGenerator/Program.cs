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
            string queryText = $"SELECT TOP 1 * FROM {table.SchemaName}.{table.TableName}";
            DataTable dt = GetDataTable(queryText);
            List<DataColumn> columns = dt.Columns.Cast<DataColumn>().ToList();

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

        private static List<string> GetInsertScript(List<DataColumn> columns, Table table, Random rand)
        {
            List<string> scriptLines = new List<string>();

            if (columns.Count > 1)
            {
                scriptLines.Add($"INSERT INTO {table.SchemaName}.{table.TableName}");
                scriptLines.Add("(");
                scriptLines.Add($"{Tab}{columns[1].ColumnName}");
                scriptLines.AddRange(columns.Skip(2).Select(column => $"{Tab}, {column.ColumnName}"));
                scriptLines.Add(")");
                scriptLines.Add("SELECT");

                scriptLines.Add($"{Tab}{columns[1].ColumnName} = {GetColumnValue(columns[1], rand)}");
                scriptLines.AddRange(columns.Skip(2).Select(column => $"{Tab}, {column.ColumnName} = {GetColumnValue(column, rand)}"));
                scriptLines.Add("");
            }

            return scriptLines;
        }

        private static string GetColumnValue(DataColumn column, Random rand)
        {
            string output = "TODO";
            bool needQuotes = true;

            if (column.DataType == typeof(bool))
            {
                output = rand.Next(0, 2).ToString();
                needQuotes = false;
            }
            else if (column.DataType == typeof(byte) || column.DataType == typeof(short) || column.DataType == typeof(int))
            {
                output = rand.Next(1, 10).ToString();
                needQuotes = false;
            }
            else if (column.DataType == typeof(DateTime))
            {
                int minuteOffset = 0;

                if (column.ColumnName != "DateCreated" && column.ColumnName != "DateModified")
                {
                    minuteOffset = rand.Next(0, 100000);
                }

                output = DateTime.Now.AddMinutes(-minuteOffset).ToString();
            }
            else if (column.DataType == typeof(string))
            {
                output = GetStringByColumnName(column, rand);
            }

            if (needQuotes)
            {
                output = $"'{output}'";
            }

            return output;
        }

        private static string GetStringByColumnName(DataColumn column, Random rand)
        {
            string output;

            if (column.ColumnName == "UserCreated" || column.ColumnName == "UserModified")
            {
                output = "admin";
            }
            else if (column.ColumnName.ToLower().Contains("firstname"))
            {
                output = FirstNames[rand.Next(0, FirstNames.Count)];
            }
            else if (column.ColumnName.ToLower().Contains("lastname"))
            {
                output = LastNames[rand.Next(0, LastNames.Count)];
            }
            else if (column.ColumnName.ToLower().Contains("address1"))
            {
                output = $"{rand.Next(100, 999)} {Streets[rand.Next(0, Streets.Count)]} St.";
            }
            else if (column.ColumnName.ToLower().Contains("address2"))
            {
                int randomNumber = rand.Next(0, 3);
                output = randomNumber == 0 ? "" : $"Suite {randomNumber}";
            }
            else if (column.ColumnName.ToLower().EndsWith("city"))
            {
                output = "Oklahoma City";
            }
            else if (column.ColumnName.ToLower().EndsWith("state"))
            {
                output = "OK";
            }
            else if (column.ColumnName.ToLower().Contains("zipcode"))
            {
                output = rand.Next(73100, 73199).ToString();
            }
            else if (column.ColumnName.ToLower().Contains("zip4"))
            {
                output = rand.Next(1000, 9999).ToString();
            }
            else if (column.ColumnName.ToLower().EndsWith("phone"))
            {
                output = $"(405) {rand.Next(100, 999)}-{rand.Next(1000, 9999)}";
            }
            else if (column.ColumnName.ToLower().Contains("phoneext"))
            {
                output = "";
            }
            else if (column.ColumnName.ToLower().Contains("email"))
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

        #endregion

    }
}
