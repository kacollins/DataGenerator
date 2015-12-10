using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

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
            List<Column> columns = GetColumnsForInsert(table);

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

        private static List<Column> GetColumnsForInsert(Table table)
        {
            string queryText = $@"SELECT c.name, c.system_type_id, c.max_length, c.precision, c.scale, c.is_nullable{
                Environment.NewLine}FROM sys.columns c{
                Environment.NewLine}INNER JOIN sys.tables t{
                Environment.NewLine}ON c.object_id = t.object_id{
                Environment.NewLine}INNER JOIN sys.schemas s{
                Environment.NewLine}ON t.schema_id = s.schema_id{
                Environment.NewLine}WHERE s.name = '{table.SchemaName}'{
                Environment.NewLine}AND t.name = '{table.TableName}'{
                Environment.NewLine}AND is_identity = 0{
                Environment.NewLine}AND is_computed = 0";

            DataTable dt = GetDataTable(queryText);
            List<Column> columns = dt.Rows.Cast<DataRow>().Select(dr => new Column(dr)).ToList();

            return columns;
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
                int randomNumber = rand.Next(0, column.IsNullable ? 3 : 2);
                output = randomNumber == 2 ? "NULL" : randomNumber.ToString();
                needQuotes = false;
            }
            else if (column.SystemTypeID == (int)DataType.Integer)
            {
                int randomNumber = rand.Next(column.IsNullable ? 0 : 1, 10);
                output = randomNumber == 0 ? "NULL" : randomNumber.ToString();
                needQuotes = false;
            }
            else if (column.SystemTypeID == (int)DataType.Numeric || column.SystemTypeID == (int)DataType.Dec)
            {
                decimal randomNumber = rand.Next(0, (int)Math.Pow(10, column.Precision)) / (decimal)Math.Pow(10, column.Scale);
                output = randomNumber.ToString();
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
            else if (column.SystemTypeID == (int)DataType.Varchar || column.SystemTypeID == (int)DataType.Nvarchar || column.SystemTypeID == (int)DataType.Character)
            {
                output = GetStringByColumnName(column, rand);
            }
            else if (column.SystemTypeID == (int)DataType.Uniqueidentifier)
            {
                output = "NEWID()";
                needQuotes = false;
            }

            if (needQuotes)
            {
                output = $"'{output}'";
            }

            return output;
        }

        private static string GetStringByColumnName(Column column, Random rand)
        {
            string output;
            string columnName = column.ColumnName.ToLower();

            if (columnName == "usercreated" || columnName == "usermodified")
            {
                output = "admin";
            }
            else if (columnName.Contains("firstname"))
            {
                output = GetRandom(FirstNames, rand);
            }
            else if (columnName.Contains("lastname"))
            {
                output = GetRandom(LastNames, rand);
            }
            else if (columnName.Contains("address1"))
            {
                output = $"{rand.Next(10, 9999)} {GetRandom(Streets, rand)} St.";
            }
            else if (columnName.Contains("address2"))
            {
                int randomNumber = rand.Next(0, 3);
                output = randomNumber == 0 ? "" : $"Suite {randomNumber}";
            }
            else if (columnName.EndsWith("city"))
            {
                output = GetRandom(Cities, rand);
            }
            else if (columnName.EndsWith("state") && column.MaxLength <= 5)
            {
                output = GetRandom(States, rand);
            }
            else if (columnName.Contains("zipcode"))
            {
                output = rand.Next(501, 99950).ToString("00000");
            }
            else if (columnName.Contains("zip4"))
            {
                output = rand.Next(1, 9999).ToString("0000");
            }
            else if (columnName.EndsWith("phone"))
            {
                output = $"({rand.Next(100, 999)}) {rand.Next(100, 999)}-{rand.Next(1000, 9999)}";

                if (column.MaxLength < output.Length)
                {
                    output = GetDigits(output);
                }
            }
            else if (columnName.Contains("phoneext"))
            {
                int randomNumber = rand.Next(0, 3);
                output = randomNumber == 0 ? "" : $"Ext. {randomNumber}";
            }
            else if (columnName.Contains("email"))
            {
                output = $"{GetRandom(LoremIpsumWords, rand)}_{GetRandom(LoremIpsumWords, rand)}_{rand.Next(1, 999)}@email.email";
            }
            else
            {
                output = GetLoremIpsumText(column, rand);
            }

            return output;
        }

        private static string GetLoremIpsumText(Column column, Random rand)
        {
            StringBuilder sb = new StringBuilder(GetRandom(LoremIpsumWords, rand));

            int wordCounter = 1;
            int maxNumberOfWords = (int)(column.MaxLength / LoremIpsumWords.Average(w => w.Length)) + 1;
            int numberOfWordsToUse = rand.Next(1, maxNumberOfWords + 1);

            //TODO: Add punctuation and capitalization

            while (sb.Length + LoremIpsumWords.Max(w => w.Length) < column.MaxLength && wordCounter < numberOfWordsToUse)
            {
                sb.Append(' ');
                sb.Append(GetRandom(LoremIpsumWords, rand));
                wordCounter++;
            }

            string output = sb.ToString();

            return output;
        }

        private static string GetDigits(string input)
        {
            Regex regex = new Regex(@"[^\d]");
            return regex.Replace(input, "");
        }

        private static string GetRandom(List<string> strings, Random rand)
        {
            return strings[rand.Next(0, strings.Count)];
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

        private static List<string> Cities => new List<string>
        {
            "Oklahoma City",
            "Tulsa",
            "Norman",
            "Broken Arrow",
            "Lawton",
            "Edmond",
            "Moore",
            "Midwest City",
            "Enid",
            "Stillwater"
        };

        private static List<string> States => new List<string>
        {
            "AL",
            "AK",
            "AZ",
            "AR",
            "CA",
            "CO",
            "CT",
            "DE",
            "FL",
            "GA",
            "HI",
            "ID",
            "IL",
            "IN",
            "IA",
            "KS",
            "KY",
            "LA",
            "ME",
            "MD",
            "MA",
            "MI",
            "MN",
            "MS",
            "MO",
            "MT",
            "NE",
            "NV",
            "NH",
            "NJ",
            "NM",
            "NY",
            "NC",
            "ND",
            "OH",
            "OK",
            "OR",
            "PA",
            "RI",
            "SC",
            "SD",
            "TN",
            "TX",
            "UT",
            "VT",
            "VA",
            "WA",
            "WV",
            "WI",
            "WY"
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
            public bool IsNullable { get; }

            public Column(DataRow dr)
            {
                ColumnName = dr["name"].ToString();
                SystemTypeID = int.Parse(dr["system_type_id"].ToString());
                MaxLength = int.Parse(dr["max_length"].ToString());
                Precision = int.Parse(dr["precision"].ToString());
                Scale = int.Parse(dr["scale"].ToString());
                IsNullable = bool.Parse(dr["is_nullable"].ToString());
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
            Uniqueidentifier = 36,
            Date = 40,
            Integer = 56,
            Datetime = 61,
            Bit = 104,
            Dec = 106,
            Numeric = 108,
            Varchar = 167,
            Character = 175,
            Nvarchar = 231
        }

        #endregion

    }
}
