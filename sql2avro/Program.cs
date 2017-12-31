using Avro;
using Avro.File;
using Avro.Generic;
using Microsoft.CSharp;
using Newtonsoft.Json;
using System;
using System.CodeDom;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Reflection;

namespace sql2avro
{
    public class Program
    {
        public static int Main(string[] args)
        {
            RecordSchema schema = null;
            IFileWriter<GenericRecord> writer = null;

            var options = args.ToDictionary(arg => arg.TrimStart('-').Split('=').FirstOrDefault(), arg => arg.Split('=').LastOrDefault().Trim(new[] { '\'', '"' }));

            if (string.IsNullOrEmpty(options.GetOrDefault("output")))
            {
                PrintHelpMessage();
                return 1;
            }

            var builder = new SqlConnectionStringBuilder
            {
                DataSource = options.GetOrDefault("server", "localhost"),
                InitialCatalog = options.GetOrDefault("database", "RabotaUA2")
            };

            if (!string.IsNullOrEmpty(options.GetOrDefault("password")))
            {
                builder.UserID = options.GetOrDefault("username", "sa");
                builder.Password = options.GetOrDefault("password", "");
            }
            else
            {
                builder.IntegratedSecurity = true;
            }

            var query = options.GetOrDefault("query", null) ?? File.ReadAllText(options.GetOrDefault("input"));
            var provider = new CSharpCodeProvider();
            var command = new SqlCommand(query, new SqlConnection(builder.ConnectionString)) { CommandTimeout = 0 };
            command.Connection.Open();
            var reader = command.ExecuteReader();
            if (reader.HasRows)
            {
                while (reader.Read())
                {
                    if (schema == null)
                    {
                        schema = Schema.Parse(JsonConvert.SerializeObject(new
                        {
                            type = "record",
                            name = "row",
                            fields = Enumerable.Range(0, reader.FieldCount).Select(index => new
                            {
                                name = reader.GetName(index),
                                type = new[] {
                                    provider.GetTypeOutput(new CodeTypeReference(reader.GetFieldType(index))),
                                    "null"
                                }
                            })
                        })) as RecordSchema;

                        writer = DataFileWriter<GenericRecord>.OpenWriter(new GenericDatumWriter<GenericRecord>(schema), options.GetOrDefault("output"), Codec.CreateCodec(Codec.Type.Deflate));
                    }

                    var r = new GenericRecord(schema);
                    for (var i = 0; i < reader.FieldCount; i++)
                    {
                        r.Add(reader.GetName(i), reader.IsDBNull(i) ? null : reader[i]);
                    }
                    writer.Append(r);
                }
                writer?.Close();
                return 0;
            }
            return 1;
        }

        private static void PrintHelpMessage() => Console.WriteLine($@"
SQL2AVRO Version: {Assembly.GetExecutingAssembly().GetName().Version}

Required arguments:

  --query=""select top 10 * from city"" - required if there is no input argument
  --input=query.sql - required if there is no query argument
  --output=city.avro

Optional arguments:

  --server=localhost
  --database=RabotaUA2
  --username=sa
  --password=password

Usage examples:

  sql2avro --query=""select top 10 * from city"" --output=city.avro
");
    }

    public static class DictionaryExtensions
    {
        public static TValue GetOrDefault<TKey, TValue>(this Dictionary<TKey, TValue> dict, TKey key, TValue val = default(TValue)) => dict.ContainsKey(key) && dict[key] != null ? dict[key] : val;
    }
}
