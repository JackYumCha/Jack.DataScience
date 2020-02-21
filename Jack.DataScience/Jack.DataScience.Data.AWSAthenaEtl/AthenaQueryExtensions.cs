using System;
using System.IO;
using System.Linq;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Jack.DataScience.Data.AWSAthena;
using Amazon.Athena.Model;
using Jack.DataScience.Storage.AWSS3;
using System.Text.RegularExpressions;
using Newtonsoft.Json;
using System.Threading;
using Amazon.S3.Model;

namespace Jack.DataScience.Data.AWSAthenaEtl
{
    public static class AthenaQueryExtensions
    {
        private static Regex rgxDateOffset = new Regex(@"\{date\(([+-]?\d+)\)\}");

        public static async Task GetAthenaQueryResultSampleByDate(this EtlSettings etlSettings, int lines)
        {
            var athena = etlSettings.AthenaQuerySource;
            if (athena == null) throw new Exception("The ETL has an empty Athena source setting.");
            var athenaApi = etlSettings.CreateSourceAthenaAPI();

            var query = athena.AthenaSQL;
            var today = DateTime.Now;
            var date = today.AddDays(-athena.DaysAgo);
            query = query.Replace("{date}", date.ToString(athena.DateFormat));
            query = rgxDateOffset.Replace(query, m =>
            {
                var offset = int.Parse(m.Groups[1].Value);
                return date.AddDays(offset).ToString(athena.DateFormat);
            });
            query += $"\nlimit {lines}";
            var getResultRequest = await athenaApi.ExecuteQuery(query);
            var response = await athenaApi.ReadOneResult(getResultRequest);
            etlSettings.Mappings = response.ToFieldMapping();
            // load data schema to the etlsetting schema

            var sample = new DataSample()
            {
                Rows = new List<DataRow>()
            };

            var data = response.ReadData();
            foreach(var row in data)
            {
                var dataRow = new DataRow()
                {
                    Items = row.Select(item => item.ToString()).ToList()
                };
                sample.Rows.Add(dataRow);
            }
            etlSettings.Sample = sample;
        }

        /// <summary>
        /// execute athena query and return sample data
        /// </summary>
        /// <param name="athenaApi"></param>
        /// <param name="sql"></param>
        /// <param name="lines"></param>
        /// <returns></returns>
        public static async Task<DataSampleWithSchema> GetSampleDataBySQL(this AWSAthenaAPI athenaApi, string sql)
        {
            var result = new DataSampleWithSchema()
            {
                FieldMappings = new List<FieldMapping>(),
            };
            var sample = new DataSample()
            {
                Rows = new List<DataRow>()
            };
            result.DataSample = sample;

            // var response = await athenaApi.ExecuteQuery(sql);
            var getResultRequest = await athenaApi.ExecuteQuery(sql);
            var response = await athenaApi.ReadOneResult(getResultRequest);
            var data = response.ReadData();
            result.FieldMappings = response.ToFieldMapping();

            foreach (var row in data)
            {
                var dataRow = new DataRow()
                {
                    Items = row.Select(item => item.ToString()).ToList()
                };
                sample.Rows.Add(dataRow);
            }
            return result;
        }

        public static async Task<string> StartSampleDataBySQL(this AWSAthenaAPI athenaApi, string sql)
        {
            var result = new DataSampleWithSchema()
            {
                FieldMappings = new List<FieldMapping>(),
            };
            var sample = new DataSample()
            {
                Rows = new List<DataRow>()
            };
            result.DataSample = sample;

            // var response = await athenaApi.ExecuteQuery(sql);
            return await athenaApi.StartQuery(sql);
        }

        public static async Task<DataSampleWithSchema> TryObtainSampleDataResult(this AWSAthenaAPI athenaApi, string executionId)
        {
            if(await athenaApi.IsExecutionCompleted(executionId))
            {
                var result = new DataSampleWithSchema()
                {
                    FieldMappings = new List<FieldMapping>(),
                };
                var sample = new DataSample()
                {
                    Rows = new List<DataRow>()
                };
                result.DataSample = sample;
                var response = await athenaApi.ReadOneResult(new GetQueryResultsRequest() { QueryExecutionId = executionId });
                var data = response.ReadData();
                result.FieldMappings = response.ToFieldMapping();

                foreach (var row in data)
                {
                    var dataRow = new DataRow()
                    {
                        Items = row.Select(item => item.ToString()).ToList()
                    };
                    sample.Rows.Add(dataRow);
                }
                return result;
            }
            else
            {
                return null;
            }
        }

        

        public static async Task<List<string>> TransferAthenaQueryResultByDate(this EtlSettings etlSettings, AWSAthenaAPI awsAthenaAPI, DateTime? useDate = null)
        {
            var result = new List<string>();
            var athena = etlSettings.AthenaQuerySource;
            if (athena == null) throw new Exception("The ETL has an empty Athena source setting.");
            var athenaApi = etlSettings.CreateSourceAthenaAPI();

            var query = athena.AthenaSQL;
            var today = DateTime.UtcNow;
            if (useDate.HasValue) today = useDate.Value;
            var date = today.AddDays(-athena.DaysAgo);
            query = query.Replace("{date}", date.ToString(athena.DateFormat));
            query = rgxDateOffset.Replace(query, m =>
            {
                var offset = int.Parse(m.Groups[1].Value);
                return date.AddDays(offset).ToString(athena.DateFormat);
            });
            var dateKey = date.ToString("yyyyMMdd");

            // var response = await athenaApi.ExecuteQuery(query);

            var getResultRequest = await athenaApi.ExecuteQuery(query);
            //var response = await athenaApi.ReadOneResult(getResultRequest);

            //var enumerator = response.ResultSet.Rows.GetEnumerator();
            ResultSetMetadata resultSetMetadata = null;

            var enumerator = athenaApi.EnumerateRows(getResultRequest, res=> resultSetMetadata = res.ResultSet.ResultSetMetadata).GetEnumerator();

            List<Row> rows = new List<Row>();

            int parquetIndex = 0;

            var targetS3 = etlSettings.CreateTargetS3API();

            //skip first row;
            enumerator.MoveNext();
            while (enumerator.MoveNext())
            {
                rows.Add(enumerator.Current);
                if (rows.Count >= etlSettings.NumberOfItemsPerParquet)
                {
                    var s3key = etlSettings.MakeTargetS3Key(dateKey, "", false, parquetIndex);
                    await targetS3.WriteResultRowsToS3Bucket(rows, resultSetMetadata, etlSettings, s3key);
                    result.Add($"s3://{etlSettings.TargetS3BucketName}/{s3key}");
                    parquetIndex += 1;
                }
            }

            // write what ever left less than 200000
            if (rows.Count > 0)
            {
                var s3key = etlSettings.MakeTargetS3Key(dateKey, "", false, parquetIndex);
                await targetS3.WriteResultRowsToS3Bucket(rows, resultSetMetadata, etlSettings, s3key);
                result.Add($"s3://{etlSettings.TargetS3BucketName}/{s3key}");
                parquetIndex += 1;
            }

            {
                // load partition to athena table
                await awsAthenaAPI.LoadPartition(
                    $"`{etlSettings.AthenaDatabaseName}`.`{etlSettings.AthenaTableName}`",
                    $"`{etlSettings.DatePartitionKey}` = '{dateKey}'",
                    $"s3://{etlSettings.TargetS3BucketName}/{etlSettings.TargetS3Prefix}/{dateKey}/");
            }

            return result;

        }

        public static async Task WriteResultRowsToS3Bucket(this AWSS3API awsS3Api, List<Row> rows, ResultSetMetadata metadata, EtlSettings etlSettings, string s3Key)
        {
            using (MemoryStream gaStream = new MemoryStream())
            {
                gaStream.WriteAthenaRowsAsParquet(metadata, etlSettings.Mappings, rows);
                using (MemoryStream uploadStream = new MemoryStream(gaStream.ToArray()))
                {
                    await awsS3Api.Upload(s3Key, uploadStream);
                }
            }
            rows.Clear();
        }

        public static List<FieldMapping> ToFieldMapping(this GetQueryResultsResponse getQueryResultsResponse)
        {
            var columnInfos = getQueryResultsResponse.ResultSet.ResultSetMetadata.ColumnInfo;
            var results = new List<FieldMapping>();
            foreach (var column in columnInfos)
            {
                results.Add(new FieldMapping()
                {
                    SourceFieldName = column.Name,
                    MappedName = column.Name,
                    MappedType = column.ToAthenaType()
                });
            }
            return results;
        }

        public static AthenaTypeEnum ToAthenaType(this ColumnInfo column)
        {
            switch (column.Type)
            {
                case AthenaDataTypes.@string:
                case AthenaDataTypes.varchar:
                    return AthenaTypeEnum.athena_string;
                case AthenaDataTypes.tinyint:
                    return AthenaTypeEnum.athena_tinyint;
                case AthenaDataTypes.smallint:
                    return AthenaTypeEnum.athena_smallint;
                case AthenaDataTypes.integer:
                    return AthenaTypeEnum.athena_integer;
                case AthenaDataTypes.bigint:
                    return AthenaTypeEnum.athena_bigint;
                case AthenaDataTypes.@double:
                    return AthenaTypeEnum.athena_double;
                case AthenaDataTypes.boolean:
                    return AthenaTypeEnum.athena_boolean;
                case AthenaDataTypes.date:
                    return AthenaTypeEnum.athena_date;
                case AthenaDataTypes.timestamp:
                    return AthenaTypeEnum.athena_timestamp;
            }
            throw new Exception("Unexpected Athena Type");
        }

        public static AWSAthenaAPI CreateSourceAthenaAPI(this EtlSettings etlSettings)
        {
            var athena = etlSettings.AthenaQuerySource;
            if (athena == null) throw new Exception("The ETL has an empty Athena source setting.");
            return new AWSAthenaAPI(new AWSAthenaOptions()
            {
                Key = athena.Key,
                Secret = athena.Secret,
                DefaultOutputLocation = athena.DefaultOutputLocation,
                Region = athena.Region
            });
        }

        /// <summary>
        /// compile the pipes and run the definition query
        /// </summary>
        /// <param name="etlSettings"></param>
        /// <returns></returns>
        public static async Task ParseAthenaQueryPipes(this EtlSettings etlSettings)
        {
            if (etlSettings.SourceType != EtlSourceEnum.AmazonAthenaPipes) return;

            var pipesSource = etlSettings.AthenaQueryPipesSource;
            AthenaParserSetting parserLogger = new AthenaParserSetting();
            parserLogger.DefaultExportPath = $"s3://{etlSettings.TargetS3BucketName}/{etlSettings.TargetS3Prefix}".FixPathEnd();
            parserLogger.DefaultTableName = $"`{etlSettings.AthenaDatabaseName}`.`{etlSettings.AthenaTableName}`";
            parserLogger.Date = DateTime.UtcNow.AddDays(-pipesSource.DaysAgo);
            parserLogger.DateFormat = pipesSource.DateFormat;
            parserLogger.TempDatabase = pipesSource.TempDatabase;
            parserLogger.TempTablePath = pipesSource.TempDataPath.FixPathEnd();

            pipesSource.ParseErrors = "";

            if (!string.IsNullOrWhiteSpace(etlSettings.AthenaQueryPipesSource.Caches))
            {
                try
                {
                    var caches = JsonConvert.DeserializeObject<List<CacheSetting>>(etlSettings.AthenaQueryPipesSource.Caches);
                    foreach(var cache in caches)
                    {
                        if (!cache.S3Path.EndsWith("/")) cache.S3Path += "/";
                        parserLogger.Caches.Add(cache.Key, cache);
                    }
                }
                catch(Exception ex)
                {
                    pipesSource.ParseErrors += ex.Message;
                    pipesSource.ParseErrors += "\n";
                }
            }

            try
            {
                var parsed = pipesSource.AthenaSQL.ParseAthenaPipes(parserLogger);
                pipesSource.ParsedQuery = parsed.ToQueryString();
            }
            catch(Exception ex)
            {
                pipesSource.ParseErrors = parserLogger.ToString();
                pipesSource.ParseErrors += "\n";
                pipesSource.ParseErrors += ex.Message;
                pipesSource.ParsedQuery = "";
            }

            // run if there is definition query
            if(Regex.IsMatch(pipesSource.AthenaDefinitionSQL, @"\S+"))
            {
                var athenaApi = etlSettings.CreatePipesSourceAthenaAPI();
                var getResultRequest = await athenaApi.ExecuteQuery(pipesSource.AthenaDefinitionSQL);
                var response = await athenaApi.ReadOneResult(getResultRequest);
                etlSettings.Mappings = response.ToFieldMapping();
                // load data schema to the etlsetting schema

                var sample = new DataSample()
                {
                    Rows = new List<DataRow>()
                };

                var data = response.ReadData();
                foreach (var row in data)
                {
                    var dataRow = new DataRow()
                    {
                        Items = row.Select(item => item.ToString()).ToList()
                    };
                    sample.Rows.Add(dataRow);
                }

                etlSettings.Sample = sample;
            }
        }

        /// <summary>
        /// run the athena query pipes
        /// </summary>
        /// <param name="etlSettings"></param>
        /// <param name="useDate"></param>
        /// <returns></returns>
        public static async Task RunAthenaQueryPipes(this EtlSettings etlSettings, DateTime? useDate = null)
        {
            if (etlSettings.SourceType != EtlSourceEnum.AmazonAthenaPipes) return;

            var pipesSource = etlSettings.AthenaQueryPipesSource;

            AthenaParserSetting parserSetting = new AthenaParserSetting();
            parserSetting.DefaultExportPath = $"s3://{etlSettings.TargetS3BucketName}/{etlSettings.TargetS3Prefix}".FixPathEnd();
            parserSetting.DefaultTableName = $"`{etlSettings.AthenaDatabaseName}`.`{etlSettings.AthenaTableName}`";
            parserSetting.Date = useDate == null ? DateTime.UtcNow.AddDays(-pipesSource.DaysAgo) : useDate.Value.AddDays(-pipesSource.DaysAgo);
            parserSetting.DateFormat = pipesSource.DateFormat;
            parserSetting.TempDatabase = pipesSource.TempDatabase;
            parserSetting.TempTablePath = pipesSource.TempDataPath.FixPathEnd();

            var caches = JsonConvert.DeserializeObject<List<CacheSetting>>(etlSettings.AthenaQueryPipesSource.Caches);
            foreach (var cache in caches)
            {
                if (!cache.S3Path.EndsWith("/")) cache.S3Path += "/";
                parserSetting.Caches.Add(cache.Key, cache);
            }

            var parsed = pipesSource.AthenaSQL.ParseAthenaPipes(parserSetting);

            await etlSettings.ExecuteControlFlow(parsed, parserSetting);

            var athenaApi = etlSettings.CreatePipesSourceAthenaAPI();
            foreach (var kvp in parserSetting.Partitions)
            {
                await athenaApi.LoadPartitionIfNotExists(parserSetting.DefaultTableName, kvp.Key, kvp.Value);
            }
        }

        public static StateMachineQueryContext BuildStateMachineQueryContext(this EtlSettings etlSettings, DateTime? useDate = null)
        {
            if (etlSettings.SourceType != EtlSourceEnum.AmazonAthenaPipes) return null;

            var pipesSource = etlSettings.AthenaQueryPipesSource;

            AthenaParserSetting parserSetting = new AthenaParserSetting();
            parserSetting.DefaultExportPath = $"s3://{etlSettings.TargetS3BucketName}/{etlSettings.TargetS3Prefix}".FixPathEnd();
            parserSetting.DefaultTableName = $"`{etlSettings.AthenaDatabaseName}`.`{etlSettings.AthenaTableName}`";
            parserSetting.Date = useDate == null ? DateTime.UtcNow.AddDays(-pipesSource.DaysAgo) : useDate.Value.AddDays(-pipesSource.DaysAgo);
            parserSetting.DateFormat = pipesSource.DateFormat;
            parserSetting.TempDatabase = pipesSource.TempDatabase;
            parserSetting.TempTablePath = pipesSource.TempDataPath.FixPathEnd();

            var caches = JsonConvert.DeserializeObject<List<CacheSetting>>(etlSettings.AthenaQueryPipesSource.Caches);
            foreach (var cache in caches)
            {
                if (!cache.S3Path.EndsWith("/")) cache.S3Path += "/";
                parserSetting.Caches.Add(cache.Key, cache);
            }

            StateMachineQueryContext context = new StateMachineQueryContext();

            context.raw = pipesSource.AthenaSQL;

            context.settings = new StateMachineSettings()
            {
                DefaultExportPath = parserSetting.DefaultExportPath,
                DefaultTableName = parserSetting.DefaultTableName,
                Date = parserSetting.Date,
                DateFormat = parserSetting.DateFormat,
                TempDatabase = parserSetting.TempDatabase,
                TempTablePath = parserSetting.TempTablePath,
                Caches = parserSetting.Caches.Values.ToList(),
                Clearings = parserSetting.Clearings.Select(kvp => new KeyValueEntry() { Key = kvp.Key, Value = kvp.Value }).ToList(),
                Commands = parserSetting.Commands,
                DroppingTables = parserSetting.DroppingTables,
                Partitions = parserSetting.Partitions.Select(kvp => new KeyValueEntry() { Key = kvp.Key, Value = kvp.Value }).ToList(),
                Variables = parserSetting.Variables.Select(kvp => new KeyValueEntry() { Key = kvp.Key, Value = kvp.Value }).ToList()
            };

            //var parsed = pipesSource.AthenaSQL.ParseAthenaPipes(parserSetting);

            return context;

            //var athenaApi = etlSettings.CreatePipesSourceAthenaAPI();
            //foreach (var kvp in parserSetting.Partitions)
            //{
            //    await athenaApi.LoadPartitionIfNotExists(parserSetting.DefaultTableName, kvp.Key, kvp.Value);
            //}
        }

        public static AthenaParserSetting BuildParserSetting(this StateMachineQueryContext context)
        {
            StateMachineSettings settings = context.settings;
            AthenaParserSetting parserSetting = new AthenaParserSetting();
            parserSetting.DefaultExportPath = settings.DefaultExportPath;
            parserSetting.DefaultTableName = settings.DefaultTableName;
            parserSetting.Date = settings.Date;
            parserSetting.DateFormat = settings.DateFormat;
            parserSetting.TempDatabase = settings.TempDatabase;
            parserSetting.TempTablePath = settings.TempTablePath;
            return parserSetting;
        }

        private static string FixPathEnd(this string value)
        {
            return value.EndsWith("/") ? value : value + "/";
        }

        public static AWSAthenaAPI CreatePipesSourceAthenaAPI(this EtlSettings etlSettings)
        {
            var athenaPipes = etlSettings.AthenaQueryPipesSource;
            if (athenaPipes == null) throw new Exception("The ETL has an empty Athena source setting.");
            return new AWSAthenaAPI(new AWSAthenaOptions()
            {
                Key = athenaPipes.Key,
                Secret = athenaPipes.Secret,
                DefaultOutputLocation = athenaPipes.DefaultOutputLocation,
                Region = athenaPipes.Region
            });
        }

        public static AWSS3API CreatePipesSourceS3API(this EtlSettings etlSettings)
        {
            var athenaPipes = etlSettings.AthenaQueryPipesSource;
            if (athenaPipes == null) throw new Exception("The ETL has an empty Athena source setting.");
            return new AWSS3API(new AWSS3Options()
            {
                Key = athenaPipes.Key,
                Secret = athenaPipes.Secret,
                Region = athenaPipes.Region
            });
        }
        public static async Task ClearAthenaTable(this EtlSettings etlSettings, string tableName, string s3Path)
        {
            if (etlSettings.SourceType != EtlSourceEnum.AmazonAthenaPipes) return;
            var pipesSource = etlSettings.AthenaQueryPipesSource;
            var athenaApi = etlSettings.CreatePipesSourceAthenaAPI();
            Console.WriteLine($"DROP TABLE IF EXISTS {tableName}");
            var executionId = await athenaApi.StartQuery($"DROP TABLE IF EXISTS {tableName}");
            while (!await athenaApi.IsExecutionCompleted(executionId))
            {
                Thread.Sleep(2000);
            }
            var s3Api = etlSettings.CreatePipesSourceS3API();
            var s3Object = s3Path.ParseS3URI();
            if(s3Object is S3Object)
            {
                Console.WriteLine($"Delete S3: {s3Path}");
                var files = await s3Api.ListFiles(s3Object.Key, "/", s3Object.BucketName);
                await s3Api.Delete(files.Select(key => $"{s3Object.Key}{key}"), s3Object.BucketName);
                Console.WriteLine($"{s3Path}: {files.Count} S3 Files Deleted");
            }
        }

        public static async Task ClearAthenaTable(this AWSAthenaAPI athenaApi, AWSS3API awsS3Api, string tableName, string s3Path) 
        {
            Console.WriteLine($"DROP TABLE IF EXISTS {tableName}");
            var executionId = await athenaApi.StartQuery($"DROP TABLE IF EXISTS {tableName}");
            while (!await athenaApi.IsExecutionCompleted(executionId))
            {
                Thread.Sleep(2000);
            }
            var s3Object = s3Path.ParseS3URI();
            if (s3Object is S3Object)
            {
                Console.WriteLine($"Delete S3: {s3Path}");
                var files = await awsS3Api.ListFiles(s3Object.Key, "/", s3Object.BucketName);
                if (files.Any())
                {
                    await awsS3Api.Delete(files.Select(key => $"{s3Object.Key}{key}"), s3Object.BucketName);
                }
                Console.WriteLine($"{s3Path}: {files.Count} S3 Files Deleted");
            }
        }

        public static async Task DropAthenaTable(this AWSAthenaAPI athenaApi, string tableName)
        {
            string query = $"DROP TABLE IF EXISTS {tableName}";
            Console.WriteLine(query);
            var executionId = await athenaApi.StartQuery(query);
            while (!await athenaApi.IsExecutionCompleted(executionId))
            {
                Thread.Sleep(2000);
            }
        }

        public static async Task ClearAthenaTables(this StateMachineQueryContext context, AWSAthenaAPI athenaApi, AWSS3API awsS3Api)
        {
            var parserSetting = context.BuildParserSetting();
            var pipes = context.raw.ParseAthenaPipes(parserSetting);
            foreach (var clearing in parserSetting.Clearings)
            {
                await athenaApi.ClearAthenaTable(awsS3Api, clearing.Key, clearing.Value);
            }
        }

        public static async Task ClearTempTables(this StateMachineQueryContext context, AWSAthenaAPI athenaApi, AWSS3API awsS3Api)
        {
            foreach (var clearing in context.settings.Clearings)
            {
                await athenaApi.ClearAthenaTable(awsS3Api, clearing.Key, clearing.Value);
            }
            context.settings.Clearings.Clear();
        }

        public static async Task DropAthenaTables(this StateMachineQueryContext context, AWSAthenaAPI athenaApi)
        {
            foreach(var dropping in context.settings.DroppingTables)
            {
                await DropAthenaTable(athenaApi, dropping);
            }
        }

        public static async Task LoadPartitions(this StateMachineQueryContext context, AWSAthenaAPI athenaApi)
        {
            //Console.WriteLine("LoadPartitions:");
            foreach (var patition in context.settings.Partitions)
            {
                await LoadAthenaParition(athenaApi, context.settings.DefaultTableName, patition.Key, patition.Value);
            }
        }

        public static async Task LoadAthenaParition(this AWSAthenaAPI athenaApi, string tableName, string key, string location)
        {
            string dropQuery = $"ALTER TABLE {tableName} DROP IF EXISTS PARTITION ({key})";
            Console.WriteLine(dropQuery);
            var dropExecutionId = await athenaApi.StartQuery(dropQuery);
            while (!await athenaApi.IsExecutionCompleted(dropExecutionId))
            {
                Thread.Sleep(500);
            }

            string addQuery = $"ALTER TABLE {tableName} ADD IF NOT EXISTS PARTITION ({key}) LOCATION '{location}'";
            Console.WriteLine(addQuery);
            var addExecutionId = await athenaApi.StartQuery(addQuery);
            while (!await athenaApi.IsExecutionCompleted(addExecutionId))
            {
                Thread.Sleep(500);
            }
        }

    }
}
