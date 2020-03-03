using System;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;
using Amazon;
using Amazon.Runtime;
using Amazon.Athena;
using Amazon.Athena.Model;
using Jack.DataScience.Data.Converters;
using Jack.DataScience.Compute.AWSLambda;
using Jack.DataScience.MQ.AWSSQS;
using Newtonsoft.Json;

namespace Jack.DataScience.Data.AWSAthena
{
    public class AWSAthenaAPI
    {
        private readonly AWSAthenaOptions awsAthenaOptions;
        private readonly BasicAWSCredentials basicAWSCredentials;
        private readonly AmazonAthenaClient amazonAthenaClient;
        private readonly SessionAWSCredentials sessionAWSCredentials;

        private string DefaultOutputLocation;
        public AWSAthenaAPI(AWSAthenaOptions awsAthenaOptions)
        {
            this.awsAthenaOptions = awsAthenaOptions;
            basicAWSCredentials = new BasicAWSCredentials(awsAthenaOptions.Key, awsAthenaOptions.Secret);
            amazonAthenaClient = new AmazonAthenaClient(basicAWSCredentials, RegionEndpoint.GetBySystemName(awsAthenaOptions.Region));
            DefaultOutputLocation = awsAthenaOptions.DefaultOutputLocation;
        }

        public AWSAthenaAPI(BasicAWSCredentials basicAWSCredentials, RegionEndpoint regionEndpoint)
        {
            this.basicAWSCredentials = basicAWSCredentials;
            amazonAthenaClient = new AmazonAthenaClient(basicAWSCredentials, regionEndpoint);
            DefaultOutputLocation = Environment.GetEnvironmentVariable("AWSAthenaDefaultOutputLocation");
        }

        /// <summary>
        /// provide support for resolving AthenaAPI from AWS environment
        /// </summary>
        /// <param name="sessionAWSCredentials"></param>
        public AWSAthenaAPI(SessionAWSCredentials sessionAWSCredentials)
        {
            this.sessionAWSCredentials = sessionAWSCredentials;
            var credentials = sessionAWSCredentials.GetCredentials();
            basicAWSCredentials = new BasicAWSCredentials(credentials.AccessKey, credentials.SecretKey);
            amazonAthenaClient = new AmazonAthenaClient(sessionAWSCredentials);
            DefaultOutputLocation = Environment.GetEnvironmentVariable("AWSAthenaDefaultOutputLocation");
        }

        public AmazonAthenaClient AthenaClient { get => amazonAthenaClient; }
        public string OutputLocation { get => DefaultOutputLocation; }

        public async Task<string> LoadPartitionIfNotExists(string tableName, string keyFieldAssignment, string s3Location)
        {
            if (awsAthenaOptions.SQSOptions != null)
            {
                AWSSQSAPI awsSQSAPI = new AWSSQSAPI(awsAthenaOptions.SQSOptions);
                var request = new StartQueryExecutionRequest()
                {
                    QueryString = $@"ALTER TABLE {tableName} ADD IF NOT EXISTS PARTITION ({keyFieldAssignment}) LOCATION '{s3Location}'",
                    ResultConfiguration = new ResultConfiguration()
                    {
                        OutputLocation = DefaultOutputLocation //"s3://aws-athena-query-results-855250023996-ap-southeast-2/"
                    }
                };
                await awsSQSAPI.SendMessage(JsonConvert.SerializeObject(request));
                return $"SQS-{awsAthenaOptions.SQSOptions.Url}";
            }
            else
            {
                var queryInfo = await amazonAthenaClient.StartQueryExecutionAsync(new StartQueryExecutionRequest()
                {
                    QueryString = $@"ALTER TABLE {tableName} ADD IF NOT EXISTS PARTITION ({keyFieldAssignment}) LOCATION '{s3Location}'",
                    ResultConfiguration = new ResultConfiguration()
                    {
                        OutputLocation = DefaultOutputLocation //"s3://aws-athena-query-results-855250023996-ap-southeast-2/"
                    }
                });

                return queryInfo.QueryExecutionId;
            }
        }
        public async Task<string> LoadPartition(string tableName, string keyFieldAssignment, string s3Location)
        {
            /*
            ALTER TABLE twilio_log.twilogs ADD PARTITION (logdate=20181203)
            LOCATION 's3://datascience-twilio-sms-logs/twilio-sms-log-2018-12-03/'
             */

            if(awsAthenaOptions.SQSOptions != null)
            {
                AWSSQSAPI awsSQSAPI = new AWSSQSAPI(awsAthenaOptions.SQSOptions);
                var request = new StartQueryExecutionRequest()
                {
                    QueryString = $@"ALTER TABLE {tableName} ADD PARTITION ({keyFieldAssignment}) LOCATION '{s3Location}'",
                    ResultConfiguration = new ResultConfiguration()
                    {
                        OutputLocation = DefaultOutputLocation //"s3://aws-athena-query-results-855250023996-ap-southeast-2/"
                    }
                };
                await awsSQSAPI.SendMessage(JsonConvert.SerializeObject(request));
                return $"SQS-{awsAthenaOptions.SQSOptions.Url}";
            }
            else
            {
                var queryInfo = await amazonAthenaClient.StartQueryExecutionAsync(new StartQueryExecutionRequest()
                {
                    QueryString = $@"ALTER TABLE {tableName} ADD PARTITION ({keyFieldAssignment}) LOCATION '{s3Location}'",
                    ResultConfiguration = new ResultConfiguration()
                    {
                        OutputLocation = DefaultOutputLocation //"s3://aws-athena-query-results-855250023996-ap-southeast-2/"
                    }
                });

                return queryInfo.QueryExecutionId;
            }

            //var queryStatus = amazonAthenaClient.GetQueryExecutionAsync(new GetQueryExecutionRequest() { QueryExecutionId = queryInfo.QueryExecutionId });

            //while(!queryStatus.IsCanceled && !queryStatus.IsCompleted && !queryStatus.IsFaulted)
            //{
            //    Thread.Sleep(100);
            //    queryStatus = amazonAthenaClient.GetQueryExecutionAsync(new GetQueryExecutionRequest() { QueryExecutionId = queryInfo.QueryExecutionId });
            //}

            //if (queryStatus.IsCanceled) return "canceled";
            //if (queryStatus.IsCompleted) return "completed";
            //if (queryStatus.IsFaulted) return "faulted";
            //return "unknown";
        }


        public async Task<int> LoadPartitionViaSQS()
        {
            int result = 0;
            if (awsAthenaOptions.SQSOptions != null)
            {
                AWSSQSAPI awsSQSAPI = new AWSSQSAPI(awsAthenaOptions.SQSOptions);

                var messages = await awsSQSAPI.ReceiveMessage(10, 30);

                if (messages.Any())
                {
                    foreach (var message in messages)
                    {
                        try
                        {
                            var request = JsonConvert.DeserializeObject<StartQueryExecutionRequest>(message.Body);
                            Console.WriteLine($"Execute Query: {request.QueryString}");
                            await ExecuteQuery(request); // this will wait until the query completes
                            await awsSQSAPI.DeleteMessage(message.ReceiptHandle);
                        }
                        catch (Exception ex)
                        {
                            if(ex.Message != null && ex.Message.Contains("Partition already exists"))
                            {
                                Console.WriteLine($"Partition already exists on Athena Load Partition.");
                                await awsSQSAPI.DeleteMessage(message.ReceiptHandle);
                            }
                            else
                            {
                                Console.WriteLine($"Exception on Load Partition: {ex.Message}");
                                Console.WriteLine(ex.Source);
                                Console.WriteLine(ex.StackTrace);
                            }
                        }
                        
                        result += 1;
                    }

                    // invoke next function
                    if (awsAthenaOptions.LambdaOptions != null && !string.IsNullOrWhiteSpace(awsAthenaOptions.LoaderFunction))
                    {
                        AWSLambdaAPI awsLambdaAPI = new AWSLambdaAPI(awsAthenaOptions.LambdaOptions);

                        awsLambdaAPI.Invoke(awsAthenaOptions.LoaderFunction, JsonConvert.SerializeObject(""));
                        awsLambdaAPI.Invoke(awsAthenaOptions.LoaderFunction, JsonConvert.SerializeObject(""));

                        Thread.Sleep(2000);
                    }
                }
            }
            return result;
        }


        public async Task<GetQueryResultsRequest> ExecuteQuery(string query) // Task<GetQueryResultsResponse>
        {
            var startResponse = await amazonAthenaClient.StartQueryExecutionAsync(new StartQueryExecutionRequest()
            {
                QueryString = query,
                ResultConfiguration = new ResultConfiguration()
                {
                    OutputLocation = DefaultOutputLocation,
                }
            });

            var queryId = startResponse.QueryExecutionId;

            GetQueryExecutionRequest getQueryExecutionRequest = new GetQueryExecutionRequest()
            {
                QueryExecutionId = queryId
            };

            bool isStillingRunning = true;
            while (isStillingRunning)
            {
                var executionResponse = await amazonAthenaClient.GetQueryExecutionAsync(getQueryExecutionRequest);
                var state = executionResponse.QueryExecution.Status.State;

                if(state == QueryExecutionState.FAILED)
                {
                    throw new Exception($"Query Failed to run with Error Message: \n{executionResponse.QueryExecution.Status.StateChangeReason}");
                }
                else if(state == QueryExecutionState.CANCELLED)
                {
                    throw new Exception($"Query was cancelled.");
                }
                else if(state == QueryExecutionState.SUCCEEDED)
                {
                    isStillingRunning = false;
                }
            }

            return new GetQueryResultsRequest()
            {
                QueryExecutionId = queryId
            };

            //var resultResponse = await amazonAthenaClient.GetQueryResultsAsync(new GetQueryResultsRequest()
            //{
            //    QueryExecutionId = queryId
            //});

            //return resultResponse;
        }

        public async Task<GetQueryResultsRequest> ExecuteQuery(StartQueryExecutionRequest startQueryExecutionRequest) // Task<GetQueryResultsResponse>
        {
            var startResponse = await amazonAthenaClient.StartQueryExecutionAsync(startQueryExecutionRequest);

            var queryId = startResponse.QueryExecutionId;

            GetQueryExecutionRequest getQueryExecutionRequest = new GetQueryExecutionRequest()
            {
                QueryExecutionId = queryId
            };

            bool isStillingRunning = true;
            while (isStillingRunning)
            {
                var executionResponse = await amazonAthenaClient.GetQueryExecutionAsync(getQueryExecutionRequest);
                var state = executionResponse.QueryExecution.Status.State;

                if (state == QueryExecutionState.FAILED)
                {
                    throw new Exception($"Query Failed to run with Error Message: \n{executionResponse.QueryExecution.Status.StateChangeReason}");
                }
                else if (state == QueryExecutionState.CANCELLED)
                {
                    throw new Exception($"Query was cancelled.");
                }
                else if (state == QueryExecutionState.SUCCEEDED)
                {
                    isStillingRunning = false;
                }
            }

            return new GetQueryResultsRequest()
            {
                QueryExecutionId = queryId
            };

            //var resultResponse = await amazonAthenaClient.GetQueryResultsAsync(new GetQueryResultsRequest()
            //{
            //    QueryExecutionId = queryId
            //});

            //return resultResponse;
        }

        public async Task<bool> IsExecutionCompleted(string executionId) // Task<GetQueryResultsResponse>
        {
            GetQueryExecutionRequest getQueryExecutionRequest = new GetQueryExecutionRequest()
            {
                QueryExecutionId = executionId
            };

            bool completed = false;
            var executionResponse = await amazonAthenaClient.GetQueryExecutionAsync(getQueryExecutionRequest);
            var state = executionResponse.QueryExecution.Status.State;

            if (state == QueryExecutionState.FAILED)
            {
                throw new Exception($"Query Failed to run with Error Message: \n{executionResponse.QueryExecution.Status.StateChangeReason}");
            }
            else if (state == QueryExecutionState.CANCELLED)
            {
                throw new Exception($"Query was cancelled.");
            }
            else if (state == QueryExecutionState.SUCCEEDED)
            {
                completed = true;
            }
            return completed;
        }

        public async Task<GetQueryResultsResponse> ReadOneResult(GetQueryResultsRequest request)
        {
            var response = await amazonAthenaClient.GetQueryResultsAsync(request);
            request.NextToken = response.NextToken;
            return response;
        }

        public IEnumerable<T> EnumerateResults<T>(GetQueryResultsRequest request) where T: class, new()
        {
            int first = 1;
            do
            {
                var response = ReadOneResult(request).GetAwaiter().GetResult();
                foreach(var item in response.ReadRows<T>(first))
                {
                    yield return item;
                }
                if (first == 1) first = 0;
            }
            while (request.NextToken != null);
        }

        /// <summary>
        /// you have to skip the first row, which is the schema
        /// </summary>
        /// <param name="request"></param>
        /// <param name="responseReceived"></param>
        /// <returns></returns>
        public IEnumerable<Row> EnumerateRows(GetQueryResultsRequest request, Action<GetQueryResultsResponse> responseReceived)
        {
            do
            {
                var response = ReadOneResult(request).GetAwaiter().GetResult();
                responseReceived?.Invoke(response);
                foreach (var row in response.ResultSet.Rows)
                {
                    yield return row;
                }
            }
            while (request.NextToken != null);
        }

        public async Task<AthenaQueryFlatResult> GetFlatResult(GetQueryResultsRequest request, int rowsLimit = 1000000)
        {
            bool columnsRead = false;
            var data = new List<List<string>>();
            bool firstRowRemoved = false;
            var result = new AthenaQueryFlatResult()
            {
                Data = data
            };
            do
            {
                var response = await ReadOneResult(request);
                if (!columnsRead)
                {
                    result.Columns = response.ResultSet.ResultSetMetadata.ColumnInfo.Select(c => c.Name).ToList();
                    columnsRead = true;
                }
                foreach (var row in response.ResultSet.Rows)
                {
                    data.Add(row.Data.Select(c => c.VarCharValue).ToList());
                }
                if (!firstRowRemoved)
                {
                    data.RemoveAt(0);
                    firstRowRemoved = true;
                }

                if(data.Count > rowsLimit)
                {
                    if(request.NextToken != null)
                    {
                        result.Note = $"Query results were trancated at {data.Count} rows. Please try reduce the query size or aggregate in SQL before exporting.";
                    }
                    return result;
                }
            }
            while (request.NextToken != null);
            return result;
        }

        public async Task<string> StartQuery(string query)
        {
            var startResponse = await amazonAthenaClient.StartQueryExecutionAsync(new StartQueryExecutionRequest()
            {
                QueryString = query,
                ResultConfiguration = new ResultConfiguration()
                {
                    OutputLocation = DefaultOutputLocation,
                }
            });
            return startResponse.QueryExecutionId;
        }

        public async Task<List<T>> TryObtainQueryResultById<T>(string queryId, int skip = 1) where T : class, new()
        {
            GetQueryExecutionRequest getQueryExecutionRequest = new GetQueryExecutionRequest()
            {
                QueryExecutionId = queryId
            };

            bool isStillingRunning = true;
            var executionResponse = await amazonAthenaClient.GetQueryExecutionAsync(getQueryExecutionRequest);
            var state = executionResponse.QueryExecution.Status.State;

            if (state == QueryExecutionState.FAILED)
            {
                throw new Exception($"Query Failed to run with Error Message: \n{executionResponse.QueryExecution.Status.StateChangeReason}");
            }
            else if (state == QueryExecutionState.CANCELLED)
            {
                throw new Exception($"Query was cancelled.");
            }
            else if (state == QueryExecutionState.SUCCEEDED)
            {
                isStillingRunning = false;
            }
            if (isStillingRunning) return null;

            var resultResponse = await amazonAthenaClient.GetQueryResultsAsync(new GetQueryResultsRequest()
            {
                QueryExecutionId = queryId
            });

            return resultResponse.ReadRows<T>(skip);
        }

        public async Task<string> GenerateQueryCSSchema(string query) 
        {
            var result = await ExecuteQuery(query);
            var response = await ReadOneResult(result);
            return response.ToCSharpType();
        }

        public async Task<List<T>> GetQueryResults<T>(string query) where T: class, new()
        {
            var result = await ExecuteQuery(query);

            List<T> results = new List<T>();
            int first = 1;
            do
            {
                var response = await ReadOneResult(result);
                results.AddRange(response.ReadRows<T>(first));
                if (first > 0) first = 0;
            }
            while (result.NextToken != null);
            return results;
        }

        public async Task<List<List<object>>> GetQueryResults(string query, int skip = 1)
        {
            var result = await ExecuteQuery(query);
            List<List<object>> results = new List<List<object>>();
            int first = skip;
            do
            {
                var response = await ReadOneResult(result);
                results.AddRange(response.ReadData(first));
                if (first > 0) first = 0;
            }
            while (result.NextToken != null);
            return results;
        }

        public async Task RepairTable(string tableName)
        {
            await ExecuteQuery($"MSCK REPAIR TABLE {tableName}");
        }
    }

    public static class AthenaQueryExtensions
    {
        public static List<T> ReadRows<T>(this GetQueryResultsResponse getQueryResultsResponse, int skip = 1) where T: class, new()
        {

            var columnInfos = getQueryResultsResponse.ResultSet.ResultSetMetadata.ColumnInfo;
            var type = typeof(T);
            var propertyInfos = type.GetProperties().ToDictionary(p => p.Name.ToLower(), p => p);

            var results = new List<T>();

            foreach (var row in getQueryResultsResponse.ResultSet.Rows.Skip(skip))
            {
                results.Add(row.ReadRowAs<T>(columnInfos, propertyInfos));
            }
            return results;
        }

        public static List<List<object>> ReadData(this GetQueryResultsResponse getQueryResultsResponse, int skip = 1)
        {
            var columnInfos = getQueryResultsResponse.ResultSet.ResultSetMetadata.ColumnInfo;
            var results = new List<List<object>>();
            foreach (var row in getQueryResultsResponse.ResultSet.Rows.Skip(skip))
            {
                results.Add(row.ReadRowAsObjects(columnInfos));
            }
            return results;
        }

        /// <summary>
        /// this is the code generation tool to build C# type
        /// </summary>
        /// <param name="getQueryResultsResponse"></param>
        /// <returns></returns>
        public static string ToCSharpType(this GetQueryResultsResponse getQueryResultsResponse)
        {
            var columnInfos = getQueryResultsResponse.ResultSet.ResultSetMetadata.ColumnInfo;

            StringBuilder stb = new StringBuilder();
            stb.AppendLine($"public class QueryResult");
            stb.AppendLine($"{{");
            foreach (var column in columnInfos)
            {
                stb.AppendLine($"\tpublic {column.MapCSharpType()} {column.Name} {{ get; set; }}");
            }
            stb.AppendLine("}}");
            return stb.ToString();
        }

        public static Dictionary<string, string>ToSchemaDictionary(this GetQueryResultsResponse getQueryResultsResponse)
        {
            var columnInfos = getQueryResultsResponse.ResultSet.ResultSetMetadata.ColumnInfo;
            var results = new Dictionary<string, string>();
            foreach (var column in columnInfos)
            {
                results.Add(column.Name, column.MapCSharpType());
            }
            return results;
        }

        public static string MapCSharpType(this ColumnInfo column)
        {
            switch (column.Type)
            {
                case AthenaDataTypes.@string:
                case AthenaDataTypes.varchar:
                    return "string";
                case AthenaDataTypes.tinyint:
                    return "byte";
                case AthenaDataTypes.smallint:
                    return "short";
                case AthenaDataTypes.integer:
                    return "int";
                case AthenaDataTypes.bigint:
                    return "long";
                case AthenaDataTypes.@double:
                    return "double";
                case AthenaDataTypes.boolean:
                    return "bool";
                case AthenaDataTypes.date:
                    return "DateTime";
                case AthenaDataTypes.timestamp:
                    return "DateTime";
            }
            throw new Exception("Unexpected Athena Type");
        }

        public static Type AsCSharpType(this ColumnInfo column)
        {
            switch (column.Type)
            {
                case AthenaDataTypes.@string:
                case AthenaDataTypes.varchar:
                    return typeof(string);
                case AthenaDataTypes.tinyint:
                    return typeof(byte);
                case AthenaDataTypes.smallint:
                    return typeof(short);
                case AthenaDataTypes.integer:
                    return typeof(int);
                case AthenaDataTypes.bigint:
                    return typeof(long);
                case AthenaDataTypes.@double:
                    return typeof(double);
                case AthenaDataTypes.boolean:
                    return typeof(bool);
                case AthenaDataTypes.date:
                    return typeof(DateTime);
                case AthenaDataTypes.timestamp:
                    return typeof(DateTime);
            }
            throw new Exception("Unexpected Athena Type");
        }


        public static T ReadRowAs<T>(this Row row, List<ColumnInfo> columnInfos, Dictionary<string, PropertyInfo> propertyInfos = null) where T : class, new()
        {
            if (propertyInfos == null)
            {
                var type = typeof(T);
                propertyInfos = type.GetProperties().ToDictionary(p => p.Name.ToLower(), p => p);
            }

            var data = new T();

            for (int i = 0; i < columnInfos.Count; i++)
            {
                var column = columnInfos[i];
                var propertyName = column.Name.ToLower();
                if (!propertyInfos.ContainsKey(propertyName)) continue;
                var property = propertyInfos[propertyName];
                if(row.Data.Count < columnInfos.Count && row.Data.Count ==1)
                {
                    var splitted = row.Data[0].VarCharValue.Split(new char[] { '\t' }).Select(item => Regex.Replace(item, @"\s+$", "")).ToList();
                    var value = splitted[i];
                    property.SetValue(data, value.As(property.PropertyType));
                }
                else
                {
                    property.SetValue(data, row.Data[i].VarCharValue.As(property.PropertyType));
                }
            }

            return data;
        }

        public static List<object> ReadRowAsObjects(this Row row, List<ColumnInfo> columnInfos)
        {
            var results = new List<object>();
            for (int i = 0; i < columnInfos.Count; i++)
            {
                var column = columnInfos[i];
                results.Add(row.Data[i].VarCharValue.As(column.AsCSharpType()));
            }
            return results;
        }

        //public static T ReadRow<T>(this Row row, List<ColumnInfo> columnInfos, Dictionary<string, PropertyInfo> propertyInfos = null) where T: class, new()
        //{

        //    if(propertyInfos == null)
        //    {
        //        var type = typeof(T);
        //        propertyInfos = type.GetProperties().ToDictionary(p => p.Name.ToLower(), p => p);
        //    }

        //    for(int i = 0; i < columnInfos.Count; i ++)
        //    {
        //        var column = columnInfos[i];
        //        var data = new T();
        //        var stringValue = row.Data[i].VarCharValue;
        //        switch (column.Type)
        //        {
        //            case AthenaDataTypes.varchar:
        //                {

        //                }
        //                break;
        //            case AthenaDataTypes.tinyint: 
        //                {

        //                }
        //                break;
        //            case AthenaDataTypes.smallint:
        //                {

        //                }
        //                break;
        //            case AthenaDataTypes.integer:
        //                {

        //                }
        //                break;
        //            case AthenaDataTypes.bigint:
        //                {

        //                }
        //                break;
        //            case AthenaDataTypes.@double:
        //                {

        //                }
        //                break;
        //            case AthenaDataTypes.boolean:
        //                {

        //                }
        //                break;
        //            case AthenaDataTypes.date:
        //                {

        //                }
        //                break;
        //            case AthenaDataTypes.timestamp:
        //                {

        //                }
        //                break;
        //        }
        //    }
        //}
    }

    public static class AthenaDataTypes
    {
        public const string @string = "string";
        public const string varchar = "varchar";
        public const string tinyint = "tinyint";
        public const string smallint = "smallint";
        public const string integer = "integer";
        public const string bigint = "bigint";
        public const string @double = "double";
        public const string boolean = "boolean";
        public const string date = "date";
        public const string timestamp = "timestamp";
        
    }
}
