﻿using System;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Text;
using Amazon;
using Amazon.Runtime;
using Amazon.Athena;
using Amazon.Athena.Model;
using Jack.DataScience.Data.Converters;

namespace Jack.DataScience.Data.AWSAthena
{
    public class AWSAthenaAPI
    {
        private readonly AWSAthenaOptions awsAthenaOptions;
        private readonly BasicAWSCredentials basicAWSCredentials;
        private readonly AmazonAthenaClient amazonAthenaClient;
        public AWSAthenaAPI(AWSAthenaOptions awsAthenaOptions)
        {
            this.awsAthenaOptions = awsAthenaOptions;
            basicAWSCredentials = new BasicAWSCredentials(awsAthenaOptions.Key, awsAthenaOptions.Secret);
            amazonAthenaClient = new AmazonAthenaClient(basicAWSCredentials, RegionEndpoint.GetBySystemName(awsAthenaOptions.Region));
        }

        public async Task<string> LoadPartition(string tableName, string keyFieldAssignment, string s3Location)
        {
            /*
            ALTER TABLE twilio_log.twilogs ADD PARTITION (logdate=20181203)
            LOCATION 's3://datascience-twilio-sms-logs/twilio-sms-log-2018-12-03/'
             */

            var queryInfo = await amazonAthenaClient.StartQueryExecutionAsync(new StartQueryExecutionRequest()
            {
                QueryString = $@"ALTER TABLE {tableName} ADD PARTITION ({keyFieldAssignment}) LOCATION '{s3Location}'",
                ResultConfiguration = new ResultConfiguration()
                {
                    OutputLocation = "s3://aws-athena-query-results-855250023996-ap-southeast-2/"
                }
            });

            return queryInfo.QueryExecutionId;

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


        public async Task<GetQueryResultsResponse> ExecuteQuery(string query)
        {
            var startResponse = await amazonAthenaClient.StartQueryExecutionAsync(new StartQueryExecutionRequest()
            {
                QueryString = query,
                ResultConfiguration = new ResultConfiguration()
                {
                    OutputLocation = awsAthenaOptions.DefaultOutputLocation,
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

            var resultResponse = await amazonAthenaClient.GetQueryResultsAsync(new GetQueryResultsRequest()
            {
                QueryExecutionId = queryId
            });

            return resultResponse;
        }

        public async Task<string> GenerateQuerySchema(string query)
        {
            var result = await ExecuteQuery(query);
            return result.ToCSharpType();
        }

        public async Task<List<T>> GetQueryResults<T>(string query) where T: class, new()
        {
            var result = await ExecuteQuery(query);
            return result.ReadRows<T>();
        }
        
    }

    public static class AthenaQueryExtensions
    {
        public static List<T> ReadRows<T>(this GetQueryResultsResponse getQueryResultsResponse) where T: class, new()
        {

            var columnInfos = getQueryResultsResponse.ResultSet.ResultSetMetadata.ColumnInfo;
            var type = typeof(T);
            var propertyInfos = type.GetProperties().ToDictionary(p => p.Name.ToLower(), p => p);

            var results = new List<T>();

            foreach (var row in getQueryResultsResponse.ResultSet.Rows.Skip(1))
            {
                results.Add(row.ReadRowAs<T>(columnInfos, propertyInfos));
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

        public static string MapCSharpType(this ColumnInfo column)
        {
            switch (column.Type)
            {
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
                    return "boolean";
                case AthenaDataTypes.date:
                    return "DateTime";
                case AthenaDataTypes.timestamp:
                    return "DateTime";
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
                var stringValue = row.Data[i].VarCharValue;
                var property = propertyInfos[column.Name.ToLower()];
                property.SetValue(data, row.Data[i].VarCharValue.As(property.PropertyType));
            }

            return data;
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
