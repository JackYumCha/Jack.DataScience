using System;
using System.Linq;
using System.Threading.Tasks;
using System.Reflection;
using Amazon;
using Amazon.Runtime;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.DocumentModel;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Newtonsoft.Json.Linq;
using System.Collections.Generic;

namespace Jack.DataScience.Data.AWSDynamoDB
{
    public class AWSDynamoAPI
    {
        private readonly AWSDynamoDBOptions awsDynamoDBOptions;
        private readonly AmazonDynamoDBClient amazonDynamoDBClient;
        private readonly BasicAWSCredentials basicAWSCredentials;
        private readonly JsonSerializerSettings jsonSerializerSettings;
        private static readonly object[] EmptyObjectArray = new object[] { };

        public AWSDynamoAPI(AWSDynamoDBOptions awsDynamoDBOptions )
        {
            this.awsDynamoDBOptions = awsDynamoDBOptions;
            basicAWSCredentials = new BasicAWSCredentials(awsDynamoDBOptions.Key, awsDynamoDBOptions.Secret);
            amazonDynamoDBClient = new AmazonDynamoDBClient(basicAWSCredentials, RegionEndpoint.GetBySystemName(awsDynamoDBOptions.Region));
            jsonSerializerSettings = new JsonSerializerSettings()
            {
                Converters = { new StringEnumConverter() }
            };
        }

        public JsonSerializerSettings JsonSerializerSettings { get => jsonSerializerSettings; }

        private string UseTableName(string tableName) => string.IsNullOrWhiteSpace(tableName) ? awsDynamoDBOptions.TableName : tableName;
        /// <summary>
        /// read item from the table
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key"></param>
        /// <returns></returns>
        public async Task<T> ReadItem<T>(string key, string tableName = null) where T: class, new()
        {
            tableName = UseTableName(tableName);
            var table = Table.LoadTable(amazonDynamoDBClient, tableName);
            var type = typeof(T);
            var properties = type.GetProperties();

            var config = new GetItemOperationConfig
            {
                AttributesToGet = properties.Select(property => property.Name).ToList(),
                ConsistentRead = true
            };

            var document = await table.GetItemAsync(key, config);
            if (document == null) return null;
            return document.ParseDocument<T>(jsonSerializerSettings);
        }

        public async Task<T> ReadItem<T>(Dictionary<string, object> keys, string tableName = null) where T : class, new()
        {
            tableName = UseTableName(tableName);
            var table = Table.LoadTable(amazonDynamoDBClient, tableName);
            var type = typeof(T);
            var properties = type.GetProperties();

            var config = new GetItemOperationConfig
            {
                AttributesToGet = properties.Select(property => property.Name).ToList(),
                ConsistentRead = true
            };

            var attributeKeys = keys.ToDictionary(kvp => kvp.Key, kvp => kvp.Value.AsDBEntry(jsonSerializerSettings));

            var document = await table.GetItemAsync(attributeKeys, config);
            if (document == null) return null;
            return document.ParseDocument<T>(jsonSerializerSettings);
        }

        /// <summary>
        /// write item to the table
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="obj"></param>
        /// <returns></returns>
        public async Task WriteItem<T>(T obj, string tableName = null) where T : class, new()
        {
            tableName = UseTableName(tableName);
            var table = Table.LoadTable(amazonDynamoDBClient, tableName);
            var document = obj.BuildDocument(jsonSerializerSettings);
            await table.PutItemAsync(document);
        }

        public async Task DeleteItem(Dictionary<string, object> keys, string tableName = null)
        {
            tableName = UseTableName(tableName);
            var attributeKeys = keys.ToDictionary(kvp => kvp.Key, kvp => kvp.Value.AsAttributeValue());
            await amazonDynamoDBClient.DeleteItemAsync(new DeleteItemRequest()
            {
                Key = attributeKeys,
                TableName = tableName
            });
        }

        public async Task UpsertItem<T>(T obj, IEnumerable<string> keys, string tableName = null) where T : class, new()
        {
            tableName = UseTableName(tableName);
            var table = Table.LoadTable(amazonDynamoDBClient, tableName);
            var type = typeof(T);
            var properties = type.GetProperties();
            var attributeKeys = new Dictionary<string, DynamoDBEntry>();
            foreach(var key in keys)
            {
                attributeKeys.Add(key, type.GetProperty(key).GetValue(obj).AsDBEntry(jsonSerializerSettings));
            }
            var config = new GetItemOperationConfig
            {
                AttributesToGet = keys.ToList(),
                ConsistentRead = true
            };
            var found = await table.GetItemAsync(attributeKeys, config);
            if(found == null) // insert
            {
                var document = obj.BuildDocument(jsonSerializerSettings);
                await table.PutItemAsync(document);
            }
            else // update
            {
                var document = obj.BuildDocument(jsonSerializerSettings);
                await table.UpdateItemAsync(document);
            }
        }

        public async Task<bool> Exists<T>(T obj, IEnumerable<string> keys, string tableName = null) where T : class, new()
        {
            tableName = UseTableName(tableName);
            var table = Table.LoadTable(amazonDynamoDBClient, tableName);
            var type = typeof(T);
            var properties = type.GetProperties();
            var attributeKeys = new Dictionary<string, DynamoDBEntry>();
            foreach (var key in keys)
            {
                attributeKeys.Add(key, type.GetProperty(key).GetValue(obj).AsDBEntry(jsonSerializerSettings));
            }
            var config = new GetItemOperationConfig
            {
                AttributesToGet = keys.ToList(),
                ConsistentRead = true
            };
            var found = await table.GetItemAsync(attributeKeys, config);
            return found != null;
        }

        public async Task<List<T>> Query<T>(string indexName, QueryOperator op, List<object> values, int limit = 0, string tableName = null) where T: class, new()
        {
            tableName = UseTableName(tableName);
            var table = Table.LoadTable(amazonDynamoDBClient, tableName);
            var attributeValues = values.Select(value => value.AsAttributeValue()).ToList();
            var filter = new QueryFilter(indexName, op, attributeValues);
            var request = new QueryRequest()
            {
                KeyConditions = filter.ToConditions(),
                TableName = tableName,
            };
            if (limit > 0) request.Limit = limit;

            Dictionary<string, AttributeValue> lastEvaluatedKey = null;

            List<T> results = new List<T>();

            do
            {
                request.ExclusiveStartKey = lastEvaluatedKey;
                var response = await amazonDynamoDBClient.QueryAsync(request);
                results.AddRange(response
                    .Items
                    .Select(item => item.ParseDocument<T>(jsonSerializerSettings))
                );
                lastEvaluatedKey = response.LastEvaluatedKey;

            } while (lastEvaluatedKey != null && lastEvaluatedKey.Any());

            return results;
        }

        public async Task<List<T>> Scan<T>(string indexName, QueryOperator op, List<object> values, int limit = 0, string tableName = null) where T : class, new()
        {
            tableName = UseTableName(tableName);
            var table = Table.LoadTable(amazonDynamoDBClient, tableName);
            var attributeValues = values.Select(value => value.AsAttributeValue()).ToList();
            var filter = new QueryFilter(indexName, op, attributeValues);
            var request = new ScanRequest()
            {
                ScanFilter = filter.ToConditions(),
                TableName = tableName,
            };
            if (limit > 0) request.Limit = limit;
            var response = await amazonDynamoDBClient.ScanAsync(request);
            return response
                .Items
                .Select(item => item.ParseDocument<T>(jsonSerializerSettings))
                .ToList();
        }
    }
}
