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

namespace Jack.DataScience.Data.AWSDynamoDB
{
    public class AWSDynamoAPI
    {
        private readonly AWSDynamoDBOptions awsDynamoDBOptions;
        private readonly AmazonDynamoDBClient amazonDynamoDBClient;
        private readonly BasicAWSCredentials basicAWSCredentials;
        private static readonly object[] EmptyObjectArray = new object[] { };

        public AWSDynamoAPI(AWSDynamoDBOptions awsDynamoDBOptions )
        {
            this.awsDynamoDBOptions = awsDynamoDBOptions;
            basicAWSCredentials = new BasicAWSCredentials(awsDynamoDBOptions.Key, awsDynamoDBOptions.Secret);
            amazonDynamoDBClient = new AmazonDynamoDBClient(basicAWSCredentials, RegionEndpoint.GetBySystemName(awsDynamoDBOptions.Region));
        }

        /// <summary>
        /// read item from the table
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key"></param>
        /// <returns></returns>
        public async Task<T> ReadItem<T>(string key) where T: class, new()
        {
            var table = Table.LoadTable(amazonDynamoDBClient, awsDynamoDBOptions.TableName);
            var type = typeof(T);
            var properties = type.GetProperties();

            var config = new GetItemOperationConfig
            {
                AttributesToGet = properties.Select(property => property.Name).ToList(),
                ConsistentRead = true
            };

            var document = await table.GetItemAsync(key, config);

            var obj = new T();
            foreach(var property in properties)
            {
                if (property.PropertyType.IsEnum)
                {
                    property.SetValue(obj, Enum.Parse(property.PropertyType, document[property.Name].AsString()));
                }
                else
                {
                    property.SetValue(obj, document[property.Name].AsType(property.PropertyType));
                }
            }
            return obj;
        }

        /// <summary>
        /// write item to the table
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="obj"></param>
        /// <returns></returns>
        public async Task WriteItem<T>(T obj) where T : class, new()
        {
            var table = Table.LoadTable(amazonDynamoDBClient, awsDynamoDBOptions.TableName);
            var document = new Document();
            var type = typeof(T);
            var properties = type.GetProperties();
            foreach (var property in properties)
            {
                if(property.PropertyType == DynamoDBEntryExtensions.boolType)
                {
                    var value = new DynamoDBBool((bool)property.GetValue(obj));
                    document[property.Name] = value;
                }
                else if (property.PropertyType.IsEnum)
                {
                    // enum as string
                    var value = Enum.GetName(property.PropertyType, property.GetValue(obj));
                    document[property.Name] = value;
                }
                else
                {
                    dynamic value = property.GetValue(obj);
                    document[property.Name] = value;
                }
            }
            await table.PutItemAsync(document);
        }
    }
}
