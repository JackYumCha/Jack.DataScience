using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using Snowflake.Data.Client;
using Snowflake.Data.Configuration;
using Snowflake.Data.Core;
using sf = Snowflake;

namespace Jack.DataScience.Data.Snowflake
{
    public class SnowflakeAPI
    {
        private readonly string ConnectionString;
        public SnowflakeAPI()
        {
            ConnectionString = Environment.GetEnvironmentVariable("SnowflakeConnectionString");
        }
        public SnowflakeAPI(SnowflakeOptions snowflakeOptions)
        {
            ConnectionString = snowflakeOptions.ConnectionString;
        }

        private static object[] emptyArguments = new object[0];
        public async Task<List<T>> Query<T>(string query, params DbParameter[] parameters) where T: class, new()
        {
            using (SnowflakeDbConnection connection = new SnowflakeDbConnection())
            {
                connection.ConnectionString = ConnectionString;
                connection.Open();
                SnowflakeDbCommand command = new SnowflakeDbCommand(connection);
                command.CommandText = query;
                foreach (var parameter in parameters) command.Parameters.Add(parameter);
                using (var reader = command.ExecuteReader())
                {
                    var type = typeof(T);
                    var properties = type.GetProperties()
                        .ToDictionary(p => p.Name.ToLower(), p => p);
                    List<T> results = new List<T>();
                    while(await reader.ReadAsync())
                    {
                        T item = new T();
                        for(int i = 0; i < reader.FieldCount; i++)
                        {
                            string columnName = reader.GetName(i).ToLower();
                            if (!properties.ContainsKey(columnName)) continue;
                            var property = properties[columnName];
                            var valueType = reader.GetFieldType(i);
                            if(valueType != property.PropertyType)
                            {
                                throw new Exception($"Query Deserialization Error: Column '{reader.GetName(i)}' type '{valueType.FullName}' does now match PropertyType '{property.PropertyType.FullName}' of '{type.FullName}->{property.Name}'");
                            }
                            if (valueType == typeof(string))
                                property.SetValue(item, await reader.GetFieldValueAsync<string>(i), emptyArguments);
                            else if(valueType == typeof(int))
                                property.SetValue(item, await reader.GetFieldValueAsync<int>(i), emptyArguments);
                            else if (valueType == typeof(uint))
                                property.SetValue(item, await reader.GetFieldValueAsync<uint>(i), emptyArguments);
                            else if (valueType == typeof(long))
                                property.SetValue(item, await reader.GetFieldValueAsync<long>(i), emptyArguments);
                            else if (valueType == typeof(ulong))
                                property.SetValue(item, await reader.GetFieldValueAsync<ulong>(i), emptyArguments);
                            else if (valueType == typeof(bool))
                                property.SetValue(item, await reader.GetFieldValueAsync<bool>(i), emptyArguments);
                            else if (valueType == typeof(char))
                                property.SetValue(item, await reader.GetFieldValueAsync<char>(i), emptyArguments);
                            else if (valueType == typeof(DateTime))
                                property.SetValue(item, await reader.GetFieldValueAsync<DateTime>(i), emptyArguments);
                            else if (valueType == typeof(byte[]))
                                property.SetValue(item, await reader.GetFieldValueAsync<byte[]>(i), emptyArguments);
                            else if (valueType == typeof(char[]))
                                property.SetValue(item, await reader.GetFieldValueAsync<char[]>(i), emptyArguments);
                        }
                        results.Add(item);
                    }
                    return results;
                }
            }
        }

        public async Task<List<List<object>>> QueryAsLists(string query, params DbParameter[] parameters)
        {
            using (SnowflakeDbConnection connection = new SnowflakeDbConnection())
            {
                connection.ConnectionString = ConnectionString;
                connection.Open();
                SnowflakeDbCommand command = new SnowflakeDbCommand(connection);
                command.CommandText = query;
                foreach (var parameter in parameters) command.Parameters.Add(parameter);
                using (var reader = command.ExecuteReader())
                {
                    List<List<object>> results = new List<List<object>>();
                    int index = 0;
                    while (await reader.ReadAsync())
                    {
                        if(index == 0)
                        {
                            List<object> headers = new List<object>();
                            List<object> types = new List<object>();
                            for (int i = 0; i < reader.FieldCount; i++)
                            {
                                headers.Add(reader.GetName(i));
                                types.Add(reader.GetFieldType(i));
                            }
                            results.Add(headers);
                            results.Add(types);
                        }
                        List<object> item = new List<object>();
                        for (int i = 0; i < reader.FieldCount; i++)
                            item.Add(reader.GetValue(i));
                        results.Add(item);
                        index++;
                    }
                    return results;
                }
            }
        }
    }
}
