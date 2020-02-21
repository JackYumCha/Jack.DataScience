using System;
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
        public async void Query(string query, params object[] parameters)
        {
            using (SnowflakeDbConnection connection = new SnowflakeDbConnection())
            {
                connection.ConnectionString = ConnectionString;
                SnowflakeDbCommand command = new SnowflakeDbCommand(connection);
                command.CommandText = query;
                foreach (var parameter in parameters) command.Parameters.Add(parameter);
                using (var reader = command.ExecuteReader())
                {
                    var read = await reader.ReadAsync();
                    //reader.FieldCount
                }
            }
        }
    }
}
