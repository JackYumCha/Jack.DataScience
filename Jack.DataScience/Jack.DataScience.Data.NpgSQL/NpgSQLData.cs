using System;
using System.Text;
using System.Diagnostics;
using System.Reflection;
using System.Linq;
using System.Linq.Expressions;
using System.Collections.Generic;
using Jack.DataScience.Common;
using Jack.DataScience.DataTypes;
using Npgsql;

namespace Jack.DataScience.Data.NpgSQL
{
    public class NpgSQLData: IDisposable
    {
        private readonly PostgreSQLOptions postgreSQLOptions;
        private readonly NpgsqlConnection npgsqlConnection;
        public NpgSQLData(PostgreSQLOptions postgreSQLOptions)
        {
            this.postgreSQLOptions = postgreSQLOptions;
            npgsqlConnection = new NpgsqlConnection(this.postgreSQLOptions.ConnectionString);
            npgsqlConnection.Open();
        }

        public void Dispose()
        {
            npgsqlConnection.Close();
            npgsqlConnection.Dispose();
        }

        public NpgsqlConnection Connection
        {
            get => npgsqlConnection;
        }

        public void CreateTable<T>(string suffix = null) where T: class, new()
        {
            var schema = TypeSchema.GetSchema<T>();
            var sql = schema.TableDefinition(suffix);
            var cmdCreate = new NpgsqlCommand(sql, npgsqlConnection);
            cmdCreate.ExecuteNonQuery();
            var indices = schema.TableIndices(suffix);
            foreach(var sqlIndex in indices)
            {
                var cmdIndex = new NpgsqlCommand(sqlIndex, npgsqlConnection);
                cmdIndex.ExecuteNonQuery();
            }
        }

        public void DropTable<T>(string suffix = null) where T : class, new()
        {
            var schema = TypeSchema.GetSchema<T>();
            string tableName = schema.TableNameWithSuffix(suffix);
            var sql = $"DROP TABLE {tableName};";
            var cmd = new NpgsqlCommand(sql, npgsqlConnection);
            try
            {
                cmd.ExecuteNonQuery();
            }
            catch(PostgresException ex)
            {
                if(ex.ErrorCode != -2147467259)
                {
                    throw ex;
                }
            }
            
        }

        public IEnumerable<T> Search<T>(
        IEnumerable<string> properties,
        string query,
        params NpgsqlParameter[] parameters) where T : class, new()
        {
            return Search<T>(null, properties, query, parameters);
        }

        /// <summary>
        /// Query the Table with the Specified SQL condition and return as IEnumerable
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="properties"></param>
        /// <param name="query"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public IEnumerable<T> Search<T>(string suffix, 
            IEnumerable<string> properties, 
            string query,
            params NpgsqlParameter[] parameters) where T: class, new()
        {
            var schema = TypeSchema.GetSchema<T>();
            string tableName = schema.TableNameWithSuffix(suffix);

            var fields = schema.Fields.ToList();
            if (properties != null && properties.Any())
            {
                fields = properties
                    .Select(p => schema.GetField(p))
                    .Where(f => f != null)
                    .ToList();
            }

            var cmd = new NpgsqlCommand($@"SELECT 
{(string.Join(",\n", fields.Select(f => f.ReadExpression)))}
FROM {tableName}
{((query != null && query.Length > 0)?"WHERE":"")} {query};", npgsqlConnection);

            foreach(var para in parameters)
            {
                cmd.Parameters.Add(para);
            }

            using (var reader = cmd.ExecuteReader())
            {
                while (reader.Read())
                {

                    int index = 0;
                    T obj = new T();
                    foreach (var f in fields)
                    {
                        obj.WriteField(schema, f.Name, reader, index);
                        index += 1;
                    }
                    yield return obj;
                }
            }
        }

        public long Count<T>(string query,
           params NpgsqlParameter[] parameters) where T : class, new()
        {
            return Count<T>(null, query, parameters);
        }

        public long Count<T>(string suffix,
           string query,
           params NpgsqlParameter[] parameters) where T : class, new()
        {
            var schema = TypeSchema.GetSchema<T>();
            string tableName = schema.TableNameWithSuffix(suffix);

            var fields = schema.Fields.ToList();
            var keyField = fields.Where(f => f.IsPrimaryKey).First();

            var cmd = new NpgsqlCommand($@"SELECT 
COUNT({keyField.Name})
FROM {tableName}
{((query != null && query.Length > 0) ? "WHERE" : "")} {query};", npgsqlConnection);

            foreach (var para in parameters)
            {
                cmd.Parameters.Add(para);
            }

            using (var reader = cmd.ExecuteReader())
            {
                while (reader.Read())
                {
                    return reader.GetInt64(0);
                }
            }
            throw new Exception($"The Count Query did not generate any results.");
        }

        public int Insert<T>(IEnumerable<string> properties, IEnumerable<T> values) where T : class, new()
        {
            return Insert<T>(null, properties, values);
        }

        public int Insert<T>(string suffix, IEnumerable<string> properties, IEnumerable<T> values) where T : class, new()
        {
            var schema = TypeSchema.GetSchema<T>();
            string tableName = schema.TableNameWithSuffix(suffix);

            var fields = schema.Fields.ToList();

            if(properties != null && properties.Any())
            {
                fields = properties
                    .Select(p => schema.GetField(p))
                    .Where(f => f != null)
                    .ToList();
            }

            int count = 0;
            foreach(var value in values)
            {
                // build value dict
                Dictionary<TypeFieldSchema, object> dataDict = new Dictionary<TypeFieldSchema, object>();

                // this will ignore the null values
                foreach (var field in fields)
                {
                    var data = value.ReadField(schema, field.Name);
                    if(data != null)
                    {
                        dataDict.Add(field, data);
                    }
                }


                var cmd = new NpgsqlCommand($@"INSERT INTO {tableName} (
{string.Join(",\n", dataDict.Keys.Select(f => f.Name))}
)
VALUES(
{string.Join(",\n", dataDict.Keys.Select(f => f.WriteParameterName))}
);", npgsqlConnection);


                foreach(var field in dataDict.Keys)
                {
                    cmd.Parameters.AddWithValue(field.ParameterName, dataDict[field]);
                }
                cmd.ExecuteNonQuery();
                count += 1;
            }
            return count;
        }

        public List<TSerial> InsertReturningSerial<T, TSerial>(IEnumerable<string> properties, IEnumerable<T> values) where T : class, new() where TSerial : struct
        {
            return InsertReturningSerial<T, TSerial>(null, properties, values);
        }

        public List<TSerial> InsertReturningSerial<T, TSerial>(string suffix, IEnumerable<string> properties, IEnumerable<T> values) where T : class, new() where TSerial: struct
        {

            List<TSerial> result = new List<TSerial>();
            int count = values.Count();
            if (count == 0)
            {
                return result;
            }

            var schema = TypeSchema.GetSchema<T>();
            string tableName = schema.TableNameWithSuffix(suffix);

            var fields = schema.Fields.Where(f => !f.IsPrimaryKey).ToList();

            var primaryKey = schema.Fields.First(f => f.IsPrimaryKey);

            if (properties != null && properties.Any())
            {
                fields = properties
                    .Select(p => schema.GetField(p))
                    .Where(f => f != null)
                    .ToList();
            }

            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.Append($@"INSERT INTO {tableName} (
{string.Join(",\n", fields.Select(f => f.Name))}
)
VALUES ");
            int index = 0;
            List<NpgsqlParameter> parameters = new List<NpgsqlParameter>();
            foreach (var value in values)
            {

                sqlBuilder.Append($@"({string.Join(",", fields.Select(f => $"{f.ParameterName}{index}"))})");
                foreach (var f in fields)
                {
                    parameters.Add(new NpgsqlParameter($"{f.ParameterName}{index}", value.ReadField(schema, f.Name)));
                }
                index += 1;
                if(index < count)
                {
                    sqlBuilder.Append(",");
                }
            }

            sqlBuilder.Append($" RETURNING {primaryKey.Name}");

            var cmd = new NpgsqlCommand(sqlBuilder.ToString(), npgsqlConnection);
            foreach(var parameter in parameters)
            {
                cmd.Parameters.Add(parameter);
            }

            using(var reader = cmd.ExecuteReader())
            {
                while (reader.Read())
                {
                    result.Add(reader.GetFieldValue<TSerial>(0));
                }

            }
            return result;
        }

        public bool InsertMany<T>(IEnumerable<string> properties, IEnumerable<T> values) where T : class, new()
        {
            return InsertMany(null, properties, values);
        }

        public bool InsertMany<T>(string suffix, IEnumerable<string> properties, IEnumerable<T> values) where T : class, new()
        {

            int count = values.Count();
            if (count == 0)
            {
                return false;
            }

            var schema = TypeSchema.GetSchema<T>();
            string tableName = schema.TableNameWithSuffix(suffix);

            var fields = schema.Fields
                .Where(f => !(f.FieldType == FieldTypeEnum.BigSerial || f.FieldType == FieldTypeEnum.Serial ))
                .ToList();

            if (properties != null && properties.Any())
            {
                fields = properties
                    .Select(p => schema.GetField(p))
                    .Where(f => f != null)
                    .ToList();
            }

            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.Append($@"INSERT INTO {tableName} (
{string.Join(",\n", fields.Select(f => f.Name))}
)
VALUES ");
            int index = 0;
            List<NpgsqlParameter> parameters = new List<NpgsqlParameter>();
            foreach (var value in values)
            {

                sqlBuilder.Append($@"({string.Join(",", fields.Select(f => $"{f.ParameterName}{index}"))})");
                foreach (var f in fields)
                {
                    parameters.Add(new NpgsqlParameter($"{f.ParameterName}{index}", value.ReadField(schema, f.Name)));
                }
                index += 1;
                if (index < count)
                {
                    sqlBuilder.Append(",");
                }
            }

            var cmd = new NpgsqlCommand(sqlBuilder.ToString(), npgsqlConnection);
            foreach (var parameter in parameters)
            {
                cmd.Parameters.Add(parameter);
            }
            cmd.ExecuteNonQuery();
            return true;
        }

        public bool ExistsByKey<T>(T value) where T : class, new()
        {
            return ExistsByKey(null, value);
        }

        public bool ExistsByKey<T>(string suffix, T value) where T:class, new()
        {
            var schema = TypeSchema.GetSchema<T>();
            var keyField = schema.Fields.Where(f => f.IsPrimaryKey).First();
            var tableName = schema.TableNameWithSuffix(suffix);
            var cmd = new NpgsqlCommand($"SELECT COUNT({keyField.Name}) FROM {tableName} WHERE {keyField.Name}={keyField.ParameterName}", npgsqlConnection);
            cmd.Parameters.AddWithValue(keyField.ParameterName, value.ReadField(schema, keyField.Name));
            using (var reader = cmd.ExecuteReader())
            {
                if (reader.Read())
                {
                    return reader.GetInt64(0) > 0;
                }
            }
            return false;
        }

        public void Upsert<T>(IEnumerable<string> properties, IEnumerable<T> values) where T : class, new()
        {
            Upsert(null, properties, values);
        }

        public void Upsert<T>(string suffix, IEnumerable<string> properties, IEnumerable<T> values) where T : class, new()
        {
            var schema = TypeSchema.GetSchema<T>();
            
            var fields = schema.Fields.ToList();
            var primaryKey = fields.Where(f => f.IsPrimaryKey).First();

            if (properties != null && properties.Any())
            {
                fields = properties
                    .Select(p => schema.GetField(p))
                    .Where(f => f != null)
                    .ToList();
            }

            List<T> valuesToInsert = new List<T>();
            var updateFields = fields.Where(f => f.Name != primaryKey.Name).Select(f => f.Name).ToList();

            foreach (var value in values)
            {
                var found = Search<T>(suffix,
                new List<string>() { primaryKey.Name },
                $"{primaryKey.Name}={primaryKey.ParameterName}",
                new NpgsqlParameter(primaryKey.ParameterName, value.ReadField(schema, primaryKey.Name)));
                if(found.Any())
                {
                    //update
                    Update(suffix, updateFields, value,
                        $"{primaryKey.Name}={primaryKey.ParameterName}",
                        new NpgsqlParameter(primaryKey.ParameterName, value.ReadField(schema, primaryKey.Name)));
                }
                else
                {
                    valuesToInsert.Add(value);
                }
            }
            Insert(suffix, properties, valuesToInsert);
        }

        public void Update<T>(IEnumerable<string> properties, T value, string query, params NpgsqlParameter[] parameters) where T : class, new()
        {
            Update(null, properties, value, query, parameters);
        }

        public void Update<T>(string suffix, IEnumerable<string> properties, T value, string query, params NpgsqlParameter[] parameters) where T : class, new()
        {
            var schema = TypeSchema.GetSchema<T>();
            string tableName = schema.TableNameWithSuffix(suffix).ToLower();

            var fields = schema.Fields.ToList();

            if (properties != null && properties.Any())
            {
                fields = properties
                    .Select(p => schema.GetField(p))
                    .Where(f => f != null)
                    .ToList();
            }
            bool multipleColumn = fields.Count > 1;
            var cmd = new NpgsqlCommand($@"UPDATE {tableName}
SET
{string.Join(",",fields.Select(f => f.Name)).MultipleColumnEscapeForUpdateSetter(multipleColumn)} = {string.Join(",", fields.Select(f => f.WriteParameterName)).MultipleColumnEscapeForUpdateSetter(multipleColumn)}
{((query != null && query.Length > 0) ? "WHERE" : "")} {query}
", npgsqlConnection);

            foreach(var field in fields)
            {
                cmd.Parameters.AddWithValue(field.WriteParameterName, value.ReadField(schema, field.Name));
            }

            foreach (var para in parameters)
            {
                cmd.Parameters.Add(para);
            }

            cmd.ExecuteNonQuery();
        }
        public void Delete<T>( string query, params NpgsqlParameter[] parameters) where T : class, new()
        {
            Delete<T>(null, query, parameters);
        }
        public void Delete<T>(string suffix,  string query, params NpgsqlParameter[] parameters) where T: class, new()
        {
            var schema = TypeSchema.GetSchema<T>();
            string tableName = schema.TableNameWithSuffix(suffix).ToLower();

            var cmd = new NpgsqlCommand($@"DELETE FROM {tableName}
{((query != null && query.Length > 0) ? "WHERE" : "")} {query}
", npgsqlConnection);

            foreach (var para in parameters)
            {
                cmd.Parameters.Add(para);
            }

            cmd.ExecuteNonQuery();
        }

        /// <summary>
        /// this will list all the table names, but they are in lowercase.
        /// </summary>
        /// <returns></returns>
        public IEnumerable<string> ListTables()
        {
            var cmd = new NpgsqlCommand($"SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = 'public'", npgsqlConnection);
            using(var reader = cmd.ExecuteReader())
            {
                while (reader.Read())
                {
                    yield return reader.GetString(0);
                }
            }
            yield break;
        }

        public IEnumerable<string> ListSuffixTables<T>() where T: class, new()
        {
            var schema = TypeSchema.GetSchema<T>();
            var cmd = new NpgsqlCommand($@"SELECT tablename 
FROM pg_catalog.pg_tables 
WHERE schemaname = 'public' AND tablename LIKE '{schema.Name.ToLower()}\_\_%'", npgsqlConnection);
            using (var reader = cmd.ExecuteReader())
            {
                while (reader.Read())
                {
                    yield return reader.GetString(0);
                }
            }
            yield break;
        }

        public int DropAllSuffixTables<T>() where T : class, new()
        {
            var tableNames = ListSuffixTables<T>().ToList();
            foreach(var tableName in tableNames)
            {
                var sql = $"DROP TABLE {tableName};";
                var cmd = new NpgsqlCommand(sql, npgsqlConnection);
                cmd.ExecuteNonQuery();
            }
            return tableNames.Count;
        }

        public void ClearTable<T>(string suffix = null) where T : class, new()
        {
            var schema = TypeSchema.GetSchema<T>();
            string tableName = schema.TableNameWithSuffix(suffix).ToLower();
            var sql = $"DELETE FROM {tableName} WHERE true;";
            var cmd = new NpgsqlCommand(sql, npgsqlConnection);
            cmd.ExecuteNonQuery();
        }

        public bool TableExists<T>(string suffix = null) where T: class, new()
        {
            var schema = TypeSchema.GetSchema<T>();
            string tableName = schema.TableNameWithSuffix(suffix).ToLower();
            var cmd = new NpgsqlCommand($@"SELECT COUNT(tablename) 
FROM pg_catalog.pg_tables 
WHERE schemaname = 'public' AND tablename = '{tableName}'", 
npgsqlConnection);
            using(var reader= cmd.ExecuteReader())
            {
                while (reader.Read())
                {
                    return reader.GetInt64(0) == 1;
                }
            }
            throw new Exception($"Error in Checking Table Existence for '{tableName}'");
        }


    }

}
