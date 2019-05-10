using System;
using System.IO;
using System.Linq;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Jack.DataScience.Data.AWSAthena;
using Amazon.Athena.Model;
using Jack.DataScience.Storage.AWSS3;

namespace Jack.DataScience.Data.AWSAthenaEtl
{
    public static class AthenaQueryExtensions
    {
        public static async Task GetAthenaQueryResultSampleByDate(this EtlSettings etlSettings, int lines)
        {
            var athena = etlSettings.AthenaQuerySource;
            if (athena == null) throw new Exception("The ETL has an empty Athena source setting.");
            var athenaApi = etlSettings.CreateSourceAthenaAPI();

            var query = athena.AthenaSQL;
            var today = DateTime.Now;
            var date = today.AddDays(-athena.DaysAgo);
            query = query.Replace("{date}", date.ToString(athena.DateFormat));
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

        public static async Task<List<string>> TransferAthenaQueryResultByDate(this EtlSettings etlSettings, AWSAthenaAPI awsAthenaAPI)
        {
            var result = new List<string>();
            var athena = etlSettings.AthenaQuerySource;
            if (athena == null) throw new Exception("The ETL has an empty Athena source setting.");
            var athenaApi = etlSettings.CreateSourceAthenaAPI();

            var query = athena.AthenaSQL;
            var today = DateTime.Now;
            var date = today.AddDays(-athena.DaysAgo);
            query = query.Replace("{date}", date.ToString(athena.DateFormat));
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
    }
}
