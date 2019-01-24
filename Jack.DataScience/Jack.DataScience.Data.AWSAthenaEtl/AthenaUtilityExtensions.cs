using System;
using System.IO.Compression;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;
using System.Linq;
using System.Threading.Tasks;
using CsvHelper;
using CsvHelper.Configuration;
using System.Collections.Generic;
using Jack.DataScience.Data.CSV;
using Jack.DataScience.Data.Converters;
using Jack.DataScience.Data.Parquet;
using Jack.DataScience.Storage.SFTP;
using Jack.DataScience.Storage.AWSS3;
using Jack.DataScience.Data.AWSAthena;
using Renci.SshNet;
using Renci.SshNet.Sftp;
using Newtonsoft.Json;

namespace Jack.DataScience.Data.AWSAthenaEtl
{
    public static class AthenaUtilityExtensions
    {
        private static readonly Regex regexAthena = new Regex(@"^[\w_]+$");
        public static async Task<bool> CreateAthenaTable(this EtlSettings etlSettings, AWSAthenaAPI awsAthenaAPI)
        {
            //create athena database if not exists

            if (etlSettings.AthenaDatabaseName == null || !regexAthena.IsMatch(etlSettings.AthenaDatabaseName))
            {
                throw new Exception($@"Invalid Athena Database Name '{etlSettings.AthenaDatabaseName}'");
            };
            if (etlSettings.AthenaTableName == null || !regexAthena.IsMatch(etlSettings.AthenaTableName))
            {
                throw new Exception($@"Invalid Athena Table Name '{etlSettings.AthenaDatabaseName}'");
            }
            if (etlSettings.Mappings == null || etlSettings.Mappings.Count == 0)
            {
                throw new Exception($@"No Fields found for ETL Setting '{etlSettings.Name}'");
            }
            await awsAthenaAPI.ExecuteQuery($@"create database if not exists `{etlSettings.AthenaDatabaseName}`");

            // drop the table if it exists
            await awsAthenaAPI.ExecuteQuery($@"drop table if exists `{etlSettings.AthenaDatabaseName}`.`{etlSettings.AthenaTableName}`");

            var query = $@"CREATE EXTERNAL TABLE IF NOT EXISTS `{etlSettings.AthenaDatabaseName}`.`{etlSettings.AthenaTableName}`(
{etlSettings.MapAthenaFields()}
)
PARTITIONED BY (
    `{etlSettings.DatePartitionKey}` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = '1'
)
LOCATION 's3://{etlSettings.TargetS3BucketName}/{etlSettings.TargetS3Prefix}/'
";

            await awsAthenaAPI.ExecuteQuery(query);
            return true;
        }
        private static string MapAthenaFields(this EtlSettings etlSettings)
        {
            return string.Join(",\n", etlSettings.Mappings.Select(field => $"`{field.MappedName}` {field.MapAthenaField()}"));
        }

        private static string MapAthenaField(this FieldMapping fieldMapping)
        {
            switch (fieldMapping.MappedType)
            {
                case AthenaTypeEnum.athena_string:
                    return "string";
                case AthenaTypeEnum.athena_integer:
                    return "int";
                case AthenaTypeEnum.athena_boolean:
                    return "BOOLEAN";
                case AthenaTypeEnum.athena_bigint:
                    return "bigint";
                case AthenaTypeEnum.athena_smallint:
                    return "smallint";
                case AthenaTypeEnum.athena_tinyint:
                    return "tinyint";
                case AthenaTypeEnum.athena_double:
                    return "double";
                case AthenaTypeEnum.athena_float:
                    return "float";
                case AthenaTypeEnum.athena_timestamp:
                    return "timestamp";
                case AthenaTypeEnum.athena_date:
                    return "date";
            }
            throw new Exception($"Unexpected Athena Type '{fieldMapping.MappedType}' in the field mapping '{fieldMapping.SourceFieldName}'");
        }

        public static AthenaTypeEnum DetectedTypeToAthenaType(this string type)
        {
            switch (type)
            {
                case "string":
                    return AthenaTypeEnum.athena_string;
                case "int":
                    return AthenaTypeEnum.athena_integer;
                case "double":
                    return AthenaTypeEnum.athena_double;
                case "bool":
                    return AthenaTypeEnum.athena_boolean;
            }
            throw new Exception("Unexpected detected type for athena type conversion.");
        }
    }
}
