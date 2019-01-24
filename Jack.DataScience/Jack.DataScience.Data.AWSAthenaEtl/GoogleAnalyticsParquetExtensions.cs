using System;
using System.Reflection;
using System.Linq;
using System.IO;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using Parquet;
using Parquet.Data;
using Google.Cloud.BigQuery.V2;
using Google.Apis.Bigquery.v2.Data;


namespace Jack.DataScience.Data.AWSAthenaEtl
{
    public static class GoogleAnalyticsParquetExtensions
    {
        private static readonly Type DataFieldGenericType = typeof(DataField<>);
        private static readonly MethodInfo CastMethodGeneric = typeof(Enumerable).GetMethod("Cast");
        private static readonly MethodInfo ToArrayMethodGeneric = typeof(Enumerable).GetMethod("ToArray");
        private static readonly Type[] DataFieldConstructorGenericArguments = new Type[] { typeof(string) };
        private static readonly Type DateTimeType = typeof(DateTime);


        public static List<FieldMapping> ToFieldMappings(this BigQueryResults queryResults)
        {
            var mappings = new List<FieldMapping>();
            return queryResults.Schema.Fields.Select(field =>
            {
                switch (field.Type)
                {
                    case "STRING":
                    case "TIMESTAMP":
                    case "BYTES":
                        {
                            return new FieldMapping()
                            {
                                SourceFieldName = field.Name,
                                MappedName = field.Name,
                                MappedType = AthenaTypeEnum.athena_string
                            };
                        }
                    case "INTEGER":
                    case "INT64":
                        {
                            return new FieldMapping()
                            {
                                SourceFieldName = field.Name,
                                MappedName = field.Name,
                                MappedType = AthenaTypeEnum.athena_bigint
                            };
                        }
                    case "FLOAT":
                    case "FLOAT64":
                        {
                            return new FieldMapping()
                            {
                                SourceFieldName = field.Name,
                                MappedName = field.Name,
                                MappedType = AthenaTypeEnum.athena_double
                            };
                        }
                    case "BOOL":
                    case "BOOLEAN":
                        {
                            return new FieldMapping()
                            {
                                SourceFieldName = field.Name,
                                MappedName = field.Name,
                                MappedType = AthenaTypeEnum.athena_boolean
                            };
                        }
                    case "DATE":
                        {
                            return new FieldMapping()
                            {
                                SourceFieldName = field.Name,
                                MappedName = field.Name,
                                MappedType = AthenaTypeEnum.athena_integer
                            };
                        }
                    case "TIME":
                        {
                            return new FieldMapping()
                            {
                                SourceFieldName = field.Name,
                                MappedName = field.Name,
                                MappedType = AthenaTypeEnum.athena_integer
                            };
                        }
                    case "DATETIME":
                        {
                            return new FieldMapping()
                            {
                                SourceFieldName = field.Name,
                                MappedName = field.Name,
                                MappedType = AthenaTypeEnum.athena_string
                            };
                        }
                    case "RECORD":
                        {
                            throw new Exception($"Google Big Query 'RECORD' type is currently not supported in the ETL tool.");
                        }
                }
                return new FieldMapping();
            }).ToList();
        }

        private static DataColumn ToParquetColumn(this TableFieldSchema field, List<FieldMapping> mappings, int index, IEnumerable<BigQueryRow> rows)
        {
            var foundMapping = mappings.FirstOrDefault(m => m.SourceFieldName.ToLower() == field.Name.ToLower());

            var mappedName = field.Name;
            if(foundMapping != null)
            {
                mappedName = foundMapping.MappedName;
            }

            switch (field.Type)
            {
                case "STRING":
                    {
                        return new DataColumn(new DataField<string>(mappedName), rows.Select(r => r[index] as string).ToArray());
                    }
                case "TIMESTAMP":
                    {
                        return new DataColumn(new DataField<string>(mappedName), rows.Select(r => r[index].ToString()).ToArray());
                    }
                case "BYTES":
                    {
                        return new DataColumn(new DataField<string>(mappedName), rows.Select(r =>
                        {
                            var bytes = r[index] as byte[];
                            if (bytes == null)
                            {
                                return Convert.ToBase64String(new byte[] { });
                            }
                            else
                            {
                                return Convert.ToBase64String(bytes);
                            }
                        }).ToArray());
                    }
                case "INTEGER":
                case "INT64":
                    {
                        return new DataColumn(new DataField<long>(mappedName), rows.Select(r => (long)r[index]).ToArray());
                    }
                case "FLOAT":
                case "FLOAT64":
                    {
                        return new DataColumn(new DataField<double>(mappedName), rows.Select(r => (double)r[index]).ToArray());
                    }
                case "BOOL":
                case "BOOLEAN":
                    {
                        return new DataColumn(new DataField<bool>(mappedName), rows.Select(r => (bool)r[index]).ToArray());
                    }
                case "DATE":
                    {
                        return new DataColumn(new DataField<string>(mappedName), rows.Select(r => ((DateTime)r[index]).ToString("o")).ToArray());
                    }
                case "TIME":
                    {
                        return new DataColumn(new DataField<string>(mappedName), rows.Select(r => ((DateTime)r[index]).ToString("o")).ToArray());
                    }
                case "DATETIME":
                    {
                        return new DataColumn(new DataField<string>(mappedName), rows.Select(r => ((DateTime)r[index]).ToString("o")).ToArray());
                    }
                case "RECORD":
                    {
                        throw new Exception($"Google Big Query 'RECORD' type is currently not supported in the ETL tool.");
                    }
                default:
                    {
                        throw new Exception($"Google Big Query Unexpected type");
                    }
            }
        }

        public static void WriteGARowsAsParquet(this Stream stream, TableSchema tableSchema, List<FieldMapping> mappings, IEnumerable<BigQueryRow> rows)
        {

            List<DataColumn> columns = new List<DataColumn>();

            int index = 0;
            foreach (var field in tableSchema.Fields)
            {
                columns.Add(field.ToParquetColumn(mappings, index, rows));
                index++;
            }

            Schema schema = new Schema(new ReadOnlyCollection<Field>(columns.Select(column => column.Field).ToArray()));

            using (ParquetWriter writer = new ParquetWriter(schema, stream))
            {
                writer.CompressionMethod = CompressionMethod.Snappy;
                using (ParquetRowGroupWriter rowGroupWriter = writer.CreateRowGroup())
                {
                    foreach (var column in columns)
                    {
                        rowGroupWriter.WriteColumn(column);
                    }
                }
            }
        }
    }
}
