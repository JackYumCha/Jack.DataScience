using System;
using System.Linq;
using System.IO;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using Jack.DataScience.Data.Converters;
using Jack.DataScience.Data.Parquet;
using Parquet;
using Parquet.Data;
using MvcAngular;
using Amazon.Athena.Model;
using Jack.DataScience.Data.AWSAthena;

namespace Jack.DataScience.Data.AWSAthenaEtl
{
    public static class AthenaParquetExtensions
    {
        public static void WriteParquet(this Stream stream, List<ParquetField> parquetFields, List<List<string>> data)
        {
            List<DataColumn> columns = new List<DataColumn>();

            for (int i = 0; i < parquetFields.Count; i ++)
            {
                var field = parquetFields[i];
                var column = data.Select(row => row == null ? null : ( row.Count > i ? row[i]: null)).ToList();
                switch (field.AthenaType)
                {
                    case AthenaTypeEnum.athena_string:
                        {
                            columns.Add(new DataColumn(new DataField<string>(field.Name), column.Cast<string>().ToArray()));
                        }
                        break;
                    case AthenaTypeEnum.athena_integer:
                        {
                            columns.Add(new DataColumn(new DataField<int>(field.Name), column.Select(item => item.As<int>()).Cast<int>().ToArray()));
                        }
                        break;
                    case AthenaTypeEnum.athena_bigint:
                        {
                            columns.Add(new DataColumn(new DataField<long>(field.Name), column.Select(item => item.As<long>()).Cast<long>().ToArray()));
                        }
                        break;
                    case AthenaTypeEnum.athena_smallint:
                        {
                            columns.Add(new DataColumn(new DataField<short>(field.Name), column.Select(item => item.As<short>()).Cast<short>().ToArray()));
                        }
                        break;
                    case AthenaTypeEnum.athena_tinyint:
                        {
                            columns.Add(new DataColumn(new DataField<byte>(field.Name), column.Select(item => item.As<byte>()).Cast<byte>().ToArray()));
                        }
                        break;
                    case AthenaTypeEnum.athena_boolean:
                        {
                            columns.Add(new DataColumn(new DataField<bool>(field.Name), column.Select(item => item.As<bool>()).Cast<bool>().ToArray()));
                        }
                        break;
                    case AthenaTypeEnum.athena_double:
                        {
                            columns.Add(new DataColumn(new DataField<double>(field.Name), column.Select(item => item.As<double>()).Cast<double>().ToArray()));
                        }
                        break;
                    case AthenaTypeEnum.athena_varchar:
                        {
                            columns.Add(new DataColumn(new DataField<string>(field.Name), column.Select(item => item.As<string>()).Cast<string>().ToArray()));
                        }
                        break;
                }
            }
            stream.WriteParquetColumns(columns);
        }


        public static void WriteAthenaRowsAsParquet(this Stream stream, ResultSetMetadata tableSchema, List<FieldMapping> mappings, IEnumerable<Row> rows)
        {

            List<DataColumn> columns = new List<DataColumn>();

            int index = 0;
            foreach (var column in tableSchema.ColumnInfo)
            {
                columns.Add(column.ToParquetColumn(mappings, index, rows));
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

        private static DataColumn ToParquetColumn(this ColumnInfo field, List<FieldMapping> mappings, int index, IEnumerable<Row> rows)
        {
            var foundMapping = mappings.FirstOrDefault(m => m.SourceFieldName.ToLower() == field.Name.ToLower());

            var mappedName = field.Name;
            if (foundMapping != null)
            {
                mappedName = foundMapping.MappedName;
            }

            switch (field.Type)
            {
                case AthenaDataTypes.@string:
                case AthenaDataTypes.varchar:
                    return new DataColumn(new DataField<string>(mappedName), rows.Select(r => r.Data[index].VarCharValue.As<string>()).Cast<string>().ToArray());
                case AthenaDataTypes.tinyint:
                    return new DataColumn(new DataField<byte>(mappedName), rows.Select(r => r.Data[index].VarCharValue.As<byte>()).Cast<byte>().ToArray());
                case AthenaDataTypes.smallint:
                    return new DataColumn(new DataField<short>(mappedName), rows.Select(r => r.Data[index].VarCharValue.As<short>()).Cast<short>().ToArray());
                case AthenaDataTypes.integer:
                    return new DataColumn(new DataField<int>(mappedName), rows.Select(r => r.Data[index].VarCharValue.As<int>()).Cast<int>().ToArray());
                case AthenaDataTypes.bigint:
                    return new DataColumn(new DataField<long>(mappedName), rows.Select(r => r.Data[index].VarCharValue.As<long>()).Cast<long>().ToArray());
                case AthenaDataTypes.@double:
                    return new DataColumn(new DataField<double>(mappedName), rows.Select(r => r.Data[index].VarCharValue.As<double>()).Cast<double>().ToArray());
                case AthenaDataTypes.boolean:
                    return new DataColumn(new DataField<bool>(mappedName), rows.Select(r => r.Data[index].VarCharValue.As<bool>()).Cast<bool>().ToArray());
                case AthenaDataTypes.date:
                    return new DataColumn(new DataField<string>(mappedName), rows.Select(r => r.Data[index].VarCharValue.As<string>()).Cast<string>().ToArray());
                case AthenaDataTypes.timestamp:
                    return new DataColumn(new DataField<string>(mappedName), rows.Select(r => r.Data[index].VarCharValue.As<string>()).Cast<string>().ToArray());
            }
            throw new Exception("Unexpected Athena Type");
        }

    }

    [AngularType]
    public class ParquetField
    {
        public AthenaTypeEnum AthenaType { get; set; }
        public string Name { get; set; }
    }

}
