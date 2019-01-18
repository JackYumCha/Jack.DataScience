using System;
using System.Threading.Tasks;
using System.ComponentModel.DataAnnotations;
using System.Reflection;
using System.Linq;
using System.IO;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using Jack.DataScience.Data.Converters;
using Jack.DataScience.Data.Parquet;
using Parquet;
using Parquet.Data;
using MvcAngular;
using ParquetDataType = Parquet.Data.DataType;

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
                            columns.Add(new DataColumn(new DataField<string>(field.Name), column.ToArray()));
                        }
                        break;
                    case AthenaTypeEnum.athena_integer:
                        {
                            columns.Add(new DataColumn(new DataField<string>(field.Name), column.Select(item => item.As<int>()).ToArray()));
                        }
                        break;
                    case AthenaTypeEnum.athena_bigint:
                        {
                            columns.Add(new DataColumn(new DataField<string>(field.Name), column.Select(item => item.As<long>()).ToArray()));
                        }
                        break;
                    case AthenaTypeEnum.athena_smallint:
                        {
                            columns.Add(new DataColumn(new DataField<string>(field.Name), column.Select(item => item.As<short>()).ToArray()));
                        }
                        break;
                    case AthenaTypeEnum.athena_tinyint:
                        {
                            columns.Add(new DataColumn(new DataField<string>(field.Name), column.Select(item => item.As<byte>()).ToArray()));
                        }
                        break;
                    case AthenaTypeEnum.athena_boolean:
                        {
                            columns.Add(new DataColumn(new DataField<string>(field.Name), column.Select(item => item.As<bool>()).ToArray()));
                        }
                        break;
                    case AthenaTypeEnum.athena_double:
                        {
                            columns.Add(new DataColumn(new DataField<string>(field.Name), column.Select(item => item.As<double>()).ToArray()));
                        }
                        break;
                    case AthenaTypeEnum.athena_varchar:
                        {
                            columns.Add(new DataColumn(new DataField<string>(field.Name), column.Select(item => item.As<string>()).ToArray()));
                        }
                        break;
                }
            }
            stream.WriteParquetColumns(columns);
        }
    }

    [AngularType]
    public class ParquetField
    {
        public AthenaTypeEnum AthenaType { get; set; }
        public string Name { get; set; }
    }

}
