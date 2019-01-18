using System;
using System.Threading.Tasks;
using System.ComponentModel.DataAnnotations;
using System.Reflection;
using System.Linq;
using System.IO;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using Parquet;
using Parquet.Data;
using ParquetDataType = Parquet.Data.DataType;

namespace Jack.DataScience.Data.Parquet
{
    public static class ParquetExtensions
    {

        private static readonly Type DataFieldGenericType = typeof(DataField<>);
        private static readonly MethodInfo CastMethodGeneric = typeof(Enumerable).GetMethod("Cast");
        private static readonly MethodInfo ToArrayMethodGeneric = typeof(Enumerable).GetMethod("ToArray");
        private static readonly Type[] DataFieldConstructorGenericArguments = new Type[] {typeof(string) };
        private static readonly Type DateTimeType = typeof(DateTime);
        private static readonly Type NullableDateTimeType = typeof(DateTime?);
        private static readonly Type DecimalType = typeof(decimal);
        private static readonly Type NullableGenericType = typeof(Nullable<>);
        public static Type GetNonNullableType(this Type type)
        {
            if(type.IsGenericType && type.GetGenericTypeDefinition() == NullableGenericType)
            {
                return type.GetGenericArguments()[0];
            }
            else
            {
                return type;
            }
        }

        public static bool IsNullableType(this Type type)
        {
            return type.IsGenericType && type.GetGenericTypeDefinition() == NullableGenericType;
        }

        public static void WriteParquet<T>(this Stream stream, IEnumerable<T> items) where T: class
        {
            var columns = items.ToDataColumns();
            stream.WriteParquetColumns(columns);
        }

        public static void WriteParquetColumns(this Stream stream, List<DataColumn> columns)
        {
            Schema schema = new Schema(new ReadOnlyCollection<Field>(columns.Select(column => column.Field).ToArray()));

            using (ParquetWriter writer = new ParquetWriter(schema, stream))
            {
                writer.CompressionMethod = CompressionMethod.Snappy;

                using (ParquetRowGroupWriter rowGroupWriter = writer.CreateRowGroup()) // items.Count()
                {
                    foreach (var column in columns)
                    {
                        rowGroupWriter.WriteColumn(column);
                    }
                }
            }
        }

        private static List<DataColumn> ToDataColumns<T>(this IEnumerable<T> items) where T : class
        {
            Type classType = typeof(T);

            var properties = classType.GetProperties();

            List<DataColumn> columns = new List<DataColumn>();

            foreach (var prop in properties)
            {
                var coreType = prop.PropertyType.GetNonNullableType();
                var isNullable = prop.PropertyType.IsNullableType();
                if (coreType == DateTimeType)
                {
                    if (isNullable)
                    {
                        columns.Add(new DataColumn(
                            new DateTimeDataField(prop.Name, DateTimeFormat.DateAndTime, hasNulls: true),
                            items.Select(item => {
                                var value = prop.GetValue(item);
                                if (value == null) return new DateTimeOffset?();
                                return new DateTimeOffset((DateTime)value);
                            }).ToArray()
                            ));
                    }
                    else
                    {
                        columns.Add(new DataColumn(
                            new DateTimeDataField(prop.Name, DateTimeFormat.DateAndTime, hasNulls: false),
                            items.Select(item =>
                            {
                                var value = (DateTime?)prop.GetValue(item);
                                if (value.HasValue)
                                {
                                    return new DateTimeOffset?(new DateTimeOffset(value.Value));
                                }
                                else
                                {
                                    return null;
                                }
                            }).ToArray()
                            ));
                    }
                }
                else if (prop.PropertyType == DecimalType)
                {
                    var stringLength = prop.GetCustomAttributes<StringLengthAttribute>().FirstOrDefault();
                    int precision = 18;
                    int scale = 3;

                    if (stringLength != null)
                    {
                        precision = stringLength.MaximumLength;
                        scale = stringLength.MaximumLength - stringLength.MinimumLength;
                    }

                    if (isNullable)
                    {
                        columns.Add(new DataColumn(
                            new DecimalDataField(prop.Name, precision, scale),
                            items.Select(item => (decimal)prop.GetValue(item)).ToArray()
                            ));
                    }
                    else
                    {
                        columns.Add(new DataColumn(
                            new DecimalDataField(prop.Name, precision, scale),
                            items.Select(item => (decimal?)prop.GetValue(item)).ToArray()
                            ));
                    }
                }
                else
                {
                    var genericArguments = new Type[] { prop.PropertyType };
                    var genericType = DataFieldGenericType.MakeGenericType(genericArguments);
                    var genericConstructor = genericType.GetConstructor(DataFieldConstructorGenericArguments);
                    DataField field = genericConstructor.Invoke(new object[] { prop.Name }) as DataField;
                    var dataSource = items.Select(item => prop.GetValue(item));
                    var castMethod = CastMethodGeneric.MakeGenericMethod(genericArguments);
                    var toArrayMethod = ToArrayMethodGeneric.MakeGenericMethod(genericArguments);
                    var data = toArrayMethod.Invoke(null, new object[] { castMethod.Invoke(null, new object[] { dataSource }) }) as Array;
                    var column = new DataColumn(field, data);
                    columns.Add(column);
                }
            }

            return columns;
        }




        public static List<T> ReadParquet<T>(this Stream stream) where T: class, new()
        {
            Type classType = typeof(T);

            List<T> results = new List<T>();

            var properties = classType.GetProperties().ToDictionary(p => p.Name, p => p);

            var bytes = stream.ReadAsBytes().GetAwaiter().GetResult();

            using (ParquetReader reader = new ParquetReader(new MemoryStream(bytes)))
            {
                DataField[] fields = reader.Schema.GetDataFields();
                for (int g = 0; g < reader.RowGroupCount; g++)
                {
                    using (ParquetRowGroupReader rowGroupReader = reader.OpenRowGroupReader(g))
                    {
                        DataColumn[] columns = fields.Select(rowGroupReader.ReadColumn).ToArray();
                        if (columns.Length > 0)
                        {
                            for (int i = 0; i < columns[0].Data.Length; i++)
                            {
                                T item = new T();
                                foreach (var column in columns)
                                {
                                    var prop = properties[column.Field.Name];
                                    if (column.Field.DataType == ParquetDataType.DateTimeOffset)
                                    {
                                        if(prop.PropertyType == DateTimeType)
                                        {
                                            prop.SetValue(item, ((DateTimeOffset)column.Data.GetValue(i)).DateTime);
                                        }
                                        else if(prop.PropertyType == NullableDateTimeType)
                                        {
                                            var value = column.Data.GetValue(i);
                                            if(value != null)
                                            {
                                                prop.SetValue(item, ((DateTimeOffset)value).DateTime);
                                            }
                                        }
                                    }
                                    else
                                    {
                                        prop.SetValue(item, column.Data.GetValue(i));
                                    }
                                }
                                results.Add(item);
                            }
                        }
                    }
                }
            }

            return results;
        }

        public static async Task<byte[]> ReadAsBytes(this Stream stream)
        {
            List<byte> bytes = new List<byte>();
            byte[] buffer = new byte[4096];
            int readLength = 0;
            do
            {
                readLength = await stream.ReadAsync(buffer, 0, buffer.Length);
                bytes.AddRange(buffer.Take(readLength));
            }
            while (readLength > 0);
            return bytes.ToArray();
        }

        public static IEnumerable<List<T>> SplitIntoPartitions<T>(this IEnumerable<T> items, int partitionSize)
        {
            List<T> list = new List<T>();
            foreach(var item in items)
            {
                list.Add(item);
                if (list.Count >= partitionSize)
                {
                    yield return list;
                    list = new List<T>();
                }
            }
            yield return list;
        }
    }
}
