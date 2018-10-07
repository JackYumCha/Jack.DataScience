using System;
using System.Reflection;
using System.Linq;
using System.IO;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using Parquet;
using Parquet.Data;

namespace Jack.DataScience.Data.Parquet
{
    public static class ParquetExtensions
    {

        private static readonly Type DataFieldGenericType = typeof(DataField<>);
        private static readonly MethodInfo CastMethodGeneric = typeof(Enumerable).GetMethod("Cast");
        private static readonly MethodInfo ToArrayMethodGeneric = typeof(Enumerable).GetMethod("ToArray");
        private static readonly Type[] DataFieldConstructorGenericArguments = new Type[] {typeof(string) };
        private static readonly Type DateTimeType = typeof(DateTime);
        public static void WriteParquet<T>(this Stream stream, IEnumerable<T> items) where T: class
        {
            Type classType = typeof(T);

            var properties = classType.GetProperties();

            List<DataColumn> columns = new List<DataColumn>();

            foreach(var prop in properties)
            {
                if(prop.PropertyType == DateTimeType)
                {
                    columns.Add(new DataColumn(
                        new DateTimeDataField(prop.Name, DateTimeFormat.DateAndTime),
                        items.Select(item => new DateTimeOffset(((DateTime)prop.GetValue(item)))).ToArray()
                        ));
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

        public static IEnumerable<T> ReadParquet<T>(this Stream stream) where T: class, new()
        {
            Type classType = typeof(T);

            var properties = classType.GetProperties().ToDictionary(p => p.Name, p => p);

            using (ParquetReader reader = new ParquetReader(stream))
            {
                DataField[] fields = reader.Schema.GetDataFields();
                for(int g = 0; g < reader.RowGroupCount; g++)
                {
                    using (ParquetRowGroupReader rowGroupReader = reader.OpenRowGroupReader(g))
                    {
                        DataColumn[] columns = fields.Select(rowGroupReader.ReadColumn).ToArray();
                        if(columns.Length > 0)
                        {
                            for(int i = 0; i < columns[0].Data.Length; i++)
                            {
                                T item = new T();
                                foreach(var column in columns)
                                {
                                    var prop = properties[column.Field.Name];
                                    if(column.Field.DataType == DataType.DateTimeOffset && prop.PropertyType == DateTimeType)
                                    {
                                        prop.SetValue(item, ((DateTimeOffset)column.Data.GetValue(i)).DateTime);
                                    }
                                    else
                                    {
                                        prop.SetValue(item, column.Data.GetValue(i));
                                    }
                                }
                                yield return item;
                            }
                        }
                    }
                }
            }
        }
    }
}
