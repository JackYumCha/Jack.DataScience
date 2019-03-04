using System;
using System.IO;
using System.Linq;
using System.Collections.Generic;
using CsvHelper;
using CsvHelper.Configuration;

namespace Jack.DataScience.Data.CSV
{
    public static class CsvFile
    {
        public static List<T> Read<T>(string filename, Configuration configuration = null) where T: class
        {
            using (var stream = File.OpenRead(filename))
            {
                using (var reader = new StreamReader(stream))
                {
                    using(var csv = new CsvReader(reader, configuration))
                    {
                        return csv.GetRecords<T>().ToList();
                    }
                }
            }
        }

        public static List<T> ReadCsv<T>(this FileInfo file, Configuration configuration = null) where T: class
        {
            return Read<T>(file.FullName, configuration);
        }

        public static List<T> ReadCsv<T>(this Stream stream, Configuration configuration = null)
        {
            using (var reader = new StreamReader(stream))
            {
                using (var csv = configuration == null ? new CsvReader(reader) :  new CsvReader(reader, configuration))
                {
                    return csv.GetRecords<T>().ToList();
                }
            }
        }

        public static void ReadCsvCallback<T>(this Stream stream, Action<T> action,  Configuration configuration) where T: class
        {
            using (var reader = new StreamReader(stream))
            {
                using (var csv = configuration == null ? new CsvReader(reader) : new CsvReader(reader, configuration))
                {
                    while(csv.Read())
                    {
                        var item = csv.GetRecord<T>();
                        action(item);
                    }
                }
            }
        }

        public static void Write<T>(string filename, IEnumerable<T> items)
        {
            using (var stream = File.OpenWrite(filename))
            {
                using (var writer = new StreamWriter(stream))
                {
                    using (var csv = new CsvWriter(writer))
                    {
                        csv.WriteRecords(items);
                        csv.Flush();
                        writer.Flush();
                    }
                }
            }
        }

        public static void WriteCsv<T>(this FileInfo fileInfo, IEnumerable<T> items)
        {
            Write(fileInfo.FullName, items);
        }

        public static void WriteCsv<T>(this Stream stream, IEnumerable<T> items)
        {
            using (var writer = new StreamWriter(stream))
            {
                using (var csv = new CsvWriter(writer))
                {
                    csv.WriteRecords(items);
                    csv.Flush();
                    writer.Flush();
                }
            }
        }
    }
}
