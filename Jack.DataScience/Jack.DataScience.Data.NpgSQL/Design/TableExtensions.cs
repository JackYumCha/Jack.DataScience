using System;
using System.Linq;
using System.Reflection;
using System.Collections.Generic;
using System.Text;
using GeoJSON.Net.Geometry;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace Jack.DataScience.DataTypes
{
    public static class TableExtensions
    {

        public static List<string> TableIndices(this TypeSchema schema, string suffix = null)
        {
            return schema
                .Fields
                .Where(f => f.IsIndex && !f.IsPrimaryKey)
                .Select(p => $"CREATE INDEX {schema.TableNameWithSuffix(suffix)}_{p.Name}_idx ON {schema.TableNameWithSuffix(suffix)} {p.TableIndex()};")
                .ToList();
        }

        public static string TableDefinition(this TypeSchema schema, string suffix = null)
        {
            return $@"CREATE TABLE {schema.TableNameWithSuffix(suffix)}(
{string.Join(",\n", schema.Fields.Select(f => f.FieldDeclaration()))}
);";
        }

        public static string EscapeTableForSuffix<T>(this string tableNameWithSuffix) where T: class, new()
        {
            Type table = typeof(T);
            if (tableNameWithSuffix.StartsWith($"{table.Name}__"))
            {
                return tableNameWithSuffix.Substring($"{table.Name}__".Length);
            }
            else
            {
                throw new Exception($"the value '{tableNameWithSuffix}' is not valid table suffix name for type '{table.Name}'!");
            }
        }
    }
}
