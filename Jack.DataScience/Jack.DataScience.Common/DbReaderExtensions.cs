using System;
using System.Collections.Generic;
using System.Text;
using System.Data.Common;

namespace Jack.DataScience.Common
{
    public static class DbReaderExtensions
    {
        public static string ReadStringOrNull(this DbDataReader reader, int index)
        {
            if (reader.IsDBNull(index))
            {
                return null;
            }
            else
            {
                return reader.GetString(index);
            }
        }

        public static string ReadGuidStringOrNull(this DbDataReader reader, int index)
        {
            if (reader.IsDBNull(index))
            {
                return null;
            }
            else
            {
                return reader.GetGuid(index).ToString().ToLower();
            }
        }

        public static int? ReadInt32OrNull(this DbDataReader reader, int index)
        {
            if (reader.IsDBNull(index))
            {
                return null;
            }
            else
            {
                return reader.GetInt32(index);
            }
        }

        public static long? ReadInt64OrNull(this DbDataReader reader, int index)
        {
            if (reader.IsDBNull(index))
            {
                return null;
            }
            else
            {
                return reader.GetInt64(index);
            }
        }

        public static decimal? ReadDecimalOrNull(this DbDataReader reader, int index)
        {
            if (reader.IsDBNull(index))
            {
                return null;
            }
            else
            {
                return reader.GetDecimal(index);
            }
        }

        public static float? ReadFloatOrNull(this DbDataReader reader, int index)
        {
            if (reader.IsDBNull(index))
            {
                return null;
            }
            else
            {
                return reader.GetFloat(index);
            }
        }

        public static double? ReadDoubleOrNull(this DbDataReader reader, int index)
        {
            if (reader.IsDBNull(index))
            {
                return null;
            }
            else
            {
                return reader.GetDouble(index);
            }
        }

        public static bool? ReadBooleanOrNull(this DbDataReader reader, int index)
        {
            if (reader.IsDBNull(index))
            {
                return null;
            }
            else
            {
                return reader.GetBoolean(index);
            }
        }
    }
}
