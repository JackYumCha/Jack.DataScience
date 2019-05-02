using System;
using System.Collections.Generic;
using System.Text;

namespace Jack.DataScience.Data.MongoDB
{
    public static class DictionaryExtensions
    {
        public static TValue TryGet<TKey, TValue>(this Dictionary<TKey, TValue> dictionary, TKey key) where TValue : class
        {
            if (key == null) return null;
            TValue value;
            dictionary.TryGetValue(key, out value);
            return value;
        }
    }
}
