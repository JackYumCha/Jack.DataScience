using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;

namespace Jack.DataScience.Data.Converters
{
    public static class EntityMappingConvertExtensions
    {
        public static List<TTo> MapPropertiesFrom<TTo, TFrom1>(this List<TTo> tos, IEnumerable<TFrom1> from1s, params IMapRule<TFrom1>[] mapRules) where TTo : new()
        {
            Type tTo = typeof(TTo);
            var propertiesTo = tTo.GetProperties().ToDictionary(p => p.Name, p => p);

            // to do
            var listFroms = new List<Dictionary<string, PropertyInfo>>() {
                typeof(TFrom1).GetProperties().ToDictionary(p => p.Name, p => p)
            };

            var mapped = new Dictionary<string, PropertyMap>();
            var rules = mapRules.ToDictionary(m => m.PropretyName, m => m);
            var usingRules = new Dictionary<string, IMapRule<TFrom1>>();

            foreach (var propertyTo in propertiesTo)
            {
                if (rules.ContainsKey(propertyTo.Key))
                {
                    var rule = rules[propertyTo.Key];
                    if (rule.PropertyType != propertyTo.Value.PropertyType)
                        throw new TypeMappingException(tTo, propertyTo.Key, $"MapRule output type '{rule.PropertyType.FullName}' does not match property type '{propertyTo.Value.PropertyType.FullName}'.");
                    rules.Add(rule.PropretyName, rule);
                }
                else
                {
                    var found = listFroms
                        .Select((pdict, index) =>
                        {
                            return pdict.ContainsKey(propertyTo.Key) && pdict[propertyTo.Key].PropertyType == propertyTo.Value.PropertyType ?
                            new PropertyMap() { Index = index, Property = pdict[propertyTo.Key] } : null;
                        })
                        .Where(p => p != null)
                        .ToList();

                    switch (found.Count)
                    {
                        case 1:
                            mapped.Add(propertyTo.Key, found[0]);
                            break;
                        case 0:
                            throw new TypeMappingException(tTo, propertyTo.Key, $"No MapRule or Property source was found for mapped property '{tTo.Name}.{propertyTo.Key}'.");
                        default:

                            throw new TypeMappingException(tTo, propertyTo.Key, $"Multiple Property sources were detected for mapped property '{tTo.Name}.{propertyTo.Key}' from {string.Join(", ", found.Select(p => $"'{p.Property.DeclaringType}.{p.Property.Name}'"))}.");
                    }
                }
            }

            int count = from1s.Count();
            // check lengths

            // to do
            var enumerator1 = from1s.GetEnumerator();


            for (int i = 0; i < count; i++)
            {
                // to do
                enumerator1.MoveNext();
                var from1 = enumerator1.Current;
                var mapdict = new Dictionary<int, object>() { { 0, from1 } };


                var to = new TTo();
                foreach (var property in propertiesTo)
                {
                    if (rules.ContainsKey(property.Key))
                    {
                        // to do
                        property.Value.SetValue(to, rules[property.Key].Convert(from1));
                    }
                    else
                    {
                        var map = mapped[property.Key];
                        property.Value.SetValue(to, mapped[property.Key].Property.GetValue(mapdict[map.Index]));
                    }
                }
                tos.Add(to);
            }
            return tos;
        }


        public static List<TTo> MapPropertiesFrom<TTo, TFrom1, TFrom2>(this List<TTo> tos, IEnumerable<TFrom1> from1s, IEnumerable<TFrom2> from2s, params IMapRule<TFrom1, TFrom2>[] mapRules) where TTo : new()
        {
            Type tTo = typeof(TTo);
            var propertiesTo = tTo.GetProperties().ToDictionary(p => p.Name, p => p);

            // to do
            var listFroms = new List<Dictionary<string, PropertyInfo>>() {
                typeof(TFrom1).GetProperties().ToDictionary(p => p.Name, p => p),
                typeof(TFrom2).GetProperties().ToDictionary(p => p.Name, p => p)
            };

            var mapped = new Dictionary<string, PropertyMap>();
            var rules = mapRules.ToDictionary(m => m.PropretyName, m => m);
            var usingRules = new Dictionary<string, IMapRule<TFrom1>>();

            foreach (var propertyTo in propertiesTo)
            {
                if (rules.ContainsKey(propertyTo.Key))
                {
                    var rule = rules[propertyTo.Key];
                    if (rule.PropertyType != propertyTo.Value.PropertyType)
                        throw new TypeMappingException(tTo, propertyTo.Key, $"MapRule output type '{rule.PropertyType.FullName}' does not match property type '{propertyTo.Value.PropertyType.FullName}'.");
                    rules.Add(rule.PropretyName, rule);
                }
                else
                {
                    var found = listFroms
                        .Select((pdict, index) =>
                        {
                            return pdict.ContainsKey(propertyTo.Key) && pdict[propertyTo.Key].PropertyType == propertyTo.Value.PropertyType ?
                            new PropertyMap() { Index = index, Property = pdict[propertyTo.Key] } : null;
                        })
                        .Where(p => p != null)
                        .ToList();

                    switch (found.Count)
                    {
                        case 1:
                            mapped.Add(propertyTo.Key, found[0]);
                            break;
                        case 0:
                            throw new TypeMappingException(tTo, propertyTo.Key, $"No MapRule or Property source was found for mapped property '{tTo.Name}.{propertyTo.Key}'.");
                        default:
                            throw new TypeMappingException(tTo, propertyTo.Key, $"Multiple Property sources were detected for mapped property '{tTo.Name}.{propertyTo.Key}' from {string.Join(", ", found.Select(p => $"'{p.Property.DeclaringType}.{p.Property.Name}'"))}.");
                    }
                }
            }

            int count = from1s.Count();
            // check lengths
            if (from2s.Count() != count) throw new TypeMappingException(tTo, null, $"The source type lists do not contain the same number of elements.");

            var enumerator1 = from1s.GetEnumerator();
            var enumerator2 = from2s.GetEnumerator();
            for (int i = 0; i < count; i++)
            {
                // to do
                enumerator1.MoveNext();
                enumerator2.MoveNext();
                var from1 = enumerator1.Current;
                var from2 = enumerator2.Current;
                var mapdict = new Dictionary<int, object>() { { 0, from1 }, { 1, from2 } };

                var to = new TTo();
                foreach (var property in propertiesTo)
                {
                    if (rules.ContainsKey(property.Key))
                    {
                        // to do
                        property.Value.SetValue(to, rules[property.Key].Convert(from1, from2));
                    }
                    else
                    {
                        var map = mapped[property.Key];
                        property.Value.SetValue(to, mapped[property.Key].Property.GetValue(mapdict[map.Index]));
                    }
                }
                tos.Add(to);
            }
            return tos;
        }

        public static List<TTo> MapPropertiesFrom<TTo, TFrom1, TFrom2, TFrom3>(this List<TTo> tos, IEnumerable<TFrom1> from1s, IEnumerable<TFrom2> from2s, IEnumerable<TFrom3> from3s, params IMapRule<TFrom1, TFrom2, TFrom3>[] mapRules) where TTo : new()
        {
            Type tTo = typeof(TTo);
            var propertiesTo = tTo.GetProperties().ToDictionary(p => p.Name, p => p);

            // to do
            var listFroms = new List<Dictionary<string, PropertyInfo>>() {
                typeof(TFrom1).GetProperties().ToDictionary(p => p.Name, p => p),
                typeof(TFrom2).GetProperties().ToDictionary(p => p.Name, p => p),
                typeof(TFrom3).GetProperties().ToDictionary(p => p.Name, p => p)
            };

            var mapped = new Dictionary<string, PropertyMap>();
            var rules = mapRules.ToDictionary(m => m.PropretyName, m => m);
            var usingRules = new Dictionary<string, IMapRule<TFrom1>>();

            foreach (var propertyTo in propertiesTo)
            {
                if (rules.ContainsKey(propertyTo.Key))
                {
                    var rule = rules[propertyTo.Key];
                    if (rule.PropertyType != propertyTo.Value.PropertyType)
                        throw new TypeMappingException(tTo, propertyTo.Key, $"MapRule output type '{rule.PropertyType.FullName}' does not match property type '{propertyTo.Value.PropertyType.FullName}'.");
                    rules.Add(rule.PropretyName, rule);
                }
                else
                {
                    var found = listFroms
                        .Select((pdict, index) =>
                        {
                            return pdict.ContainsKey(propertyTo.Key) && pdict[propertyTo.Key].PropertyType == propertyTo.Value.PropertyType ?
                            new PropertyMap() { Index = index, Property = pdict[propertyTo.Key] } : null;
                        })
                        .Where(p => p != null)
                        .ToList();

                    switch (found.Count)
                    {
                        case 1:
                            mapped.Add(propertyTo.Key, found[0]);
                            break;
                        case 0:
                            throw new TypeMappingException(tTo, propertyTo.Key, $"No MapRule or Property source was found for mapped property '{tTo.Name}.{propertyTo.Key}'.");
                        default:
                            throw new TypeMappingException(tTo, propertyTo.Key, $"Multiple Property sources were detected for mapped property '{tTo.Name}.{propertyTo.Key}' from {string.Join(", ", found.Select(p => $"'{p.Property.DeclaringType}.{p.Property.Name}'"))}.");
                    }
                }
            }

            int count = from1s.Count();
            // check lengths
            if (from2s.Count() != count) throw new TypeMappingException(tTo, null, $"The source type list '{nameof(from2s)}' does not contain the same number of elements as '{nameof(from1s)}'.");
            if (from3s.Count() != count) throw new TypeMappingException(tTo, null, $"The source type list '{nameof(from3s)}' does not contain the same number of elements as '{nameof(from1s)}'.");
            var enumerator1 = from1s.GetEnumerator();
            var enumerator2 = from2s.GetEnumerator();
            var enumerator3 = from3s.GetEnumerator();

            for (int i = 0; i < count; i++)
            {
                // to do
                enumerator1.MoveNext();
                enumerator2.MoveNext();
                enumerator3.MoveNext();
                var from1 = enumerator1.Current;
                var from2 = enumerator2.Current;
                var from3 = enumerator3.Current;
                var mapdict = new Dictionary<int, object>() { { 0, from1 }, { 1, from2 }, { 2, from3 } };

                var to = new TTo();
                foreach (var property in propertiesTo)
                {
                    if (rules.ContainsKey(property.Key))
                    {
                        // to do
                        property.Value.SetValue(to, rules[property.Key].Convert(from1, from2, from3));
                    }
                    else
                    {
                        var map = mapped[property.Key];
                        property.Value.SetValue(to, mapped[property.Key].Property.GetValue(mapdict[map.Index]));
                    }
                }
                tos.Add(to);
            }
            return tos;
        }
    }


    internal class PropertyMap
    {
        internal int Index { get; set; }
        internal PropertyInfo Property { get; set; }
    }


    public class TypeMappingException : Exception
    {
        public Type Target { get; }
        public string Property { get; }
        public TypeMappingException(Type target, string property, string message) : base(message)
        {
            Target = target;
            Property = property;
        }
    }

    public interface IMapRule
    {
        string PropretyName { get; }
        Type PropertyType { get; }
    }

    public interface IMapRule<TFrom1> : IMapRule
    {

        object Convert(TFrom1 from1);
    }

    public interface IMapRule<TFrom1, TFrom2> : IMapRule
    {
        object Convert(TFrom1 from1, TFrom2 from2);
    }

    public interface IMapRule<TFrom1, TFrom2, TFrom3> : IMapRule
    {
        object Convert(TFrom1 from1, TFrom2 from2, TFrom3 from3);
    }

    public class MapRule<TFrom1, T> : IMapRule<TFrom1>
    {
        private readonly Func<TFrom1, T> mapper;
        public string PropretyName { get; }
        public Type PropertyType { get; }
        public MapRule(string propertyName, Func<TFrom1, T> mapper)
        {
            PropretyName = propertyName;
            PropertyType = typeof(T);
            this.mapper = mapper;
        }

        public T Invoke(TFrom1 from1)
        {
            return mapper(from1);
        }

        public object Convert(TFrom1 from1)
        {
            return Invoke(from1);
        }
    }

    public class MapRule<TFrom1, TFrom2, T> : IMapRule<TFrom1, TFrom2>
    {
        private readonly Func<TFrom1, TFrom2, T> mapper;
        public string PropretyName { get; }
        public Type PropertyType { get; }
        public MapRule(string propertyName, Func<TFrom1, TFrom2, T> mapper)
        {
            PropretyName = propertyName;
            PropertyType = typeof(T);
            this.mapper = mapper;
        }

        public T Invoke(TFrom1 from1, TFrom2 from2)
        {
            return mapper(from1, from2);
        }

        public object Convert(TFrom1 from1, TFrom2 from2)
        {
            return Invoke(from1, from2);
        }
    }

    public class MapRule<TFrom1, TFrom2, TFrom3, T> : IMapRule<TFrom1, TFrom2, TFrom3>
    {
        private readonly Func<TFrom1, TFrom2, TFrom3, T> mapper;
        public string PropretyName { get; }
        public Type PropertyType { get; }
        public MapRule(string propertyName, Func<TFrom1, TFrom2, TFrom3, T> mapper)
        {
            PropretyName = propertyName;
            PropertyType = typeof(T);
            this.mapper = mapper;
        }

        public T Invoke(TFrom1 from1, TFrom2 from2, TFrom3 from3)
        {
            return mapper(from1, from2, from3);
        }

        public object Convert(TFrom1 from1, TFrom2 from2, TFrom3 from3)
        {
            return Invoke(from1, from2, from3);
        }
    }

}
