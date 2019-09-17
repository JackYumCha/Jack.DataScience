using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Text;

namespace Jack.DataScience.Data.MongoDB
{
    public static class FilterDefinitionExtensions
    {

        public static void Test<T>()
        {
            var builder = Builders<T>.Filter;

            var definition = (
                 FilterComposer<TestData>.Build(b => b.Eq("Name", "23")) &
                FilterComposer<TestData>.Where(t => t.Name == "test") &
               (FilterComposer<TestData>.Where(t => t.Value == 0) | FilterComposer<TestData>.Where(t => t.Value == 10))).FilterDefinition;

        }
    }

    public class TestData
    {
        public string Name { get; set; }
        public int Value { get; set; }
    }

    public class FilterComposer<T>
    {
        public FilterComposer()
        {
            FilterDefinition = Default.Empty;
        }

        public FilterComposer(FilterDefinition<T> filterDefinition)
        {
            FilterDefinition = filterDefinition;
        }

        public FilterComposer(Func<FilterDefinitionBuilder<T>, FilterDefinition<T>> func)
        {
            FilterDefinition = func(Default);
        }

        public FilterComposer(Expression<Func<T, bool>> condition)
        {
            FilterDefinition = Default.Where(condition);
        }

        public static FilterComposer<T> Where(Expression<Func<T, bool>> condition)
        {
            return new FilterComposer<T>(condition);
        }

        public static FilterComposer<T> Build(Func<FilterDefinitionBuilder<T>, FilterDefinition<T>> func)
        {
            return new FilterComposer<T>(func);
        }

        public FilterDefinition<T> FilterDefinition { get; set; }

        public static FilterDefinitionBuilder<T> Default { get => Builders<T>.Filter; }

        public static FilterComposer<T> operator & (FilterComposer<T> a, FilterComposer<T> b)
        {
            return new FilterComposer<T>(Default.And(new FilterDefinition<T>[] { a.FilterDefinition, b.FilterDefinition }));
        }

        public static FilterComposer<T> operator | (FilterComposer<T> a, FilterComposer<T> b)
        {
            return new FilterComposer<T>(Default.Or(new FilterDefinition<T>[] { a.FilterDefinition, b.FilterDefinition }));
        }

        public static FilterComposer<T> operator !(FilterComposer<T> a)
        {
            return new FilterComposer<T>(Default.Not(a.FilterDefinition));
        }
    }
}
