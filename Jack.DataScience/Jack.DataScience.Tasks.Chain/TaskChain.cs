using System;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Jack.DataScience.Tasks.Chain
{
    public static class TaskChain
    {
        public static async Task<TOut> Then<TIn, TOut>(this Task<TIn> taskIn, Func<TIn, TOut> map)
        {
            var result = await taskIn;
            return map(result);
        }

        public static async Task<TOut> Then<TIn, TOut>(this Task<TIn> taskIn, Func<TIn, Task<TOut>> map)
        {
            var result = await taskIn;
            return await map(result);
        }

        public static async Task<T[]> ToArrayAsync<T>(this IEnumerable<Task<T>> tasks)
        {
            return await Task.WhenAll<T>(tasks.ToArray());
        }
    }
}
