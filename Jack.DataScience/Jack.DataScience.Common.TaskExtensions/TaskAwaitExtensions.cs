using System;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace Jack.DataScience.Common.TaskExtensions
{
    public static class TaskAwaitExtensions
    {
        private static Type TaskGenericType = typeof(Task<>);
        private static Type TaskType = typeof(Task);
        private static Type TaskAwaiterGenericType = typeof(TaskAwaiter<>);
        private static object[] EmptyParameters = new object[] { };

        public static object AwaitIfTask(this object value)
        {
            var valueType = value.GetType();
            if (valueType.IsGenericType && TaskType.IsAssignableFrom(valueType))
            { 
                var awaiter = valueType.GetMethod(nameof(Task<object>.GetAwaiter)).Invoke(value, EmptyParameters);
                var awaiterType = awaiter.GetType();
                var result = awaiterType.GetMethod(nameof(TaskAwaiter<object>.GetResult)).Invoke(awaiter, EmptyParameters);
                return result;
            }
            else
            {
                return value;
            }
        }
    }
}
