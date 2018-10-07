using System;
using System.Threading;
using System.Collections.Generic;
using System.Text;
using Serilog;

namespace Jack.DataScience.Common
{
    public static class ActionRetry
    {
        public static bool Retry<T>(this IEnumerable<T> items, Action<T> action, int retryTimes = 10, int sleepTime = 1000, ILogger logger = null)
        {
            foreach (var item in items)
            {
                bool success = false;
                int retry = 0;
                while(!success && retry < retryTimes)
                {
                    try
                    {
                        action(item);
                        success = true;
                    }
                    catch(Exception ex)
                    {
                        retry += 1;
                        if (logger != null) logger.Error(ex.Message);
                        Thread.Sleep(1000);
                    }
                }
                if (!success)
                {
                    throw new MaxRetryReachedException($"Fatel Error. Failed at Max Retry of {retryTimes} times.");
                }
            }
            return true;
        }

        public static bool Retry<T>(this T item, Action<T> action, int retryTimes = 10, int sleepTime = 1000, ILogger logger = null)
        {
            bool success = false;
            int retry = 0;
            while (!success && retry < retryTimes)
            {
                try
                {
                    action(item);
                    success = true;
                }
                catch (Exception ex)
                {
                    retry += 1;
                    if (logger != null) logger.Error(ex.Message);
                    Thread.Sleep(1000);
                }
            }
            if (!success)
            {
                throw new MaxRetryReachedException($"Fatel Error. Failed at Max Retry of {retryTimes} times.");
            }
            return true;
        }

        public static TResult Retry<T, TResult>(this T item, Func<T, TResult> func, int retryTimes = 10, int sleepTime = 1000, ILogger logger = null)
        {
            bool success = false;
            int retry = 0;
            while (!success && retry < retryTimes)
            {
                try
                {
                    success = true;
                    return func(item);
                }
                catch (Exception ex)
                {
                    retry += 1;
                    if (logger != null) logger.Error(ex.Message);
                    Thread.Sleep(1000);
                }
            }
            if (!success)
            {
                throw new MaxRetryReachedException($"Fatel Error. Failed at Max Retry of {retryTimes} times.");
            }
            return default(TResult);
        }
    }

    public class MaxRetryReachedException : Exception
    {
        public MaxRetryReachedException(): base(){}
        public MaxRetryReachedException(string message) : base(message) { }
    }
}
