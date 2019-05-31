using System;
using System.Collections.Generic;
using System.Text;

namespace MvcAngular.Generator.Lambda
{
    public class LongJob<T>
    {
        public string Key { get; set; }
        public bool Completed { get; set; }
        public int RetryTime { get; set; }
        public bool Failed { get; set; }
        public string ErrorMessage { get; set; }
        public T Data { get; set; }
    }
}
