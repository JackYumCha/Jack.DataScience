using System;
using System.Collections.Generic;
using System.Text;

namespace MvcAngular.Generator.Lambda
{
    public class RequestHeaders: Dictionary<string, string>
    {
        public static RequestHeaders From(IDictionary<string, string> headers)
        {
            RequestHeaders instance = new RequestHeaders();
            if(headers != null)
            {
                foreach (var kvp in headers)
                {
                    instance.Add(kvp.Key, kvp.Value);
                }
            }
            return instance;
        }
    }
}
