using System;
using System.Collections.Generic;
using System.Text;

namespace MvcAngular.Generator.Lambda
{
    public class CompactServerException: Exception
    {
        public int StatusCode { get; private set; }

        public CompactServerException(int statusCode, string message): base(message)
        {
            this.StatusCode = statusCode;
        }
    }
}
