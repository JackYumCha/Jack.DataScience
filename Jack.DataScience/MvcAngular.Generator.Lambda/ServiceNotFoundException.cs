using System;
using System.Collections.Generic;
using System.Text;

namespace MvcAngular
{
    public class ServiceNotFoundException: Exception
    {
        public ServiceNotFoundException() : base() { }
        public ServiceNotFoundException(string message): base(message) { }

    }
}
