using MvcAngular.Generator.Lambda;
using System;
using System.Collections.Generic;
using System.Text;

namespace MvcAngular
{
    public class ServiceNotFoundException: CompactServerException
    {
        public ServiceNotFoundException(string message): base(404, message) { }

    }
}
