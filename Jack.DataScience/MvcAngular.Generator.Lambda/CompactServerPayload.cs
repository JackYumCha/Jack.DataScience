using System;
using System.Collections.Generic;
using System.Text;

namespace MvcAngular.Generator.Lambda
{
    public class CompactServerPayload
    {
        public string Controller { get; set; }
        public string Method { get; set; }
        public string Credential { get; set; }
        public List<object> Parameters { get; set; }
    }
}
