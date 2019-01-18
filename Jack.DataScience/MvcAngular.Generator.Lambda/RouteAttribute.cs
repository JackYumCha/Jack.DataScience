using System;
using System.Collections.Generic;
using System.Text;

namespace MvcAngular
{
    [AttributeUsage(AttributeTargets.Class)]
    public class RouteAttribute: Attribute
    {

        public RouteAttribute(string template)
        {
            Template = template;
        }
        public string Template { get; private set; }
    }
}
