using System;
using System.Collections.Generic;
using System.Text;

namespace MvcAngular.Generator.Lambda
{
    public class CompactServerTypes: Dictionary<string, Type> {
        public void Add<T>()
        {
            Type type = typeof(T);
            base.Add(type.FullName, type);
        }
    }
}
