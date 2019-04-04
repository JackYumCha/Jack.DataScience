using System;
using System.Collections.Generic;
using System.Text;

namespace MvcAngular
{
    [AttributeUsage(AttributeTargets.Method | AttributeTargets.Property | AttributeTargets.Parameter)]
    public class AngularMapTypeAttribute: Attribute
    {
        public AngularMapTypeAttribute(Type mappedType) : base()
        {
            MappedType = mappedType;
        }

        public Type MappedType { get; private set; }
    }
}
