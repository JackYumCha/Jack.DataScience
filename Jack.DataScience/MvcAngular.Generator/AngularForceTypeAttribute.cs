using System;
using System.Collections.Generic;
using System.Text;

namespace MvcAngular
{
    /// <summary>
    /// force the angular tranpilor to use the specified type
    /// </summary>
    [AttributeUsage(AttributeTargets.Method | AttributeTargets.Property)]
    public class AngularForceTypeAttribute: Attribute
    {
        public AngularForceTypeAttribute(string cutomizedType): base()
        {
            CustomizedType = cutomizedType;
        }

        public string CustomizedType { get; private set; }
    }
}
