using System;

namespace MvcAngular.Generator
{
    /// <summary>
    /// Force the Mvc generator to use the provided Type.
    /// </summary>
    [AttributeUsage(AttributeTargets.Method | AttributeTargets.Property)]
    public class AnguarTypeMapAttribute: Attribute
    {
        public AnguarTypeMapAttribute(string angularType)
        {
            AngularType = angularType;
        }

        public string AngularType { get; private set; }
    }
}
