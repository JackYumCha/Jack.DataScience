using System;

namespace MvcAngular
{
    /// <summary>
    /// Ignore a mvc method or a json property
    /// </summary>
    [AttributeUsage(AttributeTargets.Method | AttributeTargets.Property)]
    public class AngularIgnoreAttribute: Attribute
    {
    }
}
