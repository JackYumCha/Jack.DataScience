using System;

namespace MvcAngular
{

    /// <summary>
    /// Use this attribute on a class to allow it to be converted to front end TypeScript interface;
    /// Use Attribute [JsonType("Path/Of/Your/Json/Type/Relative/To/Root")]
    /// </summary>
    [AttributeUsage(AttributeTargets.Class | AttributeTargets.Enum)]
    public class AngularTypeAttribute : Attribute { }

}