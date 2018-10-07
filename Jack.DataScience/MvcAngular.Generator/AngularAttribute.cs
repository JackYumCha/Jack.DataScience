using System;

namespace MvcAngular
{
    /// <summary>
    /// Use this attribute on mvc classes that need to be converted into Angular services.
    /// The Route("api/[controller]/[action]") attribute must be used together
    /// Use Description("Description for your front end consumer.") to add front end documentation.
    /// Use Attribute [AngularService("Path/Of/Your/Service/Type/Relative/To/Root"), Route("api/[controller]/[action]")].
    /// To make methods of this class visible to Angular, add HttpPost or HttpGet attributes to the methods;
    /// </summary>
    [AttributeUsage(AttributeTargets.Class)]
    public class AngularAttribute : Attribute { }
}