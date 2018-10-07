using System;

namespace MvcAngular
{
    /// <summary>
    /// use JsonParameter to decorate a parameter in the action method. the wrapped type will be shown in the client angular service.
    /// the decorated parameter must be string type. use NewtonSoft.Json.JsonConvert.Deserialize to deserialize the string to the wrapped type.
    /// </summary>
    [AttributeUsage(AttributeTargets.Parameter)]
    public class JsonPostParameterAttribute : Attribute
    {
        public JsonPostParameterAttribute(Type type)
        {
            _type = type;
        }
        private Type _type;
        /// <summary>
        /// The input type of the parameter
        /// </summary>
        public Type Type
        {
            get
            {
                return _type;
            }
        }
    }
}
