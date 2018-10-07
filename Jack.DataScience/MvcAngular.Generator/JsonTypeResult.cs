using Microsoft.AspNetCore.Mvc;

namespace MvcAngular
{
    /// <summary>
    /// This is the polyfill for Strong/Static Type JsonResult. It tells the Angular service what type will return in the Promise.
    /// </summary>
    public class JsonTypeResult<T> : JsonResult
    {
        /// <summary>
        /// Be careful, camel case is the default.
        /// </summary>
        /// <param name="value"></param>
        public JsonTypeResult(T value) : base(value)
        {
        }
        /// <summary>
        /// Be careful, camel case is the default.
        /// </summary>
        /// <param name="value"></param>
        /// <param name="serializerSettings"></param>
        public JsonTypeResult(T value, Newtonsoft.Json.JsonSerializerSettings serializerSettings) : base(value, serializerSettings)
        {
        }
    }


}