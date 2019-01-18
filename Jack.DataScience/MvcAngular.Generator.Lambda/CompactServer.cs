using System;
using System.Threading.Tasks;
using System.Linq;
using System.Collections.Generic;
using System.Text;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Autofac;
using System.Reflection;
using System.Runtime.CompilerServices;

namespace MvcAngular.Generator.Lambda
{
    public class CompactServer
    {

        private readonly IContainer services;
        private readonly CompactServerTypes serviceTypes;
        private readonly JsonSerializerSettings jsonSerializerSettings;
        private static Type IActionFilterType = typeof(IActionFilter);
        private static Type TaskGenericType = typeof(Task<>);
        private static Type TaskType = typeof(Task);
        private static Type TaskAwaiterGenericType = typeof(TaskAwaiter<>);
        private static object[] EmptyParameters = new object[] { };

        /// <summary>
        /// 
        /// </summary>
        /// <param name="services"></param>
        /// <param name="jsonSerializerSettings"></param>
        public CompactServer(IContainer services, JsonSerializerSettings jsonSerializerSettings)
        {
            this.services = services;
            if(!services.TryResolve(out serviceTypes))
            {
                throw new Exception($"Type '{nameof(CompactServerTypes)}' was not registerd in AutoFace. Please register it as instance and add controller types to it.");
            }
            this.jsonSerializerSettings = jsonSerializerSettings;
        }

        /// <summary>
        /// invoke the service method. there are series of guard checks to make sure it works.
        /// </summary>
        /// <param name="requst"></param>
        /// <returns></returns>
        public string Invoke(string requst)
        {
            var requestObj = JsonConvert.DeserializeObject<JObject>(requst);
            var controllerName = requestObj.Property("Controller").Value.Value<string>();
            var methodName = requestObj.Property("Method").Value.Value<string>();
            

            object service = null;

            if (!serviceTypes.ContainsKey(controllerName))
            {
                throw new ServiceNotFoundException($"Controller '{controllerName}' was not found in the CompactServerTypes instance. Please make sure you have added it to the registered instance of CompactServerTypes in AutoFac.");
            }

            if(!services.TryResolve(serviceTypes[controllerName], out service))
            {
                throw new ServiceNotFoundException($"Controller '{serviceTypes[controllerName].FullName}' was not found in the AutoFac services. Please make sure you have registered it as Type in AutoFac.");
            }

            Type serviceType = serviceTypes[controllerName];
            var methodInfo = serviceType.GetMethod(methodName);

            if (methodInfo == null)
            {
                throw new ServiceNotFoundException($"Method '{methodName}' was not found in Controller '{serviceTypes[controllerName].FullName}'.");
            }

            if(!methodInfo.GetCustomAttributes<RpcAttribute>().Any())
            {
                throw new ServiceNotFoundException($"Method '{methodName}' of '{serviceTypes[controllerName].FullName}' does not have {nameof(RpcAttribute)}. You need to add {nameof(RpcAttribute)} to service method.");
            }
            
            // credential is set here, this is because we need a workaround for AWS or other API Gateway/Load Balancer that can do proper CORS response
            var credential = requestObj.Property("Credential").Value.Value<string>();
            RequestCredential requestCredential = null;

            if (!services.TryResolve(out requestCredential))
            {
                throw new Exception($"Type '{nameof(RequestCredential)}' was not registered in the AutoFac services");
            }

            requestCredential.Value = credential;

            var filters = methodInfo.GetCustomAttributes(true).Where(attr => IActionFilterType.IsAssignableFrom(attr.GetType())).ToList();

            foreach(var filter in filters)
            {
                if(!(filter as IActionFilter).CanInvoke(services))
                {
                    throw new AuthenticationException($"The credential did not pass the authentication check by {filter.GetType().FullName}.");
                }
            }

            // parameters are not needed until here
            var parameters = requestObj.Property("Parameters").Value.Value<JArray>();

            var serializer = JsonSerializer.Create(jsonSerializerSettings);
            int index = 0;
            List<object> parameterList = new List<object>();
            foreach (var parameter in methodInfo.GetParameters())
            {
                parameterList.Add(parameters[index].ToObject(parameter.ParameterType, serializer));
                index += 1;
            }

            // the response could be a task
            var response = methodInfo.Invoke(service, parameterList.ToArray());
            var responseType = response.GetType();
            if (responseType.IsGenericType && TaskType.IsAssignableFrom(responseType))
            {
                // process the task await
                var awaiter = responseType.GetMethod(nameof(Task<object>.GetAwaiter)).Invoke(response, EmptyParameters);
                var awaiterType = awaiter.GetType();
                var result = awaiterType.GetMethod(nameof(TaskAwaiter<object>.GetResult)).Invoke(awaiter, EmptyParameters);
                return JsonConvert.SerializeObject(result, jsonSerializerSettings);
            }
            else
            {
                return JsonConvert.SerializeObject(response, jsonSerializerSettings);
            }
        }
    }
}
