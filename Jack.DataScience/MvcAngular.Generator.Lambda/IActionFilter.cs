using System;
using System.Collections.Generic;
using System.Text;
using Autofac;

namespace MvcAngular.Generator.Lambda
{
    /// <summary>
    /// this is the interface that you must implement for actions
    /// </summary>
    public interface IActionFilter
    {
        bool CanInvoke(IContainer services);
    }
}
