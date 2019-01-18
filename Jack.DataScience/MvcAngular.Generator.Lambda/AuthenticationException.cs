using System;
using System.Collections.Generic;
using System.Text;

namespace MvcAngular.Generator.Lambda
{
    public class AuthenticationException: Exception
    {
        public AuthenticationException(string message) : base(message) { }
    }
}
