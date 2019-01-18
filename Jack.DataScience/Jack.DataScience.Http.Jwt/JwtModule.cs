using System;
using System.Collections.Generic;
using System.Text;
using Autofac;

namespace Jack.DataScience.Http.Jwt
{
    public class JwtModule: Module
    {
        protected override void Load(ContainerBuilder builder)
        {
            builder.RegisterType<RoleJwtEncoder>();
            base.Load(builder);
        }
    }
}
