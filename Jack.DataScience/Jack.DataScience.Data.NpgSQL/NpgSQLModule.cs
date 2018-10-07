using System;
using System.Collections.Generic;
using System.Text;
using Autofac;
using Npgsql;
using Npgsql.Logging;
using Serilog;
using Jack.DataScience.Common;
using Jack.DataScience.Data.NpqSQL.Logging;

namespace Jack.DataScience.Data.NpgSQL
{
    public class NpgSQLModule: Module
    {
        protected override void Load(ContainerBuilder builder)
        {
            builder.Register<NpgsqlConnection>(context =>
            {
                var options = context.Resolve<PostgreSQLOptions>();
                if (!loggerRegistered)
                {
                    RegisterLogger(context, options);
                }
                return new NpgsqlConnection(options.ConnectionString);
            });

            builder.RegisterType<PostgreSQLBootstrap>();

            base.Load(builder);
        }

        private static bool loggerRegistered = false;
        private static void RegisterLogger(IComponentContext context, PostgreSQLOptions options)
        {
            ILogger logger = null;
            if(context.TryResolve<ILogger>(out logger))
            {
                NpgsqlLogManager.IsParameterLoggingEnabled = true;
                NpgsqlLogManager.Provider = new PostgreSQLLoggingProvider(logger, options.LogLevel);
            }
            loggerRegistered = true;
        }
    }
}
