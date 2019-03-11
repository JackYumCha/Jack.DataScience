﻿using System;
using System.Collections.Generic;
using System.Text;
using Autofac;
using MongoDB.Driver;
using System.Security.Authentication;
using Jack.DataScience.Common;

namespace Jack.DataScience.Data.MongoDB
{
    public class MongoModule: Module
    {
        protected override void Load(ContainerBuilder builder)
        {
            builder.RegisterType<MongoBootstrap>();
            builder.Register((context) =>
            {
                var options = context.Resolve<MongoOptions>();

                MongoClientSettings mongoClientSettings = MongoClientSettings.FromUrl(new MongoUrl(options.Url));

                mongoClientSettings.SslSettings = new SslSettings()
                {
                    EnabledSslProtocols = SslProtocols.Tls12
                };
                return new MongoClient(mongoClientSettings);
            });
            base.Load(builder);
        }
    }
}
