using System;
using System.Collections.Generic;
using System.Text;
using Autofac;
using MongoDB.Driver;
using System.Security.Authentication;
using Jack.DataScience.Common;
using MongoDB.Bson.Serialization.Conventions;
using MongoDB.Bson;

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

                if(options.SslProtocol != SslProtocols.None)
                {
                    mongoClientSettings.SslSettings = new SslSettings()
                    {
                        EnabledSslProtocols = options.SslProtocol // SslProtocols.Tls12
                    };
                }
                return new MongoClient(mongoClientSettings);
            });

            builder.RegisterType<MongoContext>();

            base.Load(builder);
        }
    }
}
