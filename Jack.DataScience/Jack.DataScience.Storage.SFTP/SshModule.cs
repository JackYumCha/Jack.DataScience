using System;
using System.Collections.Generic;
using System.Text;
using Renci.SshNet;
using Autofac;

namespace Jack.DataScience.Storage.SFTP
{
    public class SshModule: Module
    {
        protected override void Load(ContainerBuilder builder)
        {
            builder.Register((context) =>
            {
                var options = context.Resolve<SshOptions>();
                switch (options.AuthenticationMethod)
                {
                    case "PrivateKey":
                        return new SftpClient(options.Url, options.Username, new PrivateKeyFile($"{AppContext.BaseDirectory}/{options.PrivateKeyPath}"));
                    case "Password":
                    default:
                        return new SftpClient(options.Url, options.Username, options.Password);
                }
            });
            builder.Register((context) =>
            {
                var options = context.Resolve<SshOptions>();
                switch (options.AuthenticationMethod)
                {
                    case "PrivateKey":
                        return new SshClient(options.Url, options.Username, new PrivateKeyFile($"{AppContext.BaseDirectory}/{options.PrivateKeyPath}"));
                    case "Password":
                    default:
                        return new SshClient(options.Url, options.Username, options.Password);
                }
            });
            base.Load(builder);
        }
    }
}
