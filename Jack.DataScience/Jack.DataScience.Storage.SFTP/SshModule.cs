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
                return new SftpClient(options.Url, options.Username, options.Password);
            });
            builder.Register((context) =>
            {
                var options = context.Resolve<SshOptions>();
                return new SshClient(options.Url, options.Username, options.Password);
            });
            base.Load(builder);
        }
    }
}
