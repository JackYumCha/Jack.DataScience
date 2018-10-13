using System;
using System.IO;
using System.Text;
using System.Diagnostics;
using Jack.DataScience.Common;
using Autofac;
using Renci.SshNet;
using Xunit;

namespace Jack.DataScience.Storage.SFTP.Tests
{
    public class SFTPTests
    {
        [Theory(DisplayName = "Execute Command")]
        [InlineData("ls")]
        public void ExecuteCommand(string command)
        {
            AutoFacContainer container = new AutoFacContainer();
            container.RegisterOptions<SshOptions>();
            container.ContainerBuilder.RegisterModule<SshModule>();
            var servicesContainer = container.ContainerBuilder.Build();
            using (var client = servicesContainer.Resolve<SshClient>())
            {
                client.Connect();
                var result = client.CreateCommand(command).Execute();
                Debugger.Break();
                client.Disconnect();
            }
        }

        [Theory(DisplayName = "Upload String as File")]
        [InlineData("this is a text file")]
        public void UploadStringAsFile(string content)
        {
            AutoFacContainer container = new AutoFacContainer();
            container.RegisterOptions<SshOptions>();
            container.ContainerBuilder.RegisterModule<SshModule>();
            var servicesContainer = container.ContainerBuilder.Build();
            using (var client = servicesContainer.Resolve<SftpClient>())
            {
                client.Connect();
                using (MemoryStream stream = new MemoryStream(Encoding.UTF8.GetBytes(content)))
                {
                    client.UploadFile(stream, "./test.txt");
                    Debugger.Break();
                }
                client.Disconnect();
            }
        }

        [Theory(DisplayName = "Delete a File")]
        [InlineData("./test.txt")]
        public void DeleteAFile(string filename)
        {
            AutoFacContainer container = new AutoFacContainer();
            container.RegisterOptions<SshOptions>();
            container.ContainerBuilder.RegisterModule<SshModule>();
            var servicesContainer = container.ContainerBuilder.Build();
            using (var client = servicesContainer.Resolve<SftpClient>())
            {
                client.Connect();
                client.DeleteFile(filename);
                Debugger.Break();
                client.Disconnect();
            }
        }
    }
}
