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
        [InlineData("this is another text file")]
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
                    client.UploadFile(stream, "./test2.txt");
                    Debugger.Break();
                }
                client.Disconnect();
            }
        }

        [Theory(DisplayName = "Delete a File")]
        [InlineData("./test.txt")]
        [InlineData("./test2.txt")]
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

        [Fact(DisplayName = "Download a File")]
        public void DownloadFile()
        {
            AutoFacContainer container = new AutoFacContainer();
            container.RegisterOptions<SshOptions>();
            container.ContainerBuilder.RegisterModule<SshModule>();
            var servicesContainer = container.ContainerBuilder.Build();
            using (var client = servicesContainer.Resolve<SftpClient>())
            {
                client.Connect();
                using(FileStream fs = new FileStream($"{AppContext.BaseDirectory}/downloaded.zip", FileMode.OpenOrCreate))
                {
                    client.DownloadFile("./4117.zip", fs);
                }

                using (MemoryStream ms = new MemoryStream())
                {
                    client.DownloadFile("./test2.txt", ms);
                    var text = Encoding.UTF8.GetString(ms.ToArray());
                    Debugger.Break();
                }
                client.Disconnect();
            }
        }
    }
}
