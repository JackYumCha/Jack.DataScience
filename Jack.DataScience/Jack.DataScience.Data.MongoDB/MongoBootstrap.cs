using System;
using System.IO;
using System.Diagnostics;
using System.Collections.Generic;
using System.Text;
using Jack.DataScience.Common;

namespace Jack.DataScience.Data.MongoDB
{
    public class MongoBootstrap
    {
        private readonly MongoBootstrapOptions bootstrapOptions;
        public MongoBootstrap(MongoBootstrapOptions bootstrapOptions)
        {
            if(bootstrapOptions.DataBaseDirectory == null || bootstrapOptions.DataBaseDirectory == "")
            {
                bootstrapOptions.DataBaseDirectory = AppContext.BaseDirectory;
            }
            this.bootstrapOptions = bootstrapOptions;
        }

    private string GetMongoBin()
        {
            var mongoBin = Environment.GetEnvironmentVariable("MongoBin");

            if (mongoBin == null || mongoBin == "") throw new Exception("Environment variable 'MongoBin' was not found! " +
                "Please add mongoDb bin path as 'MongoBin' in your environment variables.");
            return mongoBin;
        }


        public void CreateDatabase()
        {
            var mongoBin = GetMongoBin();
        
            DirectoryInfo databaseDir = new DirectoryInfo($"{bootstrapOptions.DataBaseDirectory}");
            if (!databaseDir.Exists) databaseDir.Create();
            DirectoryInfo dataDir = new DirectoryInfo($"{bootstrapOptions.DataBaseDirectory}/data");
            if (!dataDir.Exists) dataDir.Create();
            FileInfo configFile = new FileInfo($"{bootstrapOptions.DataBaseDirectory}/config.yaml");
            FileInfo logFile = new FileInfo($"{bootstrapOptions.DataBaseDirectory}/log.txt");

            ProcessStartInfo psi = new ProcessStartInfo()
            {
                FileName = $@"{mongoBin}/mongod.exe",
                Arguments = $" --install " +
                $"--serviceName {bootstrapOptions.ServiceName} " +
                $"--journal " +
                $"--dbpath \"{dataDir.FullName}\" " +
                $"--port {bootstrapOptions.Port} " +
                $"--logpath \"{logFile.FullName}\" " +
                $"--logappend " +
                $"--serviceDisplayName \"{bootstrapOptions.DisplayName}\" " +
                $"--serviceDescription \"{bootstrapOptions.DisplayName}\" " +
                //$"--serviceUser \"{bootstrapOptions.Username}\" " +
                //$"--servicePassword \"{bootstrapOptions.Password}\" " +
                $" --sslMode disabled",
                RedirectStandardError = false,
                RedirectStandardOutput = false,
                UseShellExecute = true,
                Verb = "runas"
            };

            var process = Process.Start(psi);
            process.WaitForExit();
            if(process.ExitCode!= 0)
            {
                //var error = process.StandardError.ReadToEnd();
                //var output = process.StandardOutput.ReadToEnd();
                throw new Exception("Failed to create MongoDB instance." );
            }
        }

        public void DeleteDatabase()
        {
            // "C:\Program Files\MongoDB\Server\3.4\bin\mongod.exe" --remove --serviceName sanshaData

            var mongoBin = GetMongoBin();

            ProcessStartInfo psi = new ProcessStartInfo()
            {
                FileName = $@"{mongoBin}/mongod.exe",
                Arguments = $@"--remove --serviceName {bootstrapOptions.ServiceName} ",
                RedirectStandardError = false,
                RedirectStandardOutput = false,
                UseShellExecute = true,
                Verb = "runas"
            };

            var process = Process.Start(psi);
            process.WaitForExit();
            if (process.ExitCode != 0)
            {
                //var error = process.StandardError.ReadToEnd();
                //var output = process.StandardOutput.ReadToEnd();
                throw new Exception("Failed to delete MongoDB instance.");
            }
        }
    }
}
