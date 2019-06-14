using System;
using Renci.SshNet;
namespace Jack.DataScience.Network.SSHTunneling
{
    public class SSHAPI
    {
        private readonly SshClient sshClient;

        public SSHAPI()
        {
             
        }


        public void ForwardPort(string hostLocal, int portLocal, string hostDestination, int portDestination)
        {
            sshClient.AddForwardedPort(new ForwardedPortLocal(hostLocal, (uint)portLocal, hostDestination, (uint)portDestination));

        }
    }
}
