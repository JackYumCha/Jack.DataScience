using System;
using System.Diagnostics;
using System.Reactive.Linq;
using System.Threading.Tasks;

namespace Jack.DataScience.ProcessExtensions
{
    public class ProcessExecutor
    {
        private readonly ProcessStartInfo processStartInfo;
        private Task runningTask;

        public ProcessExecutor(string path)
        {
            processStartInfo = new ProcessStartInfo()
            {
                RedirectStandardError = true,
                RedirectStandardOutput = true
            };
            processStartInfo.FileName = path;
        }

        public Process RunningProcess { get; private set; }

        public IObservable<string> StandardOutput { get; private set; }

        public IObservable<string> StandardError { get; private set; }

        public void AddArgument(string name)
        {
            processStartInfo.Arguments += name + " ";
        }

        public void AddArgument(string name, string argument)
        {
            processStartInfo.Arguments += name + " ";
            processStartInfo.Arguments += argument + " ";
        }

        public void Execute()
        {
            if (RunningProcess != null) return;
            StartProcess();
            RunningProcess.WaitForExit();
        }

        public Task ExecuteAsync()
        {
            if (RunningProcess != null) return runningTask;
            StartProcess();
            runningTask = new Task(() =>
            {
                RunningProcess.BeginOutputReadLine();
                RunningProcess.BeginErrorReadLine();
                RunningProcess.WaitForExit();
            });
            runningTask.Start();
            return runningTask;
        }

        private void StartProcess()
        {
            RunningProcess = Process.Start(processStartInfo);
            StandardOutput = Observable
                .FromEventPattern<DataReceivedEventHandler, DataReceivedEventArgs>(
                h => RunningProcess.OutputDataReceived += h,
                h => RunningProcess.OutputDataReceived -= h)
                .Select(e => e.EventArgs.Data);
            StandardError = Observable
                .FromEventPattern<DataReceivedEventHandler, DataReceivedEventArgs>(
                h => RunningProcess.ErrorDataReceived += h,
                h => RunningProcess.ErrorDataReceived -= h)
                .Select(e => e.EventArgs.Data);
            RunningProcess.BeginOutputReadLine();
            RunningProcess.BeginErrorReadLine();
        }
    }
}
