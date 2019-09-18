using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Threading.Tasks;

namespace Jack.DataScience.ProcessExtensions
{
    public class ProcessExecutor: IDisposable
    {
        private readonly ProcessStartInfo processStartInfo;
        private Task runningTask;
        private IDisposable StandardOutputSubscription;
        private IDisposable StandardErrorSubscription;

        public ProcessExecutor(string path)
        {
            processStartInfo = new ProcessStartInfo()
            {
                RedirectStandardError = true,
                RedirectStandardOutput = true
            };
            processStartInfo.FileName = path;
        }

        public ProcessStartInfo StartInfo { get => processStartInfo; }

        public Process RunningProcess { get; private set; }

        public Subject<string> StandardOutput { get; private set; } = new Subject<string>();

        public Subject<string> StandardError { get; private set; } = new Subject<string>();
        public Subject<int> OnExit { get; private set; } = new Subject<int>();
        public void AddArgument(string name)
        {
            processStartInfo.Arguments += name + " ";
        }

        public void AddArgument(string name, string argument)
        {
            processStartInfo.Arguments += name + " ";
            processStartInfo.Arguments += argument + " ";
        }

        public void AddArgument(string name, int argument)
        {
            processStartInfo.Arguments += name + " ";
            processStartInfo.Arguments += argument.ToString() + " ";
        }

        public void AddArgument(string name, double argument)
        {
            processStartInfo.Arguments += name + " ";
            processStartInfo.Arguments += argument.ToString() + " ";
        }

        public void AddArguments(IEnumerable<string> arguments)
        {
            foreach(var argument in arguments)
                processStartInfo.Arguments += argument + " ";
        }

        public void Dispose()
        {
            if (RunningProcess != null) RunningProcess.Dispose();
            if (StandardOutputSubscription != null) StandardOutputSubscription.Dispose();
            if (StandardErrorSubscription != null) StandardErrorSubscription.Dispose();
            StandardOutput.Dispose();
            StandardError.Dispose();
            OnExit.Dispose();
        }

        public void Execute()
        {
            if (RunningProcess != null) return;
            StartProcess();
            RunningProcess.WaitForExit();
            OnExit.OnNext(RunningProcess.ExitCode);
        }

        public Task ExecuteAsync()
        {
            if (RunningProcess != null) return runningTask;
            StartProcess();
            runningTask = new Task(() =>
            {
                RunningProcess.WaitForExit();
                OnExit.OnNext(RunningProcess.ExitCode);
            });
            runningTask.Start();
            return runningTask;
        }

        private void StartProcess()
        {
            RunningProcess = Process.Start(processStartInfo);
            StandardOutputSubscription =  Observable
                .FromEventPattern<DataReceivedEventHandler, DataReceivedEventArgs>(
                h => RunningProcess.OutputDataReceived += h,
                h => RunningProcess.OutputDataReceived -= h)
                .Select(e => e.EventArgs.Data)
                .Subscribe(value => StandardOutput.OnNext(value));
            StandardErrorSubscription = Observable
                .FromEventPattern<DataReceivedEventHandler, DataReceivedEventArgs>(
                h => RunningProcess.ErrorDataReceived += h,
                h => RunningProcess.ErrorDataReceived -= h)
                .Select(e => e.EventArgs.Data)
                .Subscribe(value => StandardError.OnNext(value));
            RunningProcess.BeginOutputReadLine();
            RunningProcess.BeginErrorReadLine();
        }

        public void TryKill()
        {
            try
            {
                if (!RunningProcess.HasExited)
                    RunningProcess.Kill();
            }
            catch(Exception ex) { }
        }
    }
}
