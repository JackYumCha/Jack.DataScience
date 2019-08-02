using System;
using System.Collections.Generic;
using System.Text;
using SystemConsole = System.Console;

namespace Jack.DataScience.Common.Logging
{
    public class Console
    {
        public static GenericLogger Logger { get; set; }

        private static ConsoleError error;
        public static ConsoleError Error {
            get => new ConsoleError() { Logger = Logger };
        }
        public static void WriteLine(string value)
        {
            Logger?.Info(value);
        }

        public static void WriteLine(int value)
        {
            Logger?.Info($"{value}");
        }

        public static void WriteLine(bool value)
        {
            Logger?.Info($"{value}");
        }

        public static void WriteLine(float value)
        {
            Logger?.Info($"{value}");
        }

        public static void WriteLine(double value)
        {
            Logger?.Info($"{value}");
        }

        public static void WriteLine(byte value)
        {
            Logger?.Info($"{value}");
        }
    }

    public class ConsoleError
    {

        public GenericLogger Logger { get; set; }

        public void WriteLine(string value)
        {
            Logger?.Error(value);
        }

        public void WriteLine(int value)
        {
            Logger?.Error($"{value}");
        }

        public void WriteLine(bool value)
        {
            Logger?.Error($"{value}");
        }

        public void WriteLine(float value)
        {
            Logger?.Error($"{value}");
        }

        public void WriteLine(double value)
        {
            Logger?.Error($"{value}");
        }

        public void WriteLine(byte value)
        {
            Logger?.Error($"{value}");
        }
    }
}
