using System.Linq;

namespace MvcAngular.Generator
{
    internal static class ParameterExtensions
    {
        public static string ParseConsoleParameter(this string[] args, string command, params string[] alias)
        {
            if (alias == null)
                alias = new string[] { };
            int index = args.LastIndexOf(arg => arg.ToLower() == command.ToLower() || alias.Any(aliasName => arg.ToLower() == aliasName.ToLower()));
            return index > -1 && index < args.Length - 1 ? args[index + 1].Replace("\"", "") : null;
        }

        public static bool AssertConsoleParameter(this string[] args, string command, params string[] alias)
        {
            if (alias == null)
                alias = new string[] { };
            return args.Any(arg => arg.ToLower() == command.ToLower() || alias.Any(aliasName => arg.ToLower() == aliasName.ToLower()));
        }
    }
}
