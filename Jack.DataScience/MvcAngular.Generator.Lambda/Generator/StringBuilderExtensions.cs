using System.Text;

namespace MvcAngular.Generator
{
    internal static class StringBuilderExtensions
    {
        public static void AppendLf(this StringBuilder builder)
        {
            if (builder != null)
                builder.Append("\n");
        }
    }
}
