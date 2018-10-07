using System.Runtime.InteropServices;

namespace System.Runtime.Serialization.Formatters
{
    /// <summary>
    /// Patch for ns1.4
    /// </summary>
    [ComVisible(true)]
    public enum FormatterAssemblyStyle
    {
        /// <summary>
        /// Simple Style
        /// </summary>
        Simple = 0,
        /// <summary>
        /// Full Style
        /// </summary>
        Full = 1
    }
}