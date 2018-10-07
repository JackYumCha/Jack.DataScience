namespace MvcAngular.Generator
{
    /// <summary>
    /// RPC transpiler definition for appsettings.development.json
    /// </summary>
    internal class RPCSetting
    {
        /// <summary>
        /// the root folder for emitting the transpiled services and types
        /// </summary>
        public string Target { get; set; }
        /// <summary>
        /// the root folder for environments
        /// </summary>
        public string Environments { get; set; }
        /// <summary>
        /// the assemblies to scan for types and services
        /// </summary>
        public string[] Assemblies { get; set; }
    }
}