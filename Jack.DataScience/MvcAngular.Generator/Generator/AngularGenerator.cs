using System;
using System.IO;
using System.Text;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using System.Text.RegularExpressions;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Http;

namespace MvcAngular.Generator
{

    /// <summary>
    /// The angular service generator entry
    /// </summary>
    public class AngularGenerator
    {

        /// <summary>
        /// Use this at startup to generate if "--generate-angular" is passed to the command line.
        /// </summary>
        /// <param name="args"></param>
        /// <returns></returns>
        public static bool ShouldRunMvc(params string[] args)
        {
            if (args.AssertConsoleParameter("--generate-angular"))
            {
                string outputPath = args.ParseConsoleParameter("--output", "-o");
                if (outputPath == null)
                    outputPath = $"{AppContext.BaseDirectory}\\AngularCode";
                outputPath = outputPath.Replace("\\\\", "\\");
                if (!Directory.Exists(outputPath))
                    Directory.CreateDirectory(outputPath);
                string assemblyArg = args.ParseConsoleParameter("--assemblies", "-a");
                string[] assemblies = (assemblyArg == null) ? null : (assemblyArg.Split(new char[] { ',', ';' }, StringSplitOptions.RemoveEmptyEntries));
                GenerateClients(outputPath, assemblies);
                Console.ForegroundColor = ConsoleColor.White;
                return false;
            }
            return true;
        }

        /// <summary>
        /// Generate Angular Clients for Mvc Controllers and Types.
        /// </summary>
        /// <param name="rootPathCodeEmitting">The root folder where Angular 4 Transpiler Should Emit Code to.</param>
        /// <param name="rootPathAngularCLI">The root folder where Angular 4 CLI lives. It is used to obtain relative path reference to Barrel and Environments.</param>
        /// <param name="assemblyNamesToScan">The assembly names that Service generator should scan for AngularServiceAttribute.</param>
        internal static void GenerateClients(string rootPathCodeEmitting, string[] assemblyNamesToScan = null)
        {
            Dictionary<string, Assembly> loadedAssemblies = new Dictionary<string, Assembly>();

            Dictionary<string, string> assemblies = new Dictionary<string, string>();

            var assembliesInAppDomain = AppDomain.CurrentDomain.GetAssemblies();

            foreach (var assembly in assembliesInAppDomain)
            {
                loadedAssemblies.Add(assembly.GetName().Name, assembly);
                if (assemblyNamesToScan != null && assemblyNamesToScan.Length > 0)
                {
                    if (assemblyNamesToScan.Contains(assembly.GetName().Name))
                        assemblies.Add(assembly.GetName().Name, assembly.Location);
                }
            }

            var assembliedNotFound = new HashSet<string>();
            foreach (var assemblyName in assemblyNamesToScan.Except(loadedAssemblies.Keys).ToList())
            {
                if (assembliedNotFound.Add(assemblyName))
                {
                    try
                    {
                        var loadedAssembly = Assembly.Load(assemblyName);
                        if (loadedAssembly != null)
                        {
                            loadedAssemblies.Add(loadedAssembly.GetName().Name, loadedAssembly);
                            assemblies.Add(loadedAssembly.GetName().Name, loadedAssembly.Location);
                        }
                    }
                    catch (Exception ex)
                    {

                    }
                }
            }

            // fail quick rule, throw when no base path is provided.
            if (rootPathCodeEmitting == null) throw new Exception("A base path must be set for transpiling the JsonTye and Angular services in the appsettings.development.json");


            //the base folder or starting path, as we may need to emit to relative path
            var BaseDirectory = new DirectoryInfo(rootPathCodeEmitting);

            // load assembly dynamically


            // for validating the the angular service type is derived from ConrollerBase
            var controllerBaseType = typeof(ControllerBase);

            Dictionary<FileEmitResult, ConsoleColor> colorSettings = new Dictionary<FileEmitResult, ConsoleColor>()
            {
                { FileEmitResult.KeepOld, ConsoleColor.Green },
                { FileEmitResult.Changed, ConsoleColor.Magenta },
                { FileEmitResult.Created, ConsoleColor.Cyan },
                { FileEmitResult.Removed, ConsoleColor.Red },
            };

            Console.Write("Codes Emitting --> ");
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine(rootPathCodeEmitting);

            //Directory.SetCurrentDirectory(assembliesDirectory.FullName);

            Dictionary<string, bool> filesInAssembly = new Dictionary<string, bool>();

            foreach (var assemblyName in assemblies.Keys)
            {
                ScanAssemblyCodeFiles(filesInAssembly, assemblyName, rootPathCodeEmitting);
            }

            foreach (var assemblyName in assemblies.Keys)
            {
                Console.ForegroundColor = ConsoleColor.White;
                Console.Write("Transpiling Assembly : ");
                Console.ForegroundColor = ConsoleColor.Cyan;
                Console.WriteLine(assemblyName);


                // load assembly by assembly name
                foreach (var json in GetJsonTypes(loadedAssemblies[assemblyName]))
                {
                    //if (regexPatterns != null && !regexPatterns.Any(pattern => pattern.IsMatch(json.FullName)))
                    //    continue;

                    if (json.GetTypeInfo().IsEnum)
                    {
                        FileEmitResult result = TranspileJsonEnumType(json, BaseDirectory, assemblyName, string.Format(AutoGenratedCodeDeclaration, assemblyName, assemblies[assemblyName], DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.FFF")), filesInAssembly);
                        Console.ForegroundColor = ConsoleColor.White;
                        Console.Write($"\tTranspiling Enum Type : ");
                        Console.ForegroundColor = ConsoleColor.Yellow;
                        Console.Write(json.FullName);
                        Console.ForegroundColor = ConsoleColor.White;
                        Console.Write(" -> ");
                        Console.ForegroundColor = colorSettings[result];
                        Console.WriteLine($"{result}");
                    }
                    else
                    {
                        FileEmitResult result = TranspileJsonType(json, BaseDirectory, assemblyName, string.Format(AutoGenratedCodeDeclaration, assemblyName, assemblies[assemblyName], DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.FFF")), filesInAssembly);
                        Console.ForegroundColor = ConsoleColor.White;
                        Console.Write($"\tTranspiling Json Type : ");
                        Console.ForegroundColor = ConsoleColor.Yellow;
                        Console.Write(json.FullName);
                        Console.ForegroundColor = ConsoleColor.White;
                        Console.Write(" -> ");
                        Console.ForegroundColor = colorSettings[result];
                        Console.WriteLine($"{result}");
                    }
                }

                foreach (var service in GetAngularServices(loadedAssemblies[assemblyName]))
                {
                    //if (regexPatterns != null && !regexPatterns.Any(pattern => pattern.IsMatch(service.FullName)))
                    //    continue;

                    FileEmitResult result = TranspilieAngularService(service, controllerBaseType, rootPathCodeEmitting, BaseDirectory.FullName, assemblyName, string.Format(AutoGenratedCodeDeclaration, assemblyName, assemblies[assemblyName], DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.FFF")), filesInAssembly);
                    Console.ForegroundColor = ConsoleColor.White;
                    Console.Write($"\tTranspiling Service : ");
                    Console.ForegroundColor = ConsoleColor.Yellow;
                    Console.Write(service.FullName);
                    Console.ForegroundColor = ConsoleColor.White;
                    Console.Write(" -> ");
                    Console.ForegroundColor = colorSettings[result];
                    Console.WriteLine($"{result}");
                }
            }

            // delete files that are no longer in the back end
            foreach (string fileOfAssembly in filesInAssembly.Keys)
            {
                if (!filesInAssembly[fileOfAssembly])
                {
                    FileInfo fileInfo = new FileInfo(fileOfAssembly);
                    File.Delete(fileOfAssembly);
                    Console.ForegroundColor = ConsoleColor.White;
                    Console.Write($"\tValidating File : ");
                    Console.ForegroundColor = ConsoleColor.Yellow;
                    Console.Write(fileInfo.Name);
                    Console.ForegroundColor = ConsoleColor.White;
                    Console.Write(" -> ");
                    Console.ForegroundColor = colorSettings[FileEmitResult.Removed];
                    Console.WriteLine("Deleted");
                }
            }
        }

        /// <summary>
        /// iterator function to get all JsonTypeAttribute decorated types
        /// </summary>
        /// <param name="assembly"></param>
        /// <returns></returns>
        internal static IEnumerable<Type> GetJsonTypes(Assembly assembly)
        {
            Type[] types;
            try
            {
                types = assembly.DefinedTypes.ToArray();
            }
            catch (ReflectionTypeLoadException ex)
            {
                types = ex.Types.Where(t => t != null).ToArray();
            }

            foreach (Type type in types)
            {
                if (type.GetTypeInfo().GetCustomAttribute<AngularTypeAttribute>() != null) yield return type;
            }

        }

        /// <summary>
        /// iterator function to get all AngularServiceAttribute decorated types
        /// </summary>
        /// <param name="assembly"></param>
        /// <returns></returns>
        internal static IEnumerable<Type> GetAngularServices(Assembly assembly)
        {
            Type[] types;
            try
            {
                types = assembly.DefinedTypes.ToArray();
            }
            catch (ReflectionTypeLoadException ex)
            {
                types = ex.Types.Where(t => t != null).ToArray();
            }

            foreach (Type type in types)
            {
                if (type.GetTypeInfo().GetCustomAttribute<AngularAttribute>() != null) yield return type;
            }
        }

        /// <summary>
        /// helper function to triim file path for front end
        /// </summary>
        /// <param name="path"></param>
        /// <returns></returns>
        internal static string TrimFilePath(string path)
        {
            return ".\\" + path.Replace("\\", "/");
        }

        /// <summary>
        /// create folder for typescript file
        /// </summary>
        /// <param name="path"></param>
        /// <returns></returns>
        internal static bool CreateFoldersForFile(string path)
        {
            var fi = new FileInfo(path);
            var di = fi.Directory;
            if (!di.Exists) di.Create();
            return true;
        }

        /// <summary>
        /// calculate relative path for the typescript path reference in Angular service files.
        /// </summary>
        /// <param name="source"></param>
        /// <param name="reference"></param>
        /// <param name="referenceFile"></param>
        /// <returns></returns>
        internal static string CalculateRelativePath(string source, string reference, string referenceFile)
        {
            var sourceSections = source.Split(new char[] { '\\' }, StringSplitOptions.RemoveEmptyEntries);
            var referenceSections = reference.Split(new char[] { '\\' }, StringSplitOptions.RemoveEmptyEntries);
            int level = Math.Min(sourceSections.Length, referenceSections.Length);
            for (int i = 0; i < Math.Min(sourceSections.Length, referenceSections.Length); i++)
            {
                if (sourceSections[i] != referenceSections[i])
                {
                    level = i;
                    break;
                }
            }
            var stb = "";
            if (level >= sourceSections.Length)
            {
                //where reference is the same folder as source or sub folder of source
                stb = "./";
            }
            else
            {
                //where reference may be parent or branch from parent
                for (int i = level; i < sourceSections.Length; i++) stb += "../";
            }
            if (referenceSections.Length - level > 0) stb += String.Join("/", referenceSections.Skip(level)) + "/";
            stb += referenceFile;
            return stb.ToString();
        }

        /// <summary>
        /// Work out path name for JsonType and Angular Services for with controller conventions.
        /// By default, the "Controller" suffix of a service class is removed.
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        internal static string CalculateTypePath(Type type)
        {
            if (type.GetTypeInfo().GetCustomAttribute<AngularAttribute>() != null)
            {
                return "services";
            }
            else if (type.GetTypeInfo().GetCustomAttribute<AngularTypeAttribute>() != null)
            {
                if (type.IsEnum)
                {
                    return "enums";
                }
                else
                {
                    return "datatypes";
                }
            }
            else
            {
                return null;
            }
        }



        /// <summary>
        /// calculate the name for each type file
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        internal static string CalculateTypeFileName(Type type)
        {
            if (type.GetTypeInfo().GetCustomAttribute<AngularAttribute>() != null)
            {
                var name = type.FullName; //.Replace(".", "-");
                if (name.Length > 10 && name.EndsWith("controller", StringComparison.CurrentCultureIgnoreCase))
                {
                    name = name.Substring(0, name.Length - 10);
                }
                return name;
            }
            else if (type.GetTypeInfo().GetCustomAttribute<AngularTypeAttribute>() != null)
            {
                return type.FullName; //.Replace(".", "-");
            }
            else
            {
                return null;
            }
        }

        /// <summary>
        /// Unwrap the nullable type if the type is a nullable type
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        internal static Type UnwrapNullable(Type type)
        {
            if (type.IsConstructedGenericType && type.GetTypeInfo().GetGenericTypeDefinition() == typeof(Nullable<>))
            {
                return type.GetTypeInfo().GenericTypeArguments.First();
            }
            else
            {
                return type;
            }
        }

        /// <summary>
        /// Check whether a type is a valid http get action parameter
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        internal static bool IsValidGetParameterType(Type type)
        {
            type = UnwrapNullable(type);

            // add type support for enum
            if (type.GetTypeInfo().IsEnum) return true;

            switch (type.FullName)
            {
                //the following types are plain types which can be used directly in the get arguments.
                case "System.String":
                case "System.Guid":
                case "System.Int16":
                case "System.Int32":
                case "System.Int64":
                case "System.UInt16":
                case "System.UInt32":
                case "System.UInt64":
                case "System.Single":
                case "System.Double":
                case "System.Decimal":
                case "System.Boolean":
                case "System.DateTime":
                case "System.DateTimeOffset":
                    return true;

                //all generic types or arrays shall be considered as complex type, where [JsonParameter(type)] shall be used with a string parameter.
                default:
                    return false;
            }
        }

        /// <summary>
        /// this function tells which of the types should be treated as plain types by the plain post method.
        /// Please update this function according to the TypeMapping method below.
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        internal static bool IsPlainType(Type type)
        {
            type = UnwrapNullable(type);

            // add support to enum type
            if (type.GetTypeInfo().IsEnum) return true;

            switch (type.FullName)
            {
                //the following types are plain types which can be used directly in the plain post arguments.
                case "System.String":
                case "System.Guid":
                case "System.Int16":
                case "System.Int32":
                case "System.Int64":
                case "System.UInt16":
                case "System.UInt32":
                case "System.UInt64":
                case "System.Single":
                case "System.Double":
                case "System.Decimal":
                case "System.Boolean":
                case "System.DateTime":
                case "System.DateTimeOffset":
                case "Microsoft.AspNetCore.Http.IFormFile":
                    return true;

                //all generic types or arrays shall be considered as complex type, where [JsonParameter(type)] shall be used with a string parameter.
                default:
                    return false;
            }
        }

        /// <summary>
        /// Handle type mappings from back end to front end.
        /// </summary>
        /// <param name="type"></param>
        /// <param name="imports"></param>
        /// <param name="jsons"></param>
        /// <param name="currentPath"></param>
        /// <returns></returns>
        internal static string TypeMapping(Type type, HashSet<string> imports, Type host)
        {
            type = UnwrapNullable(type);

            var currentPath = CalculateTypePath(host);

            // add type support for enum
            if (type.GetTypeInfo().IsEnum)
            {
                //add type reference:
                var returnPath = CalculateTypePath(type);
                var returnFile = CalculateTypeFileName(type);
                if (returnPath == null || returnFile == null)
                {
                    Debug.WriteLine(String.Format("*** Warning ! Type {0} does not have JsonTypeAttribute. Please attach JsonTypeAttribute to it for Angular 2 consumption!", type.FullName));
                    throw new Exception(String.Format("*** Warning ! Type {0} does not have JsonTypeAttribute. Please attach JsonTypeAttribute to it for Angular 2 consumption!", type.FullName));
                    //return "any";
                }
                else
                {
                    // we don't need to import the type itself in its own definition file:
                    imports.Add(String.Format(@"import {{ {0} }} from '{1}';", type.Name, CalculateRelativePath(currentPath, returnPath, returnFile)));
                    return type.Name;
                }
            }

            switch (type.FullName)
            {
                // by default NewtonSoft.Json convert the following types to string in JSON.
                case "System.String":
                case "System.DateTime":
                case "System.DateTimeOffset":
                case "System.Guid":
                    return "string";

                //by default NewtonSoft.Json convert the following types into number in JSON.
                case "System.Int16":
                case "System.Int32":
                case "System.Int64":
                case "System.UInt16":
                case "System.UInt32":
                case "System.UInt64":
                case "System.Single":
                case "System.Double":
                case "System.Decimal":
                    return "number";

                //by default NewtonSoft.Json convert the following types into boolean in JSON.
                case "System.Boolean":
                    return "boolean";
                case "System.Void":
                    return "Response";

                //the front end shall use File to post a file or blob to the back end. in the back end MVC use IFormFile to handle posted file.
                //this is the way we map backend argument to front end. it shall not be used for return types
                case "Microsoft.AspNetCore.Http.IFormFile":
                    return "File";
                default:
                    //handle generic types and array types

                    //Map Task<T> to T
                    if (type.IsConstructedGenericType &&
                            (
                                type.GetGenericTypeDefinition() == typeof(Task<>)
                            )
                        )
                    {
                        var ItemType = type.GetGenericArguments().First();
                        //return the Array for the host type.
                        return String.Format("{0}", TypeMapping(ItemType, imports, host));
                    }

                    //Map Generic Collection Types List and Hashset to []
                    if (type.IsConstructedGenericType &&
                            (
                                type.GetGenericTypeDefinition() == typeof(List<>) ||
                                type.GetGenericTypeDefinition() == typeof(HashSet<>)
                            )
                        )
                    {
                        var ItemType = type.GetGenericArguments().First();
                        //return the Array for the host type.
                        return String.Format("{0}[]", TypeMapping(ItemType, imports, host));
                    }

                    //Map Dictionary<TKey,TValue> to { [key: number|string]: <TValue> }
                    else if (type.IsConstructedGenericType &&
                            (
                                type.GetGenericTypeDefinition() == typeof(Dictionary<,>)
                            )
                        )
                    {
                        var genericArguments = type.GetGenericArguments();
                        var keyType = genericArguments[0];
                        var valueType = genericArguments[1];
                        var keyTypeTypeScript = TypeMapping(keyType, imports, host);
                        var valueTypeTypeScript = TypeMapping(valueType, imports, host);

                        if (keyTypeTypeScript != "string" && keyTypeTypeScript != "number")
                            throw new Exception(String.Format("The Key type \"{0}\" of Dictionary<{0},{1}> is not supported by TypeScript. Key type must be either string or number/int/long/double/float/decimal. Key type is mapped to {2} currently.", keyType.Name, valueType.Name, keyTypeTypeScript));

                        return string.Format("{{ [key: {0}]: {1} }}", keyTypeTypeScript, valueTypeTypeScript);
                    }



                    //Map Array to []
                    else if (type.IsArray)
                    {
                        var elementType = type.GetTypeInfo().GetElementType();
                        return String.Format("{0}[]", TypeMapping(elementType, imports, host));
                    }

                    //Map JsonResult<T> to T
                    else if (type.IsConstructedGenericType && type.GetGenericTypeDefinition() == typeof(JsonTypeResult<>)) // type.FullName.StartsWith("Microsoft.AspNetCore.Mvc.JsonResult`1"))
                    {
                        var jsonReturnType = type.GetGenericArguments().First();
                        //look up the type from the Json List

                        //Shall call TypeMapping again in case it is string, int, bool, etc;
                        return TypeMapping(jsonReturnType, imports, host);
                    }

                    //Map a type T to T as well as 
                    else
                    {
                        //look at the types in parameters;
                        var returnPath = CalculateTypePath(type);
                        var returnFile = CalculateTypeFileName(type);
                        if (returnPath == null || returnFile == null)
                        {
                            Debug.WriteLine(String.Format("*** Warning ! Type {0} does not have JsonTypeAttribute. Please attach JsonTypeAttribute to it for Angular 2 consumption!", type.FullName));
                            throw new Exception(String.Format("*** Warning ! Type {0} does not have JsonTypeAttribute. Please attach JsonTypeAttribute to it for Angular 2 consumption!", type.FullName));
                            //return "any";
                        }
                        else
                        {
                            // we don't need to import the type itself in its own definition file:
                            if (type != host)
                            {
                                imports.Add(String.Format(@"import {{ {0} }} from '{1}';", type.Name, CalculateRelativePath(currentPath, returnPath, returnFile)));
                            }
                            //return the type name
                            return type.Name;
                        }
                    }
                    //break;
            }
            //return type.FullName;
        }

        /// <summary>
        /// convert name to camelCase for TypeScript
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        internal static string CamelCase(string value)
        {
            return value;

            //if (value != null && value.Length > 0)
            //{
            //    return value.Substring(0, 1).ToLower() + value.Substring(1);
            //}
            //else
            //{
            //    return value;
            //}
        }

        /// <summary>
        /// internal function to transpile parameters for Mvc method
        /// </summary>
        /// <param name="parameters"></param>
        /// <param name="imports"></param>
        /// <param name="jsons"></param>
        /// <param name="currentPath"></param>
        /// <returns></returns>
        internal static string ResolveParameters(ParameterInfo[] parameters, HashSet<string> imports, Type host)
        {
            return String.Join(", ", parameters.Select(p => {
                var attr = p.GetCustomAttribute<JsonPostParameterAttribute>();
                if (attr != null)
                {
                    return String.Format("{0}: {1}", p.Name, TypeMapping(attr.Type, imports, host));
                }
                else
                {
                    return String.Format("{0}: {1}", p.Name, TypeMapping(p.ParameterType, imports, host));
                }
            }));
        }

        internal static Type FileListType = typeof(IEnumerable<IFormFile>);

        /// <summary>
        /// Determine the method type and work out potential errors;
        /// </summary>
        /// <param name="method"></param>
        /// <returns></returns>
        internal static MethodTypeEnum GetMethodType(MethodInfo method)
        {
            if (method.GetCustomAttributes().Where(attr => attr.GetType() == typeof(HttpGetAttribute)).Any())
            {
                if ((method.ReturnType == typeof(FileStreamResult)) || (method.ReturnType == typeof(FileResult)) || (method.ReturnType == typeof(FileContentResult)))
                {
                    if (method.GetParameters().Where(param => !IsValidGetParameterType(param.ParameterType)).Any())
                    {
                        return MethodTypeEnum.IllegalGetParameter;
                    }
                    else
                    {
                        return MethodTypeEnum.FileGet;
                    }
                }
                else
                {
                    if (method.GetParameters().Length == 0)
                    {
                        return MethodTypeEnum.EmptyGet;
                    }
                    else
                    {
                        if (method.GetParameters().Where(param => !IsValidGetParameterType(param.ParameterType)).Any())
                        {
                            return MethodTypeEnum.IllegalGetParameter;
                        }
                        else
                        {
                            return MethodTypeEnum.PlainGet;
                        }
                    }
                }
            }
            else if (method.GetCustomAttributes().Where(attr => attr.GetType() == typeof(HttpPostAttribute)).Any())
            {
                var methodParameters = method.GetParameters();
                if (methodParameters.Length == 1 && FileListType.IsAssignableFrom(methodParameters[0].ParameterType))
                {
                    return MethodTypeEnum.MultiFilePost;
                }
                else if (method.GetParameters().Where(param => !IsPlainType(param.ParameterType)).Any())
                {
                    //there is complex type;

                    if (method.GetParameters().Length > 1)
                    {
                        return MethodTypeEnum.MultipleComplexError;
                    }
                    else
                    {
                        if (method.GetParameters().First().CustomAttributes.Where(attr => attr.AttributeType == typeof(FromBodyAttribute)).Any())
                        {
                            return MethodTypeEnum.ComplexPost;
                        }
                        else
                        {
                            return MethodTypeEnum.NotFromBodyComplexError;
                        }
                    }
                }
                else
                {
                    if (method.GetParameters().Length == 0)
                    {
                        return MethodTypeEnum.EmptyPost;
                    }
                    else
                    {
                        return MethodTypeEnum.PlainPost;
                    }

                }
            }
            {
                return MethodTypeEnum.None;
            }
        }

        internal static Regex regexCodeAssembly = new Regex(@"[\n\r]+ \* Assembly Name: ""([\w\.]+)""[\n\r]+");

        internal static Dictionary<string, bool> ScanAssemblyCodeFiles(Dictionary<string, bool> files, string assemblyName, string rootPathCodeEmitting)
        {
            ScanAssemblyCodeFiles(assemblyName, new DirectoryInfo($@"{rootPathCodeEmitting}"), files);
            return files;
        }

        internal static void ScanAssemblyCodeFiles(string assemblyName, DirectoryInfo directoryInfo, Dictionary<string, bool> files)
        {
            foreach (FileInfo fi in directoryInfo.GetFiles())
            {
                string code = File.ReadAllText(fi.FullName);
                Match assemblyMatch = regexCodeAssembly.Match(code);
                if (assemblyMatch.Success && assemblyMatch.Groups[1].Value == assemblyName)
                {
                    files.Add(fi.FullName, false);
                }
            }

            foreach (DirectoryInfo di in directoryInfo.GetDirectories())
            {
                ScanAssemblyCodeFiles(assemblyName, di, files);
            }
        }



        internal static string AutoGenratedCodeDeclaration = "/** \n * Auto Generated Code\n * Please do not modify this file manually \n * Assembly Name: \"{0}\"\n * Source Type: \"{1}\"\n * Generated At: {2}\n */\n";

        internal static string TsImports = "import { Injectable } from '@angular/core';\nimport { HttpClient } from '@angular/common/http';\nimport { Observable } from 'rxjs';\n";
        //import { Observable } from 'rxjs/Observable';\nimport { Observer } from 'rxjs/Observer';\n

        internal static FileEmitResult CompareAndWriteFile(string filename, string newCode)
        {
            if (File.Exists(filename))
            {
                string oldCode = File.ReadAllText(filename, Encoding.UTF8);
                List<string> oldLines = oldCode.Split(new char[] { '\n', '\r' }, StringSplitOptions.RemoveEmptyEntries).Skip(8).ToList();
                List<string> newLines = newCode.Split(new char[] { '\n', '\r' }, StringSplitOptions.RemoveEmptyEntries).Skip(8).ToList();

                bool shouldOverwrite = oldLines.Count != newLines.Count;

                if (!shouldOverwrite)
                {
                    for (int i = 0; i < oldLines.Count; i++)
                    {
                        if (oldLines[i] != newLines[i])
                        {
                            shouldOverwrite = true;
                            break;
                        }
                    }
                }

                if (shouldOverwrite)
                {
                    File.WriteAllText(filename, newCode, Encoding.UTF8);
                    return FileEmitResult.Changed;
                }
                else
                    return FileEmitResult.KeepOld;
            }
            else
            {
                File.WriteAllText(filename, newCode, Encoding.UTF8);
                return FileEmitResult.Created;
            }

        }

        internal static FileEmitResult TranspilieAngularService(Type service, Type controllerBaseType, string rootPathCodeEmitting, string baseDirectory, string assemblyName, string AutoGenratedCodeDeclaration, Dictionary<string, bool> filesInAssembly)
        {
            // throw error if type is assignable from ControllerBase
            if (!controllerBaseType.IsAssignableFrom(service))
                throw new Exception(String.Format("{0} is invalid angular service type. The class with Angular Service attribute must be derived from Microsoft.AspNetCore.Mvc.ControllerBase.", service.FullName));

            //get the route attribute from the service type
            var route = service.GetTypeInfo().GetCustomAttribute<RouteAttribute>();

            //set up the route based on the route attribue of the controller
            string baseroute = Regex.Replace(service.Name, "controller$", "", RegexOptions.IgnoreCase);

            if (route != null)
            {
                baseroute = String.Format(route.Template.Replace("[controller]", "{0}"), baseroute);
            }

            // create string builder for building code
            var stb = new StringBuilder();

            // create hashset for adding data type imports. hashset ensures each type reference is only added once.
            var imports = new HashSet<string>();

            //code to import
            stb.Append(TsImports);

            //add type comments/descriptions
            service.GetTypeInfo().GetCustomAttributes<DescriptionAttribute>().Select(att =>
            {
                stb.AppendFormat("/** {0} */", att.Description); stb.AppendLf();
                return true;
            }).ToArray();

            // code for the beginning of the service class
            stb.AppendFormat("@Injectable()", service.Namespace); stb.AppendLf();
            stb.AppendFormat("export class {0} {{", string.Format("{0}{1}", Regex.Replace(service.Name, "controller$", "", RegexOptions.IgnoreCase), "Service")); stb.AppendLf();

            // inject Http service from @angular/core in the contructor
            stb.Append("\tconstructor(private $httpClient: HttpClient) {{}}"); stb.AppendLf();

            // use the environment.BASE_API_URL from the front end project environment folder
            stb.Append($"\tpublic $baseURL: string = '<{assemblyName}>';"); stb.AppendLf();

            foreach (var method in service.GetMethods())
            {
                // ignore the method if the ignore attribute is shown
                if (method.GetCustomAttributes<AngularIgnoreAttribute>().Any())
                    continue;

                // code generator for different type of methods
                switch (GetMethodType(method))
                {
                    case MethodTypeEnum.EmptyGet:
                        TranspileEmptyGetMethod(service, method, stb, baseroute, imports);
                        break;
                    case MethodTypeEnum.PlainGet:
                        TranspilePlainGetMethod(service, method, stb, baseroute, imports);
                        break;
                    case MethodTypeEnum.FileGet:
                        TranspileFileGetMethod(service, method, stb, baseroute, imports);
                        break;
                    case MethodTypeEnum.EmptyPost:
                        TranspileEmptyPostMethod(service, method, stb, baseroute, imports);
                        break;
                    case MethodTypeEnum.PlainPost:
                        TranspilePlainPostMethod(service, method, stb, baseroute, imports);
                        break;
                    case MethodTypeEnum.ComplexPost:
                        TranspileComplexPostMethod(service, method, stb, baseroute, imports);
                        break;
                    case MethodTypeEnum.MultiFilePost:
                        TranspileMultiFilePostMethod(service, method, stb, baseroute, imports);
                        break;
                    case MethodTypeEnum.MultipleComplexError:
                        throw new Exception(String.Format("**** MvcAngular Transpiler Error: {0}.{1} has multiple complex type parameters. MVC only allows single complex type. Or try to use [JsonParameter(typeof(CustomType))] with string when you have to use multiple complex parameters.", service.FullName, method.Name));
                    case MethodTypeEnum.NotFromBodyComplexError:
                        throw new Exception(String.Format("**** MvcAngular Transpiler Error: {0}.{1} is complex type parameter. Please add [FromBody] attribute to the parameter.", service.FullName, method.Name));
                    case MethodTypeEnum.IllegalGetParameter:
                        throw new Exception(String.Format("**** MvcAngular Transpiler Error: {0}.{1} contains an illegal parameter type for http get method. Please make sure all parameters in get method must be plain type that can be converted to string and vice versa.", service.FullName, method.Name));
                }
            }

            //end of the class
            stb.AppendFormat("}}"); stb.AppendLf();

            //Insert the imports
            stb.Insert(0, String.Join("\n", imports) + "\n");

            var filename = String.Format("{0}\\{1}\\{2}.Service.ts", baseDirectory, CalculateTypePath(service), CalculateTypeFileName(service));

            //insert warning and declarations
            stb.Insert(0, AutoGenratedCodeDeclaration);

            CreateFoldersForFile(filename);

            if (filesInAssembly.ContainsKey(filename))
                filesInAssembly[filename] = true;

            //As suggested, we should use UTF-8 encoding for all codes;
            //File.WriteAllText(filename, stb.ToString(), Encoding.UTF8);
            return CompareAndWriteFile(filename, stb.ToString());


        }

        internal static FileEmitResult TranspileJsonType(Type json, DirectoryInfo BaseDirectory, string assemblyName, string AutoGenratedCodeDeclaration, Dictionary<string, bool> filesInAssembly)
        {
            //get all properties
            var properties = json.GetProperties();

            //create the stringbuiler
            var stb = new StringBuilder();

            //create the imports hashset; hashset ensures each type reference is only added once
            var imports = new HashSet<string>();

            //add type comments/descriptions
            json.GetTypeInfo().GetCustomAttributes<DescriptionAttribute>().Select(att =>
            {
                stb.AppendFormat("/** {0} */", att.Description); stb.AppendLf();
                return true;
            }).ToArray();

            //beginning of the interface
            stb.AppendFormat("export interface {0} {{", json.Name); stb.AppendLf();

            foreach (var property in properties)
            {
                // skip this property if angular ignore is available
                if (property.GetCustomAttributes<AngularIgnoreAttribute>().Any())
                    continue;

                //add property comments/descriptions
                property.GetCustomAttributes<DescriptionAttribute>().Select(att =>
                {
                    stb.AppendFormat("\t/** {0} */", att.Description); stb.AppendLf();
                    return true;
                }).ToArray();

                var forceTypes = property.GetCustomAttributes<AngularForceTypeAttribute>().ToList();
                if (forceTypes.Any())
                {
                    stb.AppendFormat("\t{0}?: {1};", CamelCase(property.Name), forceTypes.First().CustomizedType); stb.AppendLf();
                }
                else
                {
                    //declaration for each property. I added ? to make property optional to avoid typescript compiler errors when not all fields are set.
                    stb.AppendFormat("\t{0}?: {1};", CamelCase(property.Name), TypeMapping(property.PropertyType, imports, json)); stb.AppendLf();
                }
            }

            //end of the interface
            stb.AppendFormat("}}"); stb.AppendLf();

            //insert the import to the file
            stb.Insert(0, String.Join("\n", imports) + "\n");


            //insert warning and declarations
            stb.Insert(0, AutoGenratedCodeDeclaration);

            // work out the file name
            var filename = String.Format("{0}\\{1}\\{2}.ts", BaseDirectory.FullName, CalculateTypePath(json), CalculateTypeFileName(json));

            // create folder in case the folder does not exist
            CreateFoldersForFile(filename);

            if (filesInAssembly.ContainsKey(filename))
                filesInAssembly[filename] = true;

            //As suggested, we should use UTF-8 encoding for all codes;
            //File.WriteAllText(filename, stb.ToString(), Encoding.UTF8);
            return CompareAndWriteFile(filename, stb.ToString());
        }

        internal static FileEmitResult TranspileJsonEnumType(Type json, DirectoryInfo BaseDirectory, string assemblyName, string AutoGenratedCodeDeclaration, Dictionary<string, bool> filesInAssembly)
        {
            //get all properties
            var fields = json.GetFields().Where(typeField => typeField.IsStatic);

            //create the stringbuiler
            var stb = new StringBuilder();

            //create the imports hashset; hashset ensures each type reference is only added once
            //enum type is pure string, it does not need to import anything actually.
            var imports = new HashSet<string>();

            //add type comments/descriptions
            json.GetTypeInfo().GetCustomAttributes<DescriptionAttribute>().Select(att =>
            {
                stb.AppendFormat("/** {0} */", att.Description); stb.AppendLf();
                return true;
            }).ToArray();

            //beginning of the interface
            stb.AppendFormat("export type {0} = {1};", json.Name, String.Join("|", fields.Select(enumItem => String.Format("'{0}'", enumItem.Name)))); stb.AppendLf();
            stb.AppendLf();
            stb.Append("declare global{"); stb.AppendLf();
            stb.Append("\tinterface Number{"); stb.AppendLf();
            stb.Append($"\t\tto{json.Name} (): {json.Name};"); stb.AppendLf();
            stb.Append("\t}"); stb.AppendLf();
            stb.Append("}"); stb.AppendLf();
            stb.AppendLf();

            stb.Append($"export class {json.Name}Converter extends Number {{"); stb.AppendLf();
            stb.Append($"\tpublic static convert (value: number): {json.Name} {{"); stb.AppendLf();
            stb.Append("\t\tswitch(value){"); stb.AppendLf();
            foreach (var field in fields)
            {
                stb.Append($"\t\t\tcase {(int)field.GetValue(null)}:\n\t\t\t\treturn '{field.Name}';\n");
            }
            stb.Append("\t\t}"); stb.AppendLf();
            stb.Append("\t}"); stb.AppendLf();
            stb.Append($"\tpublic static parse (value: string): number | undefined {{"); stb.AppendLf();
            stb.Append("\t\tswitch(value){"); stb.AppendLf();
            foreach (var field in fields)
            {
                stb.Append($"\t\t\tcase '{field.Name}':\n\t\t\t\treturn {(int)field.GetValue(null)};\n");
            }
            stb.Append("\t\t}"); stb.AppendLf();
            stb.Append("\t\treturn undefined;"); stb.AppendLf();
            stb.Append("\t}"); stb.AppendLf();
            stb.Append($"\tpublic static all: {json.Name}[] = [{(string.Join(", ", fields.Select(field => $"'{field.Name}'")))}];"); stb.AppendLf();
            stb.Append("}"); stb.AppendLf();
            stb.AppendLf();

            stb.Append($"class {json.Name}Extensions extends Number {{"); stb.AppendLf();
            stb.Append($"\tpublic to{json.Name} (): {json.Name} {{"); stb.AppendLf();
            stb.Append($"\t\treturn {json.Name}Converter.convert(<any>this);"); stb.AppendLf();
            stb.Append("\t}"); stb.AppendLf();
            stb.Append("}"); stb.AppendLf();
            stb.AppendLf();

            stb.Append($"Number.prototype.to{json.Name} = {json.Name}Extensions.prototype.to{json.Name};"); stb.AppendLf();
            stb.AppendLf();

            stb.Append($"export module {json.Name} {{"); stb.AppendLf();
            foreach (var field in fields)
            {
                stb.Append($"\texport const {field.Name} = '{field.Name}';"); stb.AppendLf();
            }
            stb.Append($"}}"); stb.AppendLf();
            //insert the import to the file
            stb.Insert(0, String.Join("\n", imports) + "\n");


            //insert warning and declarations
            stb.Insert(0, AutoGenratedCodeDeclaration);

            // work out the file name
            var filename = String.Format("{0}\\{1}\\{2}.ts", BaseDirectory.FullName, CalculateTypePath(json), CalculateTypeFileName(json));

            // create folder in case the folder does not exist
            CreateFoldersForFile(filename);

            if (filesInAssembly.ContainsKey(filename))
                filesInAssembly[filename] = true;

            //As suggested, we should use UTF-8 encoding for all codes;
            //File.WriteAllText(filename, stb.ToString(), Encoding.UTF8);
            return CompareAndWriteFile(filename, stb.ToString());
        }


        /// <summary>
        /// transpile the empty get action with no arguments
        /// </summary>
        /// <param name="service"></param>
        /// <param name="method"></param>
        /// <param name="stb"></param>
        /// <param name="baseroute"></param>
        /// <param name="imports"></param>
        internal static void TranspileEmptyGetMethod(Type service, MethodInfo method, StringBuilder stb, string baseroute, HashSet<string> imports)
        {
            method.GetCustomAttributes<DescriptionAttribute>().Select(att =>
            {
                stb.AppendFormat("\t/** {0} */", att.Description); stb.AppendLf();
                return true;
            }).ToArray();
            //check plain values only
            var forceTypes = method.GetCustomAttributes<AngularForceTypeAttribute>().ToList();
            if (forceTypes.Any())
            {
                stb.AppendFormat("\tpublic {0}({1}): Observable<{2}> {{",
                    CamelCase(method.Name), ResolveParameters(method.GetParameters(), imports, service),
                    forceTypes.First().CustomizedType); stb.AppendLf();
                stb.AppendFormat("\t\treturn this.$httpClient.get<{1}>(this.$baseURL + '{0}', {{}});",
                    baseroute.Replace("[action]", method.Name),
                    TypeMapping(method.ReturnType, imports, service)
                    ); stb.AppendLf();
                stb.AppendFormat("\t}}"); stb.AppendLf();
            }
            else if (method.ReturnType.FullName == "System.Void")
            {
                stb.AppendFormat("\tpublic {0}({1}): Observable<Response> {{",
                    CamelCase(method.Name), ResolveParameters(method.GetParameters(), imports, service),
                    TypeMapping(method.ReturnType, imports, service)); stb.AppendLf();
                stb.AppendFormat("\t\treturn this.$httpClient.get<{1}>(this.$baseURL + '{0}', {{}});",
                    baseroute.Replace("[action]", method.Name),
                    TypeMapping(method.ReturnType, imports, service)
                    ); stb.AppendLf();
                stb.AppendFormat("\t}}"); stb.AppendLf();
            }
            else
            {
                stb.AppendFormat("\tpublic {0}({1}): Observable<{2}> {{",
                    CamelCase(method.Name), ResolveParameters(method.GetParameters(), imports, service),
                    TypeMapping(method.ReturnType, imports, service)); stb.AppendLf();
                stb.AppendFormat("\t\treturn this.$httpClient.get<{1}>(this.$baseURL + '{0}', {{}});",
                    baseroute.Replace("[action]", method.Name),
                    TypeMapping(method.ReturnType, imports, service)
                    ); stb.AppendLf();
                stb.AppendFormat("\t}}"); stb.AppendLf();
            }

        }

        /// <summary>
        /// transpile the plain get action
        /// </summary>
        /// <param name="service"></param>
        /// <param name="method"></param>
        /// <param name="stb"></param>
        /// <param name="baseroute"></param>
        /// <param name="imports"></param>
        internal static void TranspilePlainGetMethod(Type service, MethodInfo method, StringBuilder stb, string baseroute, HashSet<string> imports)
        {
            method.GetCustomAttributes<DescriptionAttribute>().Select(att =>
            {
                stb.AppendFormat("\t/** {0} */", att.Description); stb.AppendLf();
                return true;
            }).ToArray();
            //check plain values only

            var forceTypes = method.GetCustomAttributes<AngularForceTypeAttribute>().ToList();
            if (forceTypes.Any())
            {
                var paramaters = method.GetParameters().Select(param => String.Format("{0}=${{encodeURIComponent(String({0}))}}", param.Name));
                string query = "";
                stb.AppendFormat("\tpublic {0}({1}): Observable<{2}> {{",
                CamelCase(method.Name), ResolveParameters(method.GetParameters(), imports, service),
                forceTypes.First().CustomizedType); stb.AppendLf();
                if (paramaters.Any())
                {
                    query = "?" + String.Join("&", paramaters);
                }
                stb.AppendFormat("\t\treturn this.$httpClient.get<{2}>(this.$baseURL + '{0}' + `{1}`, {{}});",
                    baseroute.Replace("[action]", method.Name),
                    query,
                    TypeMapping(method.ReturnType, imports, service)
                    ); stb.AppendLf();
                stb.AppendFormat("\t}}"); stb.AppendLf();
            }
            else if (method.ReturnType.FullName == "System.Void")
            {
                var paramaters = method.GetParameters().Select(param => String.Format("{0}=${{encodeURIComponent(String({0}))}}", param.Name));
                string query = "";
                stb.AppendFormat("\tpublic {0}({1}): Observable<Response> {{",
                CamelCase(method.Name), ResolveParameters(method.GetParameters(), imports, service),
                TypeMapping(method.ReturnType, imports, service)); stb.AppendLf();
                if (paramaters.Any())
                {
                    query = "?" + String.Join("&", paramaters);
                }
                stb.AppendFormat("\t\treturn this.$httpClient.get<{2}>(this.$baseURL + '{0}' + `{1}`, {{}});",
                    baseroute.Replace("[action]", method.Name),
                    query,
                    TypeMapping(method.ReturnType, imports, service)
                    ); stb.AppendLf();
                stb.AppendFormat("\t}}"); stb.AppendLf();
            }
            else
            {
                var paramaters = method.GetParameters().Select(param => String.Format("{0}=${{encodeURIComponent(String({0}))}}", param.Name));
                string query = "";
                stb.AppendFormat("\tpublic {0}({1}): Observable<{2}> {{",
                CamelCase(method.Name), ResolveParameters(method.GetParameters(), imports, service),
                TypeMapping(method.ReturnType, imports, service)); stb.AppendLf();
                if (paramaters.Any())
                {
                    query = "?" + String.Join("&", paramaters);
                }
                stb.AppendFormat("\t\treturn this.$httpClient.get<{2}>(this.$baseURL + '{0}' + `{1}`, {{}});",
                    baseroute.Replace("[action]", method.Name),
                    query,
                    TypeMapping(method.ReturnType, imports, service)
                    ); stb.AppendLf();
                stb.AppendFormat("\t}}"); stb.AppendLf();
            }

        }

        /// <summary>
        /// transpile the file get action, where a file is returned, the generated method will be simply a URL builder for the link.
        /// </summary>
        /// <param name="service"></param>
        /// <param name="method"></param>
        /// <param name="stb"></param>
        /// <param name="baseroute"></param>
        /// <param name="imports"></param>
        internal static void TranspileFileGetMethod(Type service, MethodInfo method, StringBuilder stb, string baseroute, HashSet<string> imports)
        {
            method.GetCustomAttributes<DescriptionAttribute>().Select(att =>
            {
                stb.AppendFormat("\t/** {0} */", att.Description); stb.AppendLf();
                return true;
            }).ToArray();
            //check plain values only
            stb.AppendFormat("\tpublic {0}_URL_Builder({1}): string {{",
                CamelCase(method.Name),
                ResolveParameters(method.GetParameters(), imports, service)
                ); stb.AppendLf();
            var paramaters = method.GetParameters().Select(param => String.Format("{0}=${{encodeURI(String({0}))}}", param.Name));
            string query = "";
            if (paramaters.Any())
            {
                query = "?" + String.Join("&", paramaters);
            }
            stb.AppendFormat("\t\treturn this.$baseURL + '{0}' + `{1}`;",
                baseroute.Replace("[action]", method.Name),
                query); stb.AppendLf();
            stb.AppendFormat("\t}}"); stb.AppendLf();

        }

        /// <summary>
        /// transpile the empty post action with no arguments
        /// </summary>
        /// <param name="service"></param>
        /// <param name="method"></param>
        /// <param name="stb"></param>
        /// <param name="baseroute"></param>
        /// <param name="imports"></param>
        internal static void TranspileEmptyPostMethod(Type service, MethodInfo method, StringBuilder stb, string baseroute, HashSet<string> imports)
        {
            method.GetCustomAttributes<DescriptionAttribute>().Select(att =>
            {
                stb.AppendFormat("\t/** {0} */", att.Description); stb.AppendLf();
                return true;
            }).ToArray();
            //check plain values or complex value
            var forceTypes = method.GetCustomAttributes<AngularForceTypeAttribute>().ToList();
            if (forceTypes.Any())
            {
                stb.AppendFormat("\tpublic {0}({1}): Observable<{2}> {{", CamelCase(method.Name),
    ResolveParameters(method.GetParameters(), imports, service), forceTypes.First().CustomizedType);
                stb.AppendLf();
                stb.AppendFormat("\t\treturn this.$httpClient.post<{1}>(this.$baseURL + '{0}', null, {{}});",
                    baseroute.Replace("[action]", method.Name),
                    TypeMapping(method.ReturnType, imports, service)
                    ); stb.AppendLf();
                stb.AppendFormat("\t}}"); stb.AppendLf();
            }
            else if (method.ReturnType.FullName == "System.Void")
            {
                stb.AppendFormat("\tpublic {0}({1}): Observable<Response> {{", CamelCase(method.Name),
                    ResolveParameters(method.GetParameters(), imports, service), TypeMapping(method.ReturnType, imports, service));
                stb.AppendLf();
                stb.AppendFormat("\t\treturn this.$httpClient.post<{1}>(this.$baseURL + '{0}', null, {{}});",
                    baseroute.Replace("[action]", method.Name),
                    TypeMapping(method.ReturnType, imports, service)
                    ); stb.AppendLf();
                stb.AppendFormat("\t}}"); stb.AppendLf();
            }
            else
            {
                stb.AppendFormat("\tpublic {0}({1}): Observable<{2}> {{", CamelCase(method.Name),
                    ResolveParameters(method.GetParameters(), imports, service), TypeMapping(method.ReturnType, imports, service));
                stb.AppendLf();
                stb.AppendFormat("\t\treturn this.$httpClient.post<{1}>(this.$baseURL + '{0}', null, {{}});",
                    baseroute.Replace("[action]", method.Name),
                    TypeMapping(method.ReturnType, imports, service)
                    ); stb.AppendLf();
                stb.AppendFormat("\t}}"); stb.AppendLf();
            }


        }

        /// <summary>
        /// transpile the plain post action
        /// </summary>
        /// <param name="service"></param>
        /// <param name="method"></param>
        /// <param name="stb"></param>
        /// <param name="baseroute"></param>
        /// <param name="imports"></param>
        internal static void TranspilePlainPostMethod(Type service, MethodInfo method, StringBuilder stb, string baseroute, HashSet<string> imports)
        {
            method.GetCustomAttributes<DescriptionAttribute>().Select(att =>
            {
                stb.AppendFormat("\t/** {0} */", att.Description); stb.AppendLf();
                return true;
            }).ToArray();
            //check plain values or complex value
            var forceTypes = method.GetCustomAttributes<AngularForceTypeAttribute>().ToList();
            if (forceTypes.Any())
            {
                stb.AppendFormat("\tpublic {0}({1}): Observable<{2}> {{", CamelCase(method.Name),
             ResolveParameters(method.GetParameters(), imports, service), forceTypes.First().CustomizedType);
                stb.AppendLf();
                stb.AppendFormat("\t\tlet $data = new FormData();"); stb.AppendLf();
                method.GetParameters().Select(param =>
                {
                    if (param.CustomAttributes.Where(attr => attr.AttributeType == typeof(JsonPostParameterAttribute)).Any())
                    {
                        stb.AppendFormat("\t\t$data.append('{0}', JSON.stringify({0}));", param.Name); stb.AppendLf();
                    }
                    else
                    {
                        string parameterType = TypeMapping(param.ParameterType, imports, service);
                        switch (parameterType)
                        {
                            case "File":
                            case "Blob":
                            case "string":
                                stb.AppendFormat("\t\t$data.append('{0}', {0});", param.Name); stb.AppendLf();
                                break;
                            default:
                                stb.AppendFormat("\t\t$data.append('{0}', String({0}));", param.Name); stb.AppendLf();
                                break;
                        }
                    }

                    return true;
                }).ToArray();
                stb.AppendFormat("\t\treturn this.$httpClient.post<{1}>(this.$baseURL + '{0}', $data, {{}});",
                    baseroute.Replace("[action]", method.Name),
                    TypeMapping(method.ReturnType, imports, service)
                    ); stb.AppendLf();
                stb.AppendFormat("\t}}"); stb.AppendLf();
            }
            else if (method.ReturnType.FullName == "System.Void")
            {
                stb.AppendFormat("\tpublic {0}({1}): Observable<Response> {{", CamelCase(method.Name),
             ResolveParameters(method.GetParameters(), imports, service), TypeMapping(method.ReturnType, imports, service));
                stb.AppendLf();
                stb.AppendFormat("\t\tlet $data = new FormData();"); stb.AppendLf();
                method.GetParameters().Select(param =>
                {
                    if (param.CustomAttributes.Where(attr => attr.AttributeType == typeof(JsonPostParameterAttribute)).Any())
                    {
                        stb.AppendFormat("\t\t$data.append('{0}', JSON.stringify({0}));", param.Name); stb.AppendLf();
                    }
                    else
                    {
                        string parameterType = TypeMapping(param.ParameterType, imports, service);
                        switch (parameterType)
                        {
                            case "File":
                            case "Blob":
                            case "string":
                                stb.AppendFormat("\t\t$data.append('{0}', {0});", param.Name); stb.AppendLf();
                                break;
                            default:
                                stb.AppendFormat("\t\t$data.append('{0}', String({0}));", param.Name); stb.AppendLf();
                                break;
                        }

                    }

                    return true;
                }).ToArray();
                stb.AppendFormat("\t\treturn this.$httpClient.post<{1}>(this.$baseURL + '{0}', $data, {{}});",
                    baseroute.Replace("[action]", method.Name),
                    TypeMapping(method.ReturnType, imports, service)
                    ); stb.AppendLf();
                stb.AppendFormat("\t}}"); stb.AppendLf();
            }
            else
            {
                stb.AppendFormat("\tpublic {0}({1}): Observable<{2}> {{", CamelCase(method.Name),
             ResolveParameters(method.GetParameters(), imports, service), TypeMapping(method.ReturnType, imports, service));
                stb.AppendLf();
                stb.AppendFormat("\t\tlet $data = new FormData();"); stb.AppendLf();
                method.GetParameters().Select(param =>
                {
                    if (param.CustomAttributes.Where(attr => attr.AttributeType == typeof(JsonPostParameterAttribute)).Any())
                    {
                        stb.AppendFormat("\t\t$data.append('{0}', JSON.stringify({0}));", param.Name); stb.AppendLf();
                    }
                    else
                    {
                        string parameterType = TypeMapping(param.ParameterType, imports, service);
                        switch (parameterType)
                        {
                            case "File":
                            case "Blob":
                            case "string":
                                stb.AppendFormat("\t\t$data.append('{0}', {0});", param.Name); stb.AppendLf();
                                break;
                            default:
                                stb.AppendFormat("\t\t$data.append('{0}', String({0}));", param.Name); stb.AppendLf();
                                break;
                        }
                    }

                    return true;
                }).ToArray();
                stb.AppendFormat("\t\treturn this.$httpClient.post<{1}>(this.$baseURL + '{0}', $data, {{}});",
                    baseroute.Replace("[action]", method.Name),
                    TypeMapping(method.ReturnType, imports, service)
                    ); stb.AppendLf();
                stb.AppendFormat("\t}}"); stb.AppendLf();
            }


        }

        /// <summary>
        /// transpile the complex post action
        /// </summary>
        /// <param name="service"></param>
        /// <param name="method"></param>
        /// <param name="stb"></param>
        /// <param name="baseroute"></param>
        /// <param name="imports"></param>
        internal static void TranspileComplexPostMethod(Type service, MethodInfo method, StringBuilder stb, string baseroute, HashSet<string> imports)
        {
            method.GetCustomAttributes<DescriptionAttribute>().Select(att =>
            {
                stb.AppendFormat("\t/** {0} */", att.Description); stb.AppendLf();
                return true;
            }).ToArray();
            var forceTypes = method.GetCustomAttributes<AngularForceTypeAttribute>().ToList();
            if (forceTypes.Any())
            {
                //there is only one complex value, which is posted via body as json.
                stb.AppendFormat("\tpublic {0}({1}): Observable<{2}> {{", CamelCase(method.Name), ResolveParameters(method.GetParameters(), imports, service), forceTypes.First().CustomizedType); stb.AppendLf();
                stb.AppendFormat("\t\treturn this.$httpClient.post<{2}>(this.$baseURL + '{0}', {1}, {{}});",
                    baseroute.Replace("[action]", method.Name),
                    method.GetParameters().First().Name,
                    TypeMapping(method.ReturnType, imports, service)
                    ); stb.AppendLf();
                stb.AppendFormat("\t}}"); stb.AppendLf();
            }
            else if (method.ReturnType.FullName == "System.Void")
            {
                //there is only one complex value, which is posted via body as json.
                stb.AppendFormat("\tpublic {0}({1}): Observable<Response> {{", CamelCase(method.Name), ResolveParameters(method.GetParameters(), imports, service), TypeMapping(method.ReturnType, imports, service)); stb.AppendLf();
                stb.AppendFormat("\t\treturn this.$httpClient.post<{2}>(this.$baseURL + '{0}', {1}, {{}});",
                    baseroute.Replace("[action]", method.Name),
                    method.GetParameters().First().Name,
                    TypeMapping(method.ReturnType, imports, service)
                    ); stb.AppendLf();
                stb.AppendFormat("\t}}"); stb.AppendLf();
            }
            else
            {
                //there is only one complex value, which is posted via body as json.
                stb.AppendFormat("\tpublic {0}({1}): Observable<{2}> {{", CamelCase(method.Name), ResolveParameters(method.GetParameters(), imports, service), TypeMapping(method.ReturnType, imports, service)); stb.AppendLf();
                stb.AppendFormat("\t\treturn this.$httpClient.post<{2}>(this.$baseURL + '{0}', {1}, {{}});",
                    baseroute.Replace("[action]", method.Name),
                    method.GetParameters().First().Name,
                    TypeMapping(method.ReturnType, imports, service)
                    ); stb.AppendLf();
                stb.AppendFormat("\t}}"); stb.AppendLf();
            }


        }

        /// <summary>
        /// transpile the multiple file post action
        /// </summary>
        /// <param name="service"></param>
        /// <param name="method"></param>
        /// <param name="stb"></param>
        /// <param name="baseroute"></param>
        /// <param name="imports"></param>
        internal static void TranspileMultiFilePostMethod(Type service, MethodInfo method, StringBuilder stb, string baseroute, HashSet<string> imports)
        {
            method.GetCustomAttributes<DescriptionAttribute>().Select(att =>
            {
                stb.AppendFormat("\t/** {0} */", att.Description); stb.AppendLf();
                return true;
            }).ToArray();
            var forceTypes = method.GetCustomAttributes<AngularForceTypeAttribute>().ToList();
            if (forceTypes.Any())
            {
                //there is only one complex value, which is posted via body as json.
                stb.AppendFormat("\tpublic {0}({1}): Observable<{2}> {{", CamelCase(method.Name), $"{method.GetParameters()[0].Name} :File[]", forceTypes.First().CustomizedType); stb.AppendLf();
                stb.AppendFormat("\t\tlet $data = new FormData();"); stb.AppendLf();
                stb.Append($"\t\t{method.GetParameters()[0].Name}.forEach(($item, $index)=>$data.append(`item${{$index}}`, $item, $item.name));"); stb.AppendLf();
                stb.AppendFormat("\t\treturn this.$httpClient.post<{1}>(this.$baseURL + '{0}', $data, {{}});",
                    baseroute.Replace("[action]", method.Name),
                    TypeMapping(method.ReturnType, imports, service)
                    ); stb.AppendLf();
                stb.AppendFormat("\t}}"); stb.AppendLf();
            }
            else if (method.ReturnType.FullName == "System.Void")
            {
                //there is only one complex value, which is posted via body as json.
                stb.AppendFormat("\tpublic {0}({1}): Observable<Response> {{", CamelCase(method.Name), $"{method.GetParameters()[0].Name} :File[]", TypeMapping(method.ReturnType, imports, service)); stb.AppendLf();
                stb.AppendFormat("\t\tlet $data = new FormData();"); stb.AppendLf();
                stb.Append($"\t\t{method.GetParameters()[0].Name}.forEach(($item, $index)=>$data.append(`item${{$index}}`, $item, $item.name));"); stb.AppendLf();
                stb.AppendFormat("\t\treturn this.$httpClient.post<{1}>(this.$baseURL + '{0}', $data, {{}});",
                    baseroute.Replace("[action]", method.Name),
                    TypeMapping(method.ReturnType, imports, service)
                    ); stb.AppendLf();
                stb.AppendFormat("\t}}"); stb.AppendLf();
            }
            else
            {
                //there is only one complex value, which is posted via body as json.
                stb.AppendFormat("\tpublic {0}({1}): Observable<{2}> {{", CamelCase(method.Name), $"{method.GetParameters()[0].Name} :File[]", TypeMapping(method.ReturnType, imports, service)); stb.AppendLf();
                stb.AppendFormat("\t\tlet $data = new FormData();"); stb.AppendLf();
                stb.Append($"\t\t{method.GetParameters()[0].Name}.forEach(($item, $index)=>$data.append(`item${{$index}}`, $item, $item.name));"); stb.AppendLf();
                stb.AppendFormat("\t\treturn this.$httpClient.post<{1}>(this.$baseURL + '{0}', $data, {{}});",
                    baseroute.Replace("[action]", method.Name),
                    TypeMapping(method.ReturnType, imports, service)
                    ); stb.AppendLf();
                stb.AppendFormat("\t}}"); stb.AppendLf();
            }
        }

    }

    internal enum FileEmitResult
    {
        Created,
        KeepOld,
        Changed,
        Removed
    }
}
