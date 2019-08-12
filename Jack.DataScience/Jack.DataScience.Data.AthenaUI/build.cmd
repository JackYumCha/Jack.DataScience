set msbuild.exe=
for /D %%D in (%SYSTEMROOT%\Microsoft.NET\Framework64\v4*) do set msbuild.exe=%%D\MSBuild.exe
if not defined msbuild.exe echo error: can't find MSBuild.exe & goto :eof
if not exist "%msbuild.exe%" echo error: %msbuild.exe%: not found & goto :eof
%msbuild.exe% Jack.DataScience.Data.AthenaUI.csproj /t:Build /p:OutputPath=bin\output;Configuration=PRODUCTION;Platform=x64