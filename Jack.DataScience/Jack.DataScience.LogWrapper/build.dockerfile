FROM microsoft/dotnet:2.2-aspnetcore-runtime AS base
WORKDIR /wrap
FROM base AS final
ENV ASPNETCORE_ENVIRONMENT=prod
ENV DOTNETLOG=wrap
WORKDIR /wrap
COPY ./bin/docker /wrap
COPY ./bin/docker/package /app
ENTRYPOINT ["dotnet", "Jack.DataScience.LogWrapper.dll"]
# -t dotwrap