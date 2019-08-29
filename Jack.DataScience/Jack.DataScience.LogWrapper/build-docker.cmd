dotnet publish -c Release -o ./bin/docker
docker build . -f build.dockerfile -t dotwrap:1
docker run --rm --name dotwrap-test dotwrap:1
