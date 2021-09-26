# syntax = docker/dockerfile:1
# ^ enables BUILDKIT, which allows caching stuff like python packages. it needs to be there, on the first line, to work
FROM openjdk:16-slim-buster
COPY --from=python:3.9-slim-buster / /
# ^ starts with a java base image, then copeis python in from a compatible image

# needed to build dependencies
RUN --mount=type=cache,target=/var/cache/apt apt-get update \
&& echo 'updated' \
&& apt-get install gcc -y \
&& echo 'installed' \
&& apt-get clean

# docker caches the build result of each line in a Dockerfile
# that cache gets invalidated whenever a file that a line depends on changes
# for example, if we were to copy the whole repo in at this point, any change to any
# file in the repository would invalidate the cache for all subsequent commands
# therefore, we copy the bare minimum we need to installing python dependencies first
COPY ./discussion/requirements.txt /requirements.txt
RUN --mount=type=cache,target=/root/.cache pip install -U pip setuptools \
	&& pip install -r /requirements.txt


# similar to above, we copy only the java files so that only a change to java
# files will cause a rebuild from this point onwards
COPY ./src/hw1 /java-src
# create the output path matching intellij's layout so the python notebook knows
# where to find it
RUN mkdir -p /app/out/production/ParallelSort/hw1/
# compile the java source files into the output path
RUN javac -d /app/out/production/ParallelSort/hw1/ /java-src/*.java


# copy the repository into the docker image
COPY . /app

WORKDIR /app
ENTRYPOINT ["/bin/bash", "-c"]
CMD ["jupyter lab --notebook-dir=/app/discussion --ip=0.0.0.0 --allow-root --no-browser"]
