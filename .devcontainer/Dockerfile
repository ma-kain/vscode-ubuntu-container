# See here for image contents: https://github.com/microsoft/vscode-dev-containers/tree/v0.177.0/containers/ubuntu/.devcontainer/base.Dockerfile

# [Choice] Ubuntu version: bionic, focal
ARG VARIANT="focal"
FROM mcr.microsoft.com/vscode/devcontainers/base:0-${VARIANT}

# [Optional] Uncomment this section to install additional OS packages.
RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get -y install --no-install-recommends build-essential python3.8.10 python3-pip python3-dev

RUN pip3 -q install pip --upgrade

RUN mkdir src
WORKDIR src/
COPY . .

RUN pip3 install -r requirements.txt
RUN pip3 install jupyter
RUN jupyter notebook --allow-root --no-browser &
