FROM ubuntu:jammy

ENV TZ=Etc/UTC
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
RUN apt-get update
RUN apt-get upgrade
RUN apt-get -y install sudo vim git curl wget locate ca-certificates gnupg2
RUN wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -
RUN echo 'deb http://apt.postgresql.org/pub/repos/apt/ jammy-pgdg main' 11 > /etc/apt/sources.list.d/pgdg.list
RUN apt-get update
RUN apt-get -y install postgresql-11 postgresql-contrib-11 postgresql-server-dev-11 build-essential
RUN apt-get -y install libssl-dev pkg-config
RUN apt-get -y install rustc cargo
RUN apt-get -y install python3-pip python3-setuptools python3-wheel
RUN locale-gen "en_US.UTF-8"
