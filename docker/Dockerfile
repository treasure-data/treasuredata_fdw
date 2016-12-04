FROM ubuntu

RUN apt-get update
RUN apt-get upgrade
RUN apt-get -y install sudo vim curl wget ca-certificates
RUN wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -
RUN apt-get update
RUN apt-get -y install postgresql-9.5 postgresql-contrib-9.5 postgresql-server-dev-9.5 build-essential
RUN locale-gen "en_US.UTF-8"
RUN curl -sSf https://static.rust-lang.org/rustup.sh | sh
