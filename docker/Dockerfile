FROM ubuntu

RUN apt-get update
RUN apt-get upgrade
RUN apt-get -y install sudo vim git curl wget locate ca-certificates
RUN wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -
RUN echo 'deb http://apt.postgresql.org/pub/repos/apt/ xenial-pgdg main' 9.6 > /etc/apt/sources.list.d/pgdg.list
RUN apt-get update
RUN apt-get -y install postgresql-9.6 postgresql-contrib-9.6 postgresql-server-dev-9.6 build-essential
RUN apt-get -y install libssl-dev
RUN locale-gen "en_US.UTF-8"
RUN curl -sSf https://static.rust-lang.org/rustup.sh | sh
RUN wget https://bootstrap.pypa.io/ez_setup.py -O - | python3
