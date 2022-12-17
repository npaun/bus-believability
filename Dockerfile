FROM ubuntu:22.04
RUN apt update && apt install -y bzip2 curl sqlite3
RUN curl https://downloads.python.org/pypy/pypy3.9-v7.3.10-linux64.tar.bz2 | tar --strip-components 1  -C /usr/ -xjvf -
WORKDIR /usr/app
RUN chown 1001:1001 /usr/app
USER 1001:1001
ENV HOME /usr/app
RUN pypy3 -m ensurepip
COPY --chown=1001:1001 requirements.txt .
RUN pypy3 -m pip install -r requirements.txt
COPY --chown=1001:1001 . .
CMD ["pypy3", "-u", "-m", "bus_believability.observer", "db/vehicles.db"]

