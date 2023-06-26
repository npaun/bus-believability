FROM ubuntu:22.04
RUN apt update && apt install -y --no-install-recommends bzip2 curl sqlite3 git ca-certificates gnupg
RUN curl https://downloads.python.org/pypy/pypy3.10-v7.3.12-linux64.tar.bz2 | tar --strip-components 1  -C /usr/ -xjvf -
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg  add - && apt-get update -y && apt-get install google-cloud-cli -y
RUN DEBIAN_FRONTEND=noninteractive apt install -y tzdata
WORKDIR /usr/app
RUN chown 1001:1001 /usr/app
USER 1001:1001
ENV HOME /usr/app
RUN pypy3 -m ensurepip
COPY --chown=1001:1001 requirements.txt .
RUN pypy3 -m pip install -r requirements.txt
COPY --chown=1001:1001 . .
CMD ["pypy3", "-u", "-m", "bus_believability.observer", "--dir", "db/", "--bucket", "gs://bus-believability-data/wkt/"]
