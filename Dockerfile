FROM postgres:11.3

RUN apt-get update && apt-get install -y postgresql-server-dev-10 git-core default-jdk unzip

RUN git clone https://github.com/OSBI/foodmart-data.git && \
    cd foodmart-data && git -c advice.detachedHead=false checkout f686c784 && \ 
    unzip data/DataScript.zip -d data/

COPY ./pg-foodmart-init.sh /docker-entrypoint-initdb.d/