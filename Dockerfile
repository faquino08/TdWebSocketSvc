FROM sofraserv/financedb_base_chromium:test

ENV TDSTREAMING_PWD "autopass"
ENV STREAM_DEBUG False

RUN    useradd -ms /bin/bash tdstreamingdocker
RUN    echo tdstreamingdocker:${TDSTREAMING_PWD} | chpasswd
WORKDIR /var/www/StreamingFlaskDocker
RUN    mkdir /var/www/StreamingFlaskDocker/logs

EXPOSE 8080/tcp

COPY   requirements.txt requirements.txt
RUN    pip3 install -r requirements.txt

COPY   . /var/www/StreamingFlaskDocker
RUN    chown -R tdstreamingdocker:tdstreamingdocker /var/www/

ADD start.sh /var/www/StreamingFlaskDocker/start.sh
RUN chmod +x /var/www/StreamingFlaskDocker/start.sh
CMD ["/var/www/StreamingFlaskDocker/start.sh"]