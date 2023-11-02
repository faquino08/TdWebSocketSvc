FROM python:3.9.13-buster

ENV TDSTREAMING_PWD "autopass"
ENV STREAM_DEBUG False

RUN    useradd -ms /bin/bash tdstreamingdocker
RUN    echo tdstreamingdocker:${TDSTREAMING_PWD} | chpasswd
WORKDIR /var/www/StreamingFlaskDocker

RUN    apt-get update
RUN    apt-get install -y wget

#ADD    --chown=tdstreamingdocker:tdstreamingdocker /DataBroker/Sources/TosScannerReader/data/ /home/powerauto/data/

RUN    echo y | apt-get install unixodbc unixodbc-dev
RUN    echo y | apt-get install locales
RUN    echo y | apt-get install ufw
#RUN   echo y | apt-get install selinux-basics selinux-policy-default auditd
RUN    echo y | apt-get install vsftpd
RUN    echo y | apt-get install chromium
RUN    echo y | apt-get install libpam-pwdfile
RUN    wget https://chromedriver.storage.googleapis.com/90.0.4430.24/chromedriver_linux64.zip
RUN    unzip chromedriver_linux64.zip
RUN    mv chromedriver /usr/bin
RUN    sed -i 's/^# *\(en_US.UTF-8\)/\1/' /etc/locale.gen
RUN    locale-gen en_US.UTF-8  
ENV    LANG en_US.UTF-8  
ENV    LANGUAGE en_US:en  
ENV    LC_ALL en_US.UTF-8 
#COPY   vsftpd.conf /etc/
#RUN    sed -i "s|listen_ipv6=YES|listen_ipv6=NO |g" /etc/vsftpd.conf
#RUN    sed -i "s|listen=NO|listen=YES |g" /etc/vsftpd.conf
#RUN    sed -i "s|local_enable=NO|local_enable=YES |g" /etc/vsftpd.conf
#RUN    sed -i "s|xferlog_enable=NO|local_enable=YES |g" /etc/vsftpd.conf
#RUN    sed -i "s|#write_enable=YES|write_enable=YES |g" /etc/vsftpd.conf
#RUN    echo "local_root=/home/powerauto/data" >> /etc/vsftpd.conf
#RUN    echo "pasv_enable=YES" >> /etc/vsftpd.conf
#RUN    echo "pasv_min_port=10090" >> /etc/vsftpd.conf
#RUN    echo "pasv_max_port=10100" >> /etc/vsftpd.conf
#RUN    echo "write_enable=YES" >> /etc/vsftpd.conf
#RUN    echo "xferlog_enable=YES" >> /etc/vsftpd.conf
#RUN    echo "pasv_address=10.6.47.58" >> /etc/vsftpd.conf
#RUN    echo "chroot_local_user=YES" >> /etc/vsftpd.conf
#RUN    echo "allow_writeable_chroot=YES" >> /etc/vsftpd.conf
#EXPOSE 21/tcp
#EXPOSE 22/tcp
#EXPOSE 8080/tcp
#EXPOSE 10091/tcp
#EXPOSE 10092/tcp
#EXPOSE 10093/tcp
#EXPOSE 10094/tcp
#EXPOSE 10095/tcp
#EXPOSE 10096/tcp
#EXPOSE 10097/tcp
#EXPOSE 10098/tcp
#EXPOSE 10099/tcp
#EXPOSE 10100/tcp
EXPOSE 8080/tcp
RUN    ufw allow in 21/tcp
RUN    ufw allow in 22/tcp
RUN    ufw allow in 10090:10100/tcp

COPY   requirements.txt requirements.txt
RUN    pip3 install -r requirements.txt

COPY   . /var/www/StreamingFlaskDocker
RUN    chown -R tdstreamingdocker:tdstreamingdocker /var/www/

ADD start.sh /var/www/StreamingFlaskDocker/start.sh
RUN chmod +x /var/www/StreamingFlaskDocker/start.sh
CMD ["/var/www/StreamingFlaskDocker/start.sh"]