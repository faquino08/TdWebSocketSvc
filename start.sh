#!/bin/sh
service vsftpd start
gunicorn --bind 0.0.0.0:8080 wsgi:app