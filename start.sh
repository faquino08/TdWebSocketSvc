#!/bin/sh
#gunicorn --bind 0.0.0.0:8080 --log-level=debug wsgi:app --timeout 600
hypercorn --bind '0.0.0.0:8080' api:app