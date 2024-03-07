FROM ubuntu:latest

RUN apt-get update && apt-get -y install cron curl jq
COPY ./validate_dags.sh /opt/validate_dags.sh
RUN chmod +x /opt/validate_dags.sh
# Copy hello-cron file to the cron.d directory
RUN touch /etc/cron.d/validate_dags
RUN echo "* * * * * bash /opt/validate_dags.sh >> /var/log/cron.log 2>&1" >> /etc/cron.d/validate_dags
RUN chmod 0644 /etc/cron.d/validate_dags
RUN crontab /etc/cron.d/validate_dags
RUN touch /var/log/cron.log
CMD cron && tail -f /var/log/cron.log