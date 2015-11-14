import smtplib
import argparse
import subprocess
import sys
import re
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import time
import gzip
import shutil
import io

FROM = "noreply@keystone.berkeley.edu"
SERVER = "localhost"
# Prepare actual message

def send_email(subject, recepient, log, log_name):
    message = """\
        log file attached
        """
# Send the mail
    msg = MIMEMultipart()
    msg['Subject'] = subject
    msg['From'] = FROM
    msg['To'] = recepient
    f = gzip.open('/tmp/{0}.gz'.format(log_name), 'wb')
    f.write(log)
    f.close()

    f = open('/tmp/{0}.gz'.format(log_name), 'rb')
    attachment = MIMEApplication(f.read(), 'x-gzip')
    attachment['Content-Disposition'] = 'attachment; filename={0}.gz'.format(log_name)
    msg.attach(attachment)
    f.close()

    server = smtplib.SMTP(SERVER)
    print(log)
    print(subject)
    print(server.sendmail(FROM, recepient, msg.as_string()))
    server.quit()



def main():
    if (len(sys.argv) > 1):
        pipeline = sys.argv[1]
    else:
        pipeline = "default"
    log = sys.stdin.read()
    s_res = re.search(r'TEST.*%', log)
    if (s_res == None):
        subject = "{0} keystone job Failed".format(pipeline)
    else:
        subject = "{0} keystone job Succeeded! ".format(pipeline) + s_res.group(0)
    log_dir = "/mnt/logs/"
    log_name = "{0}-{1}.log".format(pipeline, int(time.time()))
    log_file = open(log_name, "w+")
    log_file.write(log)
    if (len(sys.argv) > 2):
        recepient = sys.argv[1]
    else:
        recepient = "vaishaal@gmail.com"

    send_email(subject, recepient, log,  log_name)



if __name__ == "__main__":
    main()
