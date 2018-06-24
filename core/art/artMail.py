"""
Created on 04.06.2018
:author Tatiana Korchuganova
A lib to send ART job status report by email
"""

from smtplib import SMTPException
from datetime import datetime
from django.core.mail import send_mail
from django.template import loader



def send_mail_art(ntag, summary):
    subject = 'ART jobs status report for build made on %s' % (ntag.strftime("%Y-%m-%d"))
    isSuccess = True
    nmails = 0
    html_message = loader.render_to_string(
        'templated_email/artReport.html',
        {
            'subject': subject,
            'summary': summary,
            'built': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
    )
    try:
        nmails = send_mail(
            subject=subject,
            message='',
            from_email='atlas.pandamon@cern.ch',
            recipient_list=['atlas-sw-art-users@cern.ch'],
            fail_silently=False,
            html_message=html_message,
        )
    except SMTPException:
        isSuccess = False

    if nmails == 0:
        isSuccess = False
    return isSuccess
