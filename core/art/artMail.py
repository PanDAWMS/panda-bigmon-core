"""
Created on 04.06.2018
:author Tatiana Korchuganova
A lib to send ART job status report by email
"""

from templated_email import get_templated_mail, send_templated_mail
from datetime import datetime


def send_mail_art(ntag, summary):
    subject = 'ART jobs status report for build made on %s' % (ntag.strftime("%Y-%m-%d"))
    send_templated_mail(
            template_name='artReport',
            from_email='noreply@mail.cern.ch',
            recipient_list=['tkorchug@mail.cern.ch'],
            context={
                'subject': subject,
                'summary': summary,
                'built': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            },
            # Optional:
            # cc=['cc@example.com'],
            # bcc=['bcc@example.com'],
            # headers={'My-Custom-Header':'Custom Value'},
            # template_prefix="my_emails/",
            # template_suffix="email",
    )
    return True
