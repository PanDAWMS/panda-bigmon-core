"""
Created on 04.06.2018
:author Tatiana Korchuganova
A lib to send ART job status report by email
"""
import re, logging
from django.utils.html import strip_tags
from smtplib import SMTPException
from datetime import datetime
from django.core.mail import send_mail
from django.template import loader
from django.core import mail

_logger = logging.getLogger('bigpandamon-error')


def textify(html):
    # Remove html tags and continuous whitespaces
    text_only = strip_tags(html)
    # Strip single spaces in the beginning of each line
    return text_only.replace('\n ', '\n').strip()


def send_mail_art(template, subject, summary, recipient):
    # recipient = 'mailfordebug@cern.ch'
    isSuccess = True
    nmails = 0
    html_message = loader.render_to_string(
        template,
        {
            'subject': subject,
            'summary': summary,
            'built': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
    )
    try:
        nmails = send_mail(
            subject=subject,
            message=textify(html_message),
            from_email='atlas.pandamon@cern.ch',
            # recipient_list=[recipient],
            recipient_list=['tatiana.korchuganova@cern.ch'],
            fail_silently=False,
        )
    except SMTPException as e:
        msg = 'Exception was caught while sending ART jobs report to ' + recipient
        msg += '\n' + str(e)
        _logger.exception(msg)
        isSuccess = False

    if nmails == 0:
        isSuccess = False
    return isSuccess

def send_mails(template, subject, summary):
    isSuccess = True
    nmails = 0
    connection = mail.get_connection()

    # Manually open the connection
    connection.open()
    emails = []

    # Construct  messages
    for recipient, sum in summary.items():
        html_message = loader.render_to_string(
            template,
            {
                'subject': subject,
                'summary': sum,
                'built': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
        )
        email = mail.EmailMessage(
            subject=subject,
            from_email='atlas.pandamon@cern.ch',
            to=[recipient],
            body=textify(html_message),
        )
        emails.append(email)

    # Send the two emails in a single call -
    nmails = connection.send_messages(emails)
    # The connection was already open so send_messages() doesn't close it.
    # We need to manually close the connection.
    connection.close()

    if nmails == 0:
        isSuccess = False
    return isSuccess