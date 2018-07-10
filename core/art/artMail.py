"""
Created on 04.06.2018
:author Tatiana Korchuganova
A lib to send ART job status report by email
"""
import re
from django.utils.html import strip_tags
from smtplib import SMTPException
from datetime import datetime
from django.core.mail import send_mail
from django.template import loader
from django.core import mail

def textify(html):
    # Remove html tags and continuous whitespaces
    text_only = strip_tags(html)
    # Strip single spaces in the beginning of each line
    return text_only.replace('\n ', '\n').strip()


def send_mail_art(template, subject, summary, recipient):
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
            message='',
            from_email='atlas.pandamon@cern.ch',
            recipient_list=[recipient],
            fail_silently=False,
            html_message=html_message,
        )
    except SMTPException:
        isSuccess = False

    if nmails == 0:
        isSuccess = False
    return isSuccess

def send_mails(template, subject, summary):
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
    isSent = connection.send_messages(emails)
    # The connection was already open so send_messages() doesn't close it.
    # We need to manually close the connection.
    connection.close()
    return isSent