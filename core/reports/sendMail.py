"""
Created on 04.06.2018
:author Tatiana Korchuganova
A lib to send report by email
"""
import logging
import re
from django.utils.html import strip_tags
from smtplib import SMTPException
from datetime import datetime
from django.core.mail import send_mail
from django.template import loader
from django.conf import settings

_logger = logging.getLogger('bigpandamon')


def textify(html):
    # remove style
    html = re.sub(r'<style.*?>.*?</style>', '', html, flags=re.DOTALL)
    # remove html tags and continuous whitespaces
    text_only = strip_tags(html)
    # strip single spaces in the beginning of each line
    text_only = text_only.replace('\n ', '\n').replace('\n\n', '\n').replace(';=', '=').strip()
    return text_only


def send_mail_bp(template, subject, summary, recipient, send_html=False):
    """
    Send an email using a template.

    Args:
        template (str): The name of the template to use for the email body.
        subject (str): The subject of the email.
        summary (str): A summary to include in the email.
        recipient (str or list): The recipient(s) of the email. Can be a single email address or a list of addresses.
        send_html (bool): If True, send the email as HTML; otherwise, send as plain text.
    :return:
    """
    isSuccess = True
    nmails = 0
    if isinstance(recipient, str):
        recipients = [recipient]
    elif isinstance(recipient, list) or isinstance(recipient, tuple):
        recipients = recipient
    else:
        recipients = []

    # if debug send to the first dev admin
    if settings.DEBUG:
        if len(settings.ADMINS) > 0 and settings.ADMINS[0][0] == 'dev':
            recipients = [settings.ADMINS[0][1]]
        else:
            recipients = []

    html_message = loader.render_to_string(
        template,
        {
            'subject': subject,
            'summary': summary,
            'built': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
    )
    try:
        if send_html:
            nmails = send_mail(
                subject=subject,
                html_message=html_message,
                message=textify(html_message),
                from_email='atlas.pandamon@cern.ch',
                recipient_list=recipients,
                fail_silently=False,
            )
        else:
            nmails = send_mail(
                subject=subject,
                message=textify(html_message),
                from_email='atlas.pandamon@cern.ch',
                recipient_list=recipients,
                fail_silently=False,
            )

    except SMTPException as e:
        msg = 'Internal Server Error! Exception was caught while sending report {} to {}'.format(subject, recipient)
        msg += '\n' + str(e)
        _logger.exception(msg)
        isSuccess = False

    if nmails == 0:
        isSuccess = False
    return isSuccess