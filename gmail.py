from __future__ import print_function

import base64
import logging
import os
from email.message import EmailMessage

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# If modifying these scopes, delete the file token.json.
SCOPES = ["https://www.googleapis.com/auth/gmail.send"]


def gmail_send_message(recipient, sender, subject, body, cc, reply_to):
    """Create and send an email message
    Print the returned  message id
    Returns: Message object, including message id

    Load pre-authorized user credentials from the environment.
    TODO(developer) - See https://developers.google.com/identity
    for guides on implementing OAuth2 for the application.
    """
    creds = None
    path = os.path.dirname(os.path.abspath(__file__))

    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists(path + "/gmail_token.json"):
        creds = Credentials.from_authorized_user_file(
            path + "/gmail_token.json", SCOPES
        )
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            # credentials.json file is from the Google API Console, and must be in the same directory as this file
            flow = InstalledAppFlow.from_client_secrets_file(
                path + "/credentials.json", SCOPES
            )
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open(path + "/gmail_token.json", "w") as token:
            token.write(creds.to_json())

    try:
        service = build("gmail", "v1", credentials=creds)
        message = EmailMessage()

        message.set_content(body)

        message["To"] = recipient
        message["From"] = sender
        if cc:
            message["CC"] = cc
        if reply_to:
            message["Reply-To"] = reply_to
        message["Subject"] = subject

        # encoded message
        encoded_message = base64.urlsafe_b64encode(message.as_bytes()).decode()

        create_message = {"raw": encoded_message}
        # pylint: disable=E1101
        send_message = (
            service.users().messages().send(userId="me", body=create_message).execute()
        )
        logging.info(f'Message Id: {send_message["id"]}')
    except HttpError as error:
        logging.error(f"An error occurred: {error}")
        send_message = None
    return send_message


if __name__ == "__main__":
    body = """Hi!
    
Thank you for requesting an invite to the Autoslug Discord server. We are excited to have you join us! Here's a link: https://autoslug.github.io/discord. Please reply to this email if you have any questions.
    
Thank you,
Ishan"""
    gmail_send_message(
        "Ishan Madan <imadan1@ucsc.edu>",
        "Ishan Madan <imadan1@ucsc.edu>",
        "Autoslug Discord Invite",
        body,
        "",
        ""
    )
