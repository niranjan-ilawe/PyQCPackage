import os
import pickle
# Gmail API utils
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
# for encoding/decoding messages in base64
from base64 import urlsafe_b64encode
# for dealing with attachement MIME types
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
from email.mime.audio import MIMEAudio
from email.mime.base import MIMEBase

# Request all access (permission to read/send/receive emails, manage the inbox, and more)
SCOPES = ['https://mail.google.com/']
our_email = 'niranjan.ilawe@10xgenomics.com'

def gmail_authenticate():
    creds = None
    # the file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first time
    if os.path.exists("token.pickle"):
        with open("token.pickle", "rb") as token:
            creds = pickle.load(token)
    # if there are no (valid) credentials availablle, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # save the credentials for the next run
        with open("token.pickle", "wb") as token:
            pickle.dump(creds, token)
    return build('gmail', 'v1', credentials=creds)

def build_message(destination, obj, body, attachments=[]):
    if not attachments: # no attachments given
        message = MIMEText(body)
        message['to'] = destination
        message['from'] = our_email
        message['subject'] = obj
    else:
        message = MIMEMultipart()
        message['to'] = destination
        message['from'] = our_email
        message['subject'] = obj
        message.attach(MIMEText(body))
        for filename in attachments:
            print("None")
    return {'raw': urlsafe_b64encode(message.as_bytes()).decode()}

def send_message(service, destination, obj, body, attachments=[]):
    return service.users().messages().send(
          userId="me",
            body=build_message(destination, obj, body, attachments)
        ).execute()

# send_message(service, "niranjan.ilawe@10xgenomics.com", "This is a subject", 
#            "This is the body of the email")

qc_email_dict = {
    "Admin": "niranjan.ilawe@10xgenomics.com",
    "Supervisor_CA": "fuying.zheng@10xgenomics.com",
    "Supervisor_SG": "xuelingshirlene.ong@10xgenomics.com"
}


def send_error_emails(error_list, filename, qc_by, file_loc):
    try:
        qc_tech_name = qc_by.split(".")[0].capitalize()
        last_name = qc_by.split(".")[1].capitalize()
        qc_by_email = f"{qc_by.lower()}@10xgenomics.com"
    except:
        qc_tech_name = qc_by
        qc_by_email = qc_email_dict["Admin"]

    # creating body
    body = f"""\
    Hello {qc_tech_name}, 
    
    I could not parse the file: {filename}, you uploaded to Box recently.
    
    Following errors were found: {error_list}.
    
    Please follow the recommended practices to ensure proper upload of this data.
    
    If you have any questions, or you are unable to fix the error, please reach out to CPD Data Analytics.
    Email: niranjan.ilawe@10xgenomics.com
    (Do not reply to this message directly)

    Thank you
    Auto File Parser
    """

    # creating a list of people the emails are sent too
    if file_loc == "CA":
        supervisor_email = qc_email_dict["Supervisor_CA"]
    elif file_loc == "SG":
        supervisor_email = qc_email_dict["Supervisor_SG"]
    else:
        supervisor_email = qc_email_dict["Admin"]

    receiver_email = [qc_by_email, supervisor_email, qc_email_dict["Admin"]]
    receiver_email = ",".join(receiver_email)

    # get the Gmail API service
    service = gmail_authenticate()

    send_message(service, receiver_email, "Auto QC Parser Error", 
            body)

    return 0
