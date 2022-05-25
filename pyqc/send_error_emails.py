# Import smtplib for the actual sending function
import smtplib
from email.mime.text import MIMEText

qc_email_dict = {
    "Admin": "niranjan.ilawe@10xgenomics.com",
    "Supervisor": "niranjan.ilawe@10xgenomics.com",
}


def send_error_emails(error_list, filename, qc_by):

    # check validatity of qc_by name and email
    try:
        qc_tech_name = qc_by.split(".")[1].capitalize()
        qc_by_email = f"{qc_by}@10xgenomics.com"
    except:
        qc_tech_name = qc_by
        qc_by_email = "niranjan.ilawe@10xgenomics.com"        

    # creating body
    body = f"""
    Hello {qc_tech_name}, 
    I could not parse the QC123 file: {filename}, you uploaded to Box recently.
    Following errors were found: {error_list}.
    Please follow the recommended practices to ensure proper upload of this data.
    
    If you have any questions, or you are unable to fix the error, please reach out to CPD Data Analytics.
    (Do not reply to this message directly)

    Thank you
    Auto File Parser
    """

    # creating a list of people the emails are sent too
    receivers = [qc_by_email, qc_email_dict["Supervisor"]]

    # converting string to a MIMEtext object. This was necessary since this was the only way
    # the email was able to distinguish between the subject, to, and from
    msg = MIMEText(body)
    msg["Subject"] = "QC123 File Auto-ingestion Error"
    msg["From"] = "auto-file-parser@10xgenomics.com"
    msg["To"] = f"{qc_by_email}, {qc_email_dict['Supervisor']}"

    s = smtplib.SMTP("smtp.google.com")

    # the actual email is sent here
    s.sendmail("auto-file-parser@10xgenomics.com", receivers, msg.as_string())
    s.quit()

    return 0
