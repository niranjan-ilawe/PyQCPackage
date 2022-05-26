# Import smtplib for the actual sending function
import smtplib, ssl
import keyring

qc_email_dict = {
    "Admin": "niranjan.ilawe@10xgenomics.com",
    "Supervisor": "niranjan.ilawe@10xgenomics.com",
}


def send_error_emails(error_list, filename, qc_by):

    # check validatity of qc_by name and email
    password = keyring.get_password("error_email", "niranjan.ilawe@10xgenomics.com")
    
    try:
        qc_tech_name = qc_by.split(".")[1].capitalize()
        qc_by_email = f"{qc_by}@10xgenomics.com"
    except:
        qc_tech_name = qc_by
        qc_by_email = "niranjan.ilawe@10xgenomics.com"        

    # creating body
    body = f"""
    Subject: QC File ingestion Error

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
    receiver_email = [qc_by_email, qc_email_dict["Supervisor"]]
    port = 465  # For SSL
    smtp_server = "smtp.gmail.com"
    sender_email = "auto.parser.emailer@gmail.com"  # Enter your address

    context = ssl.create_default_context()
    with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, body)

    return 0
