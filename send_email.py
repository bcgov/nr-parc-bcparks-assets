import os
import smtplib
from email.message import EmailMessage

output_path = r"Q:\dss_workarea\mlabiadh\workspace\20241015_Park_assets_script\data\bc_out_of_boundary_map.html"

# read the HTML bytes
with open(output_path, 'rb') as f:
    html_bytes = f.read()

smtp_server   = os.getenv('SMTP_SERVER')
smtp_user     = 'Moez.Labiadh@gov.bc.ca'
recipients_list = ['Moez.Labiadh@gov.bc.ca']
cc_list = ['labiadhmoez@gmail.com']

mailServer = smtplib.SMTP(smtp_server)
mailServer.ehlo()     
mailServer.starttls()   

msg = EmailMessage()
msg['Subject'] = 'Outside-BC Asset Coordinates Map'
msg['From']    = smtp_user
msg['To']      = ', '.join(recipients_list) 
msg['Cc']      = ', '.join(cc_list)
msg.set_content('Hello, Please find attached the Outside-BC Asset Coordinates map.')

msg.add_attachment(
    html_bytes,
    maintype='text',
    subtype='html',
    filename=os.path.basename(output_path)
)

mailServer.send_message(msg)