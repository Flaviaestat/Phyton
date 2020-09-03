#%% o email de envio precisa ter a opção desmarcada para permitir o envio
https://myaccount.google.com/u/1/lesssecureapps

#%% utilizando o pacote yagmail
#pip install yagmail
import yagmail
#%% email sender
sender_email = "flavsphyton@gmail.com"
receiver = "flaviaestat@gmail.com"
#password = input("Type your password and press enter:")
password = 'fl800514'

#%% local arquivo
workdir_path = 'C:/Users/FlaviaCosta/Google Drive/'
filename = 'testeEmail.xlsx'
attachement = workdir_path + filename

#%% sending test
body = "Hello there from Yagmail"

#messsage
yag = yagmail.SMTP(user = sender_email, password = password)
yag.send(
    to=receiver
    , subject= "Yagmail test with attachment"
    , contents=body
    , attachments=attachement
)
