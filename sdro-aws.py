import os
import requests
import urllib
import pandas as pd
import boto3
from boto3.session import Session
from datetime import datetime, timedelta

df_cred = pd.read_csv('~/credentials.csv', sep=";")

def lambda_handler(event, context):
    url_pt_1 = r'http://sdro.ons.org.br/SDRO/DIARIO/'
    
    #  BAIXAR TODOS OS ARQUIVOS ##########
    data_inicial = '01-01-2017'  # data inicial dos arquivos no link
    data_inicial = datetime.strptime(data_inicial, '%d-%m-%Y')  # transforma para o formato data
    
    AccessKeyId = df_cred.iloc[0,1]
    SecretKey = df_cred.iloc[0,2]
    bucketname = df_cred.iloc[0,3]
    
    lista_url_cortada = []
    lista_url = []
    
    s3_client = boto3.client('s3', 
                            aws_access_key_id=AccessKeyId, 
                            aws_secret_access_key=SecretKey
                            )
                            

    while data_inicial <= datetime.today():
        # vai começar dia 01/01/2017 e vai somando um dia a mais. Enquanto esse dia for menor e igual a hoje,
        # o loop continua
        ano = data_inicial.year  # pega ano, mes e dia para formar a url posteriormente
        mes = data_inicial.month
        dia = data_inicial.day
    
        url_pt_2 = str(ano) + "_" + str(mes).zfill(2) + "_" + str(dia).zfill(2) + "/Html/DIARIO_" + \
                   str(dia).zfill(2) + "-" + str(mes).zfill(2) + "-" + str(ano) + ".xlsx"
        # o comando zfill vai fazer com que um número obrigatoriamente tenha dois dígitos. exemplo: 1 = 01.
    
        url = url_pt_1 + url_pt_2
        data = str(dia) + "-" + str(mes) + "-" + str(ano)  # essa string foi criada só para ter um acompanhamento visual
        
        lista_url_cortada.append(url[-22:])
        lista_url.append(url)
        data_inicial = data_inicial + timedelta(days=1)
        # aqui ele incrementa a data, adiciona um dia a mais

    # A listagem só alcança 1000 objetos, então deve-se fazer uma paginação
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucketname)
    lista_arquivos_bucket = []
    
    for page in pages:
        for obj in page['Contents']:
            lista_arquivos_bucket.append(obj['Key'])
    
    for file_date in lista_url_cortada:
        if file_date not in lista_arquivos_bucket:
            print("O arquivo {} não foi baixado".format(file_date))
            pos = lista_url_cortada.index(file_date)
            url = lista_url[pos]
            
            if str(requests.get(url)) == '<Response [200]>':
                urllib.request.urlretrieve(url, "/tmp/" + str(url[-22:]))
                s3_client.upload_file("/tmp/" + str(url[-22:]), bucketname, str(url[-22:]))
                # aqui ele baixa o arquivo e o coloca no diretório escrito acima
                print("Arquivo {} Baixado".format(file_date))
            else:
                print("Deu bug no download")
        
    print("Funcionou")
