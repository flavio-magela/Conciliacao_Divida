import google.datalab.storage as storage
import pandas as pd
import simplejson as json
import numpy as np
import csv
import re
from io import BytesIO

#--------------------------------------------------------------------------Buscando o arquivo no Cloud PlatForm----------------------------------------------------------------------------------------
# jupyter notebook --NotebookApp.iopub_data_rate_limit=10000000000
mybucket = storage.Bucket('fca-finance-origin') #fca-finance
data = mybucket.object('Conciliacao/IF_BR_02_029_GAAP_20190605074552.txt')

##cadastro = storage.Object.read_lines(data,max_lines=100)
cadastro = storage.Object.read_stream(data,start_offset=0, byte_count=None)

#print (cadastro)

#--------------------------------------------------------------------------Formatando o Arquivo com replace (;)------------------------------------------------------------------------------
tamanho = len(cadastro)
#print '\n'+ str(tamanho) + '\n'
i = 1

val = str(cadastro).replace('ADJCB029__G16420190605190646000177007906007904          ', 'ADJCB029__G16420190605190646000177007906007904;') # Colocando (;) para tratrar o arquivo para transformar em uma única linha 
val2 = val.replace('BRL', 'BRL;') # Colocando (;) para tratrar o arquivo para transformar em uma única linha 
val3 = val2.replace('\r\n', '')

#print val3;

#-------------------------------------------------------------------------Mostrando os dados em uma Lista---------------------------------------------------------------------------------------

valores = val3.split(';') # separação por linhas
# print '\n Mostra os dados do Arquivo após uso do Split(;) \n'
#print valores
legthValores = len(valores)  # Tamanho de linha que tem o arquivo
# valores.find('CTEC190601W6RABR')

#print legthValores
# print '\n Linha de For \n' 
for linha in valores:
    legthlinha = len(linha)
    #print str(linha  ) + ' Nº de linha: ' + str(legthlinha) + '\n'

#-------------------------------------------------------------------Cabeçalho e rodapé dos arquivos que não serão inseridos na Tabela-------------------------------------------------    
# print '\n Mostrar o cabeçalho do arquivo  \n '    
# print valores[0]
# val = len(valores[0])
# print '\n Mostrar o rodapé - última linha ' 
# print valores[legthValores-1]
# print legthValores -2
    
# ------------------------------------------------------------------Mostrando apenas os dados que serão inseridos na Tabela--------------------------------------------------------------

# print '\n Mostrar os campos que serão processados para a Tabela '  
# while i < legthValores - 2:    
#     #print valores[i]
#     i=i+1

lenVal = len(valores) - 2 # Tamanho de linhas  dos dados que serão inseridos na Tabela
# print 'caracteres da 1ª linha: '  + str(lenVal) 

#--------------------------------------------------------------------Inseridos os dados na lista para gerar o arqivos .TSV---------------------------------------------------------------------
destbucket = storage.Bucket('fca-finance') #fca-finance
blob_file = destbucket.object('OTC_Bonus_Incentivos/dataConcFull3/Conciliação da Dívida Full.json')

i = 2
arq = []
arqJson = []
dictarq = {}
while i <= lenVal -1:     
    palavra = valores[i] # Mostra o texto atual da linha
    #print '\n Proxima Palavra eh: \n'
    #print palavra
    m = 1
    texto = ""
    cod_Rec = ""
    mes_Ano = ""
    provem = ""
    tipo_Doc = ""
    marca = ""
    marca_Dest = ""
    merc_Fat = ""
    canal = ""
    opcional = ""
    atv = ""
    mvs = ""
    vazio = ""
    cld = ""
    conta_D = ""
    cLucro_D = ""
    clc = ""
    conta_C = ""
    cLucro_C = ""
    data = ""
    sequencial =""
    data_1 = ""
    vazio_1 = ""
    valor = ""
    nome = ""
    moeda = ""
    mvs_bk = ""

    for linha in palavra:    # Varreque cada caracter na palavra
        if m <= 4:
            cod_Rec = cod_Rec + linha    

        if m >= 5 and m <= 8:
            mes_Ano = mes_Ano + linha
    
        if m >= 9 and m <= 10:
            provem = provem + linha
    
        if m >= 11 and m <= 12:
            tipo_Doc = tipo_Doc + linha
   
        if m >= 13 and m <= 14:
            marca = marca + linha
    
        if m >= 15 and m <= 16:
            marca_Dest = marca_Dest + linha
   
        if m >= 18 and m <= 21:
            merc_Fat = merc_Fat + linha
    
        if m >= 22 and m <= 24:
            canal = canal + linha
   
        if m >= 25 and m <= 28:
            opcional = opcional + linha
    
        if m >= 29 and m <= 29:
            atv = atv + linha
    
        if m >= 30 and m <= 36:
            mvs = mvs + linha
            mvs_bk = mvs_bk + linha
    
        if m >= 37 and m <= 43:
            vazio = vazio + linha
   
        if m >= 44 and m <= 45:
            cld = cld + linha
   
        if m >= 46 and m <= 55:
            conta_D = conta_D + linha
    
        if m >= 56 and m <= 58:
            cLucro_D = cLucro_D + linha
   
        if m >= 59 and m <= 60:
            clc = clc + linha
   
        if m >= 61 and m <= 70:
            conta_C = conta_C + linha
    
        if m >= 71 and m <= 73:
            cLucro_C = cLucro_C + linha
   
        if m >= 74 and m <= 79:
            data = data + linha
  
        if m >= 80 and m <= 86:
            sequencial = sequencial + linha
    
        if m >= 87 and m <= 92:
            data_1 = data_1 + linha
   
        if m >= 93 and m <= 99:
            vazio_1 = vazio_1 + linha
    
        if m >= 100 and m <= 112:
            valor = valor + linha
    
        if m >= 113 and m <= 142:
            nome = nome + linha
   
        if m >= 143 and m <= 145:
            moeda = moeda + linha        
         
        texto = cod_Rec, mes_Ano, provem, tipo_Doc, marca, marca_Dest, merc_Fat, canal, opcional, atv, mvs, vazio, cld, conta_D, cLucro_D, clc, conta_C, cLucro_C, data, sequencial, data_1, vazio_1, valor, nome, moeda, mvs_bk
        
#--------------------------------------------------------------------------------------------Gera uma Tupla dos Dados para o Dicionario---------------------------------------------------------------------------------       
        textoJson = (('cod_Rec', cod_Rec), ('mes_Ano', mes_Ano), ('provem', provem), ('tipo_Doc', tipo_Doc), ('marca', marca), ('marca_Dest', marca_Dest),('merc_Fat', merc_Fat), ('canal', canal), ('opcional', opcional),
                         ('atv', atv),('mvs', mvs), ('vazio', vazio), ('cld', cld), ('conta_D', conta_D), ('cLucro_D', cLucro_D), ('clc', clc), ('conta_C', conta_C), ('cLucro_C', cLucro_C), ('data', data), ('sequencial', sequencial),
                          ('data_1', data_1), ('vazio_1', vazio_1),('valor', valor), ('nome', nome), ('moeda', moeda), ('mvs_bk', mvs_bk))
        
#-----------------------------------------------------------------------Criando Dicionario com os arquivos--------------------------------------------------------------------------------------------------        
#         Conciliacao[i] = {'cod_Rec': cod_Rec, 'mes_Ano': mes_Ano, 'provem': provem, 'tipo_Doc': tipo_Doc, 'marca': marca, 'marca_Dest': marca_Dest,'merc_Fat': merc_Fat, 'canal': canal, 'opcional': opcional,
#                          'atv': atv,'mvs': mvs, 'vazio': vazio, 'cld': cld, 'conta_D': conta_D, 'cLucro_D': cLucro_D, 'clc': clc, 'conta_C': conta_C, 'cLucro_C': cLucro_C, 'data': data, 'sequencial': sequencial,
#                           'data_1': data_1, 'vazio_1': vazio_1,'valor': valor, 'nome': nome, 'moeda': moeda, 'mvs_bk': mvs_bk}

        
        m=m+1  # caminha em cada caracter da linha atual
        
#----------------------------------------------------------------- Imprimindo  um Array de  lista ------------------------------------------------------------------------------------------------------- 
#     print '\n Mostrar o texto: '
#     print texto
    arq.append(texto) # Montar um Array de Lista 
#     print '\n Mostrar seus respecitivos VALORES na Lista: \n'       
    arquivo = np.array(arq)  # Joga a Lista em um Array usando o  numpy
#     print arquivo # Imprimindo o arquivo na Lista    
    
#------------------------------------------------------------------Imprimindo o Dicionário para Json---------------------------------------------------------------------------------------------------
#     print '\n Imprimir a variavel TextoJson'
#     print textoJson    # 
    dictArq = dict(textoJson) # Possar a Tupla para Dicionário
    arqJson.append(dictArq)   # Adiciona cada Dicionario em uma lista
   # linhaJson = json.dumps(dictArq, ensure_ascii=False).encode('utf-8') #converte em json

#     print ('\n Imprimir o DICIONARIO')
#     print (arqJson[:1000])
    
    #     arquivoJson = np.array(arqJson)  # Joga a Lista de Dicionário em um Array usando o  numpy
    i=i+1     

#----------------------------------------------------------------------Jogar os dados que estão na lista para o  arquivo .TSV----------------------------------------------------------------------------------------

# c = csv.writer(open("Conciliação da Dívida Teste.tsv", "wb")) # Criação do arquivo - Caso não exista é criado automaticamente
# c.writerow(arquivo)  # Escreve no arquivo

#------------------------------------------------------------------------- Jogar o Dicionário para o JASON--------------------------------------------------------------------------------------
# arqJson = json.loads(Concilia)
# print arqJson
# print json.dumps(Concilia, separators=(',', ':'), sort_keys=True)
# j = csv.writer(open("Conciliação da Dívida Full.json", "wb")) # Criação do arquivo - Caso não exista é criado automaticamente
# j.writerow(arquivoJson)
# # print (json.dumps(Concilia))
# gs://fca-finance/OTC_Bonus_Incentivos/data



# write_stream("teste", 'text/plain:
#blob_file.write_stream(arquivoJson, 'text/plain')
arquivoJson = json.dumps(arqJson, ensure_ascii=False).encode('utf-8') #converte em json
blob_file.write_stream(arquivoJson, 'text/plain')
# print ('\n Imprimir o DICIONARIO')
# print (arquivoJson[:1000])