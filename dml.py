#!/usr/bin/python3
# -*- coding: utf-8 -*-
# Bibliotecas necessárias
import os
import sys
import csv
import json
import time
from pymongo import MongoClient
from pprint import pprint
#from pymongo import Connection
#from pymongo import ConnectionFailure

''' DML, Data Manipulation Language, ou Linguagem de Manipulação de Dados. interage
diretamente com os dados dentro das tabelas. São comandos do DML o INSERT, UPDATE e
DELETE. '''

dado = []
cabecalho = []

#variável para arquivo json
data = {}
local = {}
factor = {}
person = {}
vehicle = {}
cyclist = {}
motorist = {}
pedestrians = {}

def cleanData():
    data.clear()

# Realizando conexão com o MongoDB.
host = 'localhost'
port = 27017
cliente = MongoClient(host, port)
print("|===========================>>>>> Script DML no MongoDB <<<<<=============================|")
print("  [ CONEXÃO MONGODB ESTABELECIDA ] >> [ SERVIDOR:",host,"PORTA:",port,"]")
name = input("  [ INFORME O NOME DA DATABASE: ] >> ")
database = cliente.get_database(name)
print("  [ CONEXÃO ESTABELECIDA ] >> [ DATABASE:",name," ]")
colecao = input("  [ INFORME O NOME DA COLLECTION ] >> ")
if colecao:
    col = database.collection[colecao]
    print("  [ CONEXÃO ESTABELECIDA ] >> [ COLLECTION:",colecao,"]")
else:
    print("  [ CONEXÃO NÃO ESTABELECIDA, O NOME DA COLLECTION É NECESSÁRIA ]")
    colecao = input("  [ DIGITE O NOME DA COLLECTION A SER CRIADA OU JÁ EXISTENTE ] >> ")
    print("  [ O NOME DIGITADO É PARA ( 1 - UMA NOVA COLLECTION ) OU UMA ( 2 - EXSITENTE )? ]")
    p = int(input("  [ DIGITE O VALOR ] >> "))
    if colecao == '':
        print("  [ CONEXÃO NÃO ESTABELECIDA, O NOME DA COLLECTION É NECESSÁRIA ]")
        print("  [ TENTE NOVAMENTE MAIS TARDE, GOOD BYE! ]")
        cliente.close()
        exit()
    if p == 1:
        database.create_collection(colecao)
        print("  [ COLLECTION:",colecao," CRIADA ]")
        col = database.collection[colecao]
        print("  [ CONEXÃO ESTABELECIDA ] >> [ COLLECTION:",colecao,"]")
    if p == 2:
        col = database.collection[colecao]
        print("  [ CONEXÃO ESTABELECIDA ] >> [ COLLECTION:",colecao,"]")
'''>>>>>>>>>> Função que coleta cabeçalho dos dados <<<<<<<<<'''
def preProcessa ():
    with open('cabecalho', 'r', encoding='utf8') as file:
        dados = file.read()
        file.close()
    values = dados.split(', ')
    return values
'''>>>>>>>>>> Fim da função que coleta cabeçalho dos dados <<<<<<<<<'''
'''>>>>>>>>>> Função que realiza coleta do dados <<<<<<<<<'''
def insertColeta ():
    print("  [ INFORME OS VALORES DOS CAMPOS ABAIXO ]")
    for j, campo in enumerate(cabecalho): 
        retorno = input("  [",campo,"] >> ")
        dado.insert(j, retorno)
'''>>>>>>>>>> Fim da função que realiza coleta dos dados <<<<<<<<<'''
'''>>>>>>>>>> Função que cria o documentos JSON <<<<<<<<<<'''
#Funções para construção dos documentos JSON
def createData (cab, conteudo):
    data[cab] = conteudo
def createLocation (cab, conteudo):
    local[cab] = conteudo
def createPerson (cab, conteudo):
    person[cab] = conteudo
def createCyclist (cab, conteudo):
    cyclist[cab] = conteudo
def createPedestrians (cab, conteudo):
    pedestrians[cab] = conteudo
def createMotorist (cab, conteudo):
    motorist[cab] = conteudo
def createVehicle (cab, conteudo):
    vehicle[cab] = conteudo
def createFactor (cab, conteudo):
    factor[cab] = conteudo
# Insere o conteúdo do arquivo JSON nos locais devidos
def createDocument ():
    createCyclist(cabecalho[14], dado[14])
    createCyclist(cabecalho[15], dado[15])
    createMotorist(cabecalho[16], dado[16])
    createMotorist(cabecalho[17], dado[17])
    createPedestrians(cabecalho[12], dado[12])
    createPedestrians(cabecalho[13], dado[13])
    createPerson(cabecalho[10], dado[10])
    createPerson(cabecalho[11], dado[11])
    createPerson("CYCLISTS", cyclist)
    createPerson("MOTORISTS", motorist)
    createPerson("PEDESTRIANS", pedestrians)
    createFactor(cabecalho[18], dado[18])
    createFactor(cabecalho[19], dado[19])
    createFactor(cabecalho[20], dado[20])
    createFactor(cabecalho[21], dado[21])
    createFactor(cabecalho[22], dado[22])
    createVehicle(cabecalho[24], dado[24])
    createVehicle(cabecalho[25], dado[25])
    createVehicle(cabecalho[26], dado[26])
    createVehicle(cabecalho[27], dado[27])
    createVehicle(cabecalho[28], dado[28])
    createLocation(cabecalho[2], dado[2])
    createLocation(cabecalho[3], dado[3])
    createLocation(cabecalho[4], dado[4])
    createLocation(cabecalho[5], dado[5])
    createLocation(cabecalho[6], dado[6])
    createLocation(cabecalho[7], dado[7])
    createLocation(cabecalho[8], dado[8])
    createLocation(cabecalho[9], dado[9])
    createData("_id", dado[23])
    createData(cabecalho[0], dado[0])
    createData(cabecalho[1], dado[1])
    createData("LOCAL", local)
    createData("PERSONS", person)
    createData("FACTORS", factor)
    createData("VEHICLES", vehicle)
'''>>>>>>>>>> Fim da função que cria o documentos JSON <<<<<<<<<<'''
'''>>>>>>>>>> Função que insere o documento no MongoDB <<<<<<<<<<'''
def insertMongo ():
    result = col.insert(data)
    return result
'''>>>>>>>>>> Fim da função que insere o documentos no MongoDB <<<<<<<<<<'''
'''>>>>>>>>>> Função de leitura de documento JSON <<<<<<<<<<'''
def leituraJSON ():
    print("  [ SERÁ LIDO O ARQUIVO JSON 'data.json' CONTIDO NA PASTA ]")
    with open('data.json') as file:
        d = json.load(file)
        file.close()
    return d
'''>>>>>>>>>> Fim da função de leitura de documento JSON <<<<<<<<<<'''
'''>>>>>>>>>> Função de leitura de do cabeçalho <<<<<<<<<<'''
def lerCabecalho ():
    with open('cabecalho', 'r', encoding='utf8') as file:
        d= file.readline()
        valores = d.split(', ')
        for j, row in enumerate(valores):
            cabecalho.insert(j, row)
'''>>>>>>>>>> Fim função de leitura de do cabeçalho <<<<<<<<<<'''
'''>>>>>>>>>> Função principal de inserção de documentos <<<<<<<<<<'''
def realizaInsercao ():
    print("  [ DESEJA INSERIR OS DADOS MANUALMENTE OU APARTIR DE ARQUIVO JSON? ]")
    print("  [ COMO ARQUIVO JSON DIGITE 1 ]")
    print("  [ PARA INSERÇÃO MANUAL DIGITE 2 ]")
    res = int(input("  [ DIGITE A OPÇÃO DESEJADA ] >> "))
    if res == 1:
        cleanData()
        data = leituraJSON()
        insertMongo()
        print("  [ AÇÃO EXECUTADA COM SUCESSO, RETORNANDO AO MENU EM 3 SEGUNDOS! ]")
    elif res == 2:
        print("  [ CERTIFIQUE-SE DE QUE O ARQUIVO 'cabecalho' ESTAR NA PASTA ]")
        r = input("  [ QUANTOS DOCUMENTOS DESEJA INSERIR? ] >> ")
        cabecalho = preProcessa()
        print("  [ PRÉ-PROCESSAMENTO EXECUTADO ]")
        for j in range(r):
            insertColeta()
            createDocument()
            insertMongo()
        print("  [ AÇÃO EXECUTADA COM SUCESSO, RETORNANDO AO MENU EM 5 SEGUNDOS! ]")
    time.sleep(3)
'''>>>>>>>>>> Fim da função principal de inserção de documentos <<<<<<<<<<'''
'''>>>>>>>>>> Função que verifica se a coleção detém documentos <<<<<<<<<<'''
def verificaDatabase ():
    num = col.find().count()
    if (num == 0):
        print("  [ COLLECTION: EMPITY ] => [ ALTERE A COLLECTION OU INSIRA DOCUMENTOS ]")
        resposta = input("  [ DESJA REALIZAR INSERÇÕES? (yes ou no) ] => ")
        if (resposta == "no"):
            print("  [ REALIZE AS ALTERAÇÕES E REINICIE O SCRIPT ]")
            cliente.close()
            sys.exit(1)
        elif (resposta == "yes"):
            realizaInsercao()
    else:
        print("  [ COLLECTION CONTÉM:",num,"DOCUMENTOS ]")
    time.sleep(5)
'''>>>>>>>>>> Fim da função de verificação de existência de documentos <<<<<<<<<<'''
'''>>>>>>>>>> Função para realização de update de documentos <<<<<<<<<<'''
def realizaUpdate (): #não ta finalizada
    print("  [ PRECISAMOS DOS DADOS PARA REALIZAR UPDATE, NÃO ESQUEÇA O '_id' ]")
    valores = input("  [ DIGITE OS VALORES DOS CAMPOS, EX.: 0, 4, 23. SEPARE-OS POR ', ' ]")
    numeros = valores.split(', ') # atráves dos numeros eu correlaciono com os campos de cabeçalho e sei qual dado realizar update

'''>>>>>>>>>> Fim da função de update de documentos <<<<<<<<<<'''
'''>>>>>>>>>> Fim da função de colsulta de documentos <<<<<<<<<<'''
def realiazConsulta (): #não ta finalizada
    print("  [ PRECISAMOS DOS DADOS PARA REALIZAR A CONSULTA, VAMOS COMEÇAR ]")
    print("  [ DESEJA FAZER UMA CONSULTA DE QUAL TIPO? ]")
    print("  [ 1 - SIMPLES ]")
    print("  [ 2 - INER JOIN ]")
    print("  [ 3 - OUTER JOIN ]")
    print("  [ ")
    par = int(input("  [ QUANTAS CONSULTAS DESEJA REALIZAR? ] >> "))
    if par > 1:
        for i in range(par):
            valor = json.loads(input("  [ DIGITE A CONSULTA DESEJADA ] >> "))
            retorno = col.find(valor)
            print("  [ DOCUMENTOS ENCOMTRADOS: ]")
            pprint(retorno)
    else:
        retorno = col.find(valor)
        print("  [ DOCUMENTOS ENCOMTRADOS: ]")
        pprint(retorno)        
            
    print("  [ PRECISAMOS DOS DADOS PARA REALIZAR UPDATE, NÃO ESQUEÇA O '_id' ]")
'''>>>>>>>>>> Fim da função de consulta de documentos <<<<<<<<<<'''
'''>>>>>>>>>> Função de remoção de documentos <<<<<<<<<<'''
def realizaExclusao ():
    valor = int(input("  [ DESEJA ( 1 - REALIZAR UM BUSCA) OU ( 2 - INFORMAR UM '_id') ] >> "))
    if valor == 1: # Para realizar consulta e obter os _ids, salvar em um a lista e depois exluir
         pass # Criamodulo exclusivo da exlusão para modularizar.
    elif valor == 2:
        num = input("  [ INFORME O '_id' DO DOCUMENTO A SER EXCLUIDO ] >> ")
        col.remove({"_id": num})
'''>>>>>>>>>> Fim da função de remoção de documentos <<<<<<<<<<'''

'''>>>>>>>>>> MENU PRINCIPAL <<<<<<<<<<'''
def menu ():
    os.system('cls' if os.name == 'nt' else 'clear')
    print("|===========================>>>>> Script DML no MongoDB <<<<<=============================|")
    print("  [ ESCOLHA UMA DAS OPÇÕES ABAIXO ]")
    print("  [ PARA INSERIR DOCUMENTOS DIGITE >> 1 ]")
    print("  [ PARA ATUALIZAR DOCUMENTOS DIGITE >> 2 ]")
    print("  [ PARA REALIZAR EXCLUSÃO DE DOCUMENTOS DIGITE >> 3 ]")
    print("  [ PARA SAIR DO SCRIPT DIGITE >> 4 ]")
    res = int(input("  [ DIGITE A OPÇÃO DESEJADA ] >> "))
    if res == 1:
        realizaInsercao()
    elif res == 2:
        realizaUpdate()
    elif res == 3:
        realizaExclusao()
    elif res == 4:
        return False
    return True
'''>>>>>>>>>> FIM MENU PRINCIPAL <<<<<<<<<<'''

if __name__ == "__main__":
    retorno = True
    verificaDatabase()
    time.sleep(2)
    while retorno:
        data.clear()
        vehicle.clear()
        person.clear()
        cyclist.clear()
        motorist.clear()
        pedestrians.clear()
        local.clear()
        factor.clear()
        retorno = menu()
    print("|=========================>>>>> ATÉ MAIS TENHA UM BOM DIA <<<<<===========================|")
    cliente.close()