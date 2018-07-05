#!/usr/bin/python3
# -*- coding: utf-8 -*-

import csv
import time
import json
from pprint import pprint 
from pymongo import MongoClient
import pymongo
# Conexão MongoDB
host = 'localhost'
port = 27017

cliente = MongoClient(host, port)
print("Conexao mongo feita")
database = cliente.db
print("database selecionado")
collisions = database.collisions
print("collection selecionada")

# Variaveis locais para os documentos JSON
data = {}
local = {}
factor = {}
person = {}
vehicle = {}
cyclist = {}
motorist = {}
pedestrians = {}

#Funções para construção dos documentos JSON
def createData (cabecalho, dado):
    data[cabecalho] = dado

def createLocation (cabecalho, dado):
    local[cabecalho] = dado

def createPerson (cabecalho, dado):
    person[cabecalho] = dado

def createCyclist (cabecalho, dado):
    cyclist[cabecalho] = dado

def createPedestrians (cabecalho, dado):
    pedestrians[cabecalho] = dado

def createMotorist (cabecalho, dado):
    motorist[cabecalho] = dado

def createVehicle (cabecalho, dado):
    vehicle[cabecalho] = dado

def createFactor (cabecalho, dado):
    factor[cabecalho] = dado

# Insere o conteúdo do arquivo nos locais devidos
def createDocument (cabecalho, dado):
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
# Grava os dados no documento JSON. Opcional, atualmente desnecessário
def writeDocument ():
    with open('data.json', 'w') as file:
        json.dump(data, file)

def readCSV (cabecalho, conteudo):
    with open('base.csv', 'r', encoding='utf8') as file:
        dados = csv.reader(file, delimiter=',')
        # Leituras das linhas e colunas do arquivo CSV.
        flag = False
        print(flag)
        for linhas in dados:
            for j, row in enumerate(linhas):
                if flag:
                    conteudo.insert(j, row)
                    if j == 28 and flag:
                        print("Criando Documen")
                        createDocument(cabecalho, conteudo)
                        print("Documen criado, escrevendo")
                        writeDocument()
                        print("inserindo no banco")
                        insertMongo()
                        print("Limpando o conteudo de 'conteudo'")
                        conteudo.clear()
                else:
                    cabecalho.insert(j, row)
                    if row == "VEHICLE TYPE CODE 5":
                        flag = True
                # Após ler todos os dados grava no arquivo e insere no banco.
                
        file.close()

def ler (cabecalho):
    with open('base.csv', 'r', encoding='utf8') as file:
        dados = csv.reader(file, delimiter=',')
        # Leituras das linhas e colunas do arquivo CSV.
        flag = False
        for linhas in dados:
            for j, row in enumerate(linhas):
                if flag:
                    break
                else:
                    cabecalho.insert(j, row)
                    if row == "VEHICLE TYPE CODE 5":
                        flag = True
            if flag:
                break
    print(cabecalho)

#jdgklsjfkjsdk
def lerCabecalho ():
    with open('cabecalho', 'r', encoding='utf8') as file:
        d= file.readline()
        valores = d.split(', ')
        for j, row in enumerate(valores):
            cabecalho.insert(j, row)

'''>>>>>>>>>> Função que insere o documento no MongoDB <<<<<<<<<<'''
def insertMongo (tipo):
    if tipo == 'U':
        result = collisions.insert_one(data)
    elif tipo == 'M':
        sult = collisions.insert_many(data)
'''>>>>>>>>>> Fim da função que insere o documentos no MongoDB <<<<<<<<<<'''
'''>>>>>>>>>> Função de leitura de documento JSON <<<<<<<<<<'''
def leituraJSON ():
    print("  [ SERÁ LIDO O ARQUIVO JSON 'data.json' CONTIDO NA PASTA ]")
    with open('data.json') as file:
        d = json.load(file)
        file.close()
    return d
    pprint(data)
'''>>>>>>>>>> Fim da função de leitura de documento JSON <<<<<<<<<<'''
'''>>>>>>>>>> Função principal de inserção de documentos <<<<<<<<<<'''
def realizaInsercao ():
    print("  [ DESEJA INSERIR OS DADOS MANUALMENTE OU APARTIR DE ARQUIVO JSON? ]")
    print("  [ COMO ARQUIVO JSON DIGITE 1 ]")
    print("  [ PARA INSERÇÃO MANUAL DIGITE 2 ]")
    res = input("  [ DIGITE A OPÇÃO DESEJADA ] ==>> ")
    if res == '1':
        leituraJSON()
        print("  [ A INSERÇÃO É PARA UM ÚNICO ARQUIVO OU MULTIPLOS? ]")
        res = input("  [ DIGITE (U - PARA ÚNICO ARQUIVO) E (M - PARA MULTIPLOS) ] =>> ")
        insertMongo(res)
        print("  [ AÇÃO EXECUTADA COM SUCESSO, RETORNANDO AO MENU EM 5 SEGUNDOS! ]")
    elif res == '2':
        print("  [ CERTIFIQUE-SE DE QUE O ARQUIVO 'cabecalho' ESTAR NA PASTA ]")
        r = input("  [ QUANTAS DOCUMENTOS DESEJA INSERIR? ] =>> ")
        cabecalho = preProcessa()
        print("  [ PRÉ-PROCESSAMENTO EXECUTADO ]")
        for j in range(r):
            insertColeta()
            createDocument()
            insertMongo('U')
        print("  [ AÇÃO EXECUTADA COM SUCESSO, RETORNANDO AO MENU EM 3 SEGUNDOS! ]")
    time.sleep(3)
'''>>>>>>>>>> Fim da função principal de inserção de documentos <<<<<<<<<<'''

'''>>>>>>>>>> Fim da função de colsulta de documentos <<<<<<<<<<'''
def realiazConsulta (): #não ta finalizada
    ids = []
    d1 = {"PERSONS.MOTORISTS.NUMBER OF MOTORIST INJURED": "1"}
    d2 = {"DATE": "06/09/2018"}
    d3 = {"TIME": "9:59"}
    print("  [ PRECISAMOS DOS DADOS PARA REALIZAR A CONSULTA, VAMOS COMEÇAR! ]")
    print("  [ A PESQUISA DEVE SER PASSADA DA SEGUINTE FORMA: ]")
    print("  [ EX1.: ",d1,"]")
    print("  [ EX2.: ",d2,",",d3,"]")
    consulta = input("  [ FORNEÇA SUA CONSULTA ] =>> ")
    ids = collisions.find(consulta)
    print(ids)

    print("  [ PRECISAMOS DOS DADOS PARA REALIZAR UPDATE, NÃO ESQUEÇA O '_id' ]")
'''>>>>>>>>>> Fim da função de consulta de documentos <<<<<<<<<<'''

'''>>>>>>>>>> Função de remoção de documentos <<<<<<<<<<'''
def realizaExclusao ():
    valor = int(input("  [ DESEJA ( 1 - ATRAVÉS DE UMA BUSCA ) OU ( 2 - INFORMANDO UM '_id' ) ] =>> "))
    if valor == 1: # Para realizar consulta e obter os _ids, salvar em um a lista e depois exluir
        print("  [ A PESQUISA DEVE SER FEITA DA FORMA ABAIXO ]")
        print("  [ ")
    elif valor == 2:
        num = input("  [ INFORME O '_id' DO DOCUMENTO A SER EXCLUIDO ] =>> ")
        collisions.remove({"_id": num})
'''>>>>>>>>>> Fim da função de remoção de documentos <<<<<<<<<<'''

def realizaInsercao ():
    print("  [ DESEJA INSERIR OS DADOS MANUALMENTE OU APARTIR DE ARQUIVO JSON? ]")
    print("  [ COMO ARQUIVO JSON DIGITE 1 ]")
    print("  [ PARA INSERÇÃO MANUAL DIGITE 2 ]")
    res = input("  [ DIGITE A OPÇÃO DESEJADA ] ==>> ")
    if res == '3':
        return
    if res == '1':
        data.clear()
        leituraJSON()
        print("  [ A INSERÇÃO É PARA UM ÚNICO ARQUIVO OU MULTIPLOS? ]")
        res = input("  [ DIGITE (U - PARA ÚNICO ARQUIVO) E (M - PARA MULTIPLOS) ] =>> ")
        insertMongo(res)
        print("  [ AÇÃO EXECUTADA COM SUCESSO, RETORNANDO AO MENU EM 5 SEGUNDOS! ]")
    elif res == '2':
        print("  [ CERTIFIQUE-SE DE QUE O ARQUIVO 'cabecalho' ESTAR NA PASTA ]")
        r = input("  [ QUANTAS DOCUMENTOS DESEJA INSERIR? ] =>> ")
        cabecalho = preProcessa()
        print("  [ PRÉ-PROCESSAMENTO EXECUTADO ]")
        for j in range(r):
            insertColeta()
            createDocument()
            insertMongo('U')
        print("  [ AÇÃO EXECUTADA COM SUCESSO, RETORNANDO AO MENU EM 5 SEGUNDOS! ]")
    time.sleep(3)

if __name__ == "__main__":
    #parameters = input("Insira a ação DDL: ")
    cabecalho = []
    conteudo = []
    '''
    #conectMongo()
    print("Leitura do csv...")
    #readCSV(cabecalho, conteudo)
    print("Lendo find...")
    doc = collisions.find_one({"_id": "85174"})
    print(doc)
    #pagina 34 do livro 
    doc = collisions.find({"VEHICLES.VEHICLE TYPE CODE 1": "PASSENGER VEHICLE"}).count()
    print(doc)
    cliente.close()
    #with open('data.json', 'w') as file:
    #    json.dump(data, file)

    #cliente = MongoClient(host, port)
    #database = cliente.nypd_motor
    #collisions = database.collisions
    '''
    #print("  [ DIGITE OS VALORES OU USE UM DOCUMENTO JÁ FORMATODO 'JSON' ])
    #resposta = input("  [ DESEJA DIGITAS OS VALORES? (sim ou não) ] =>> ")
    #if (resposta == "sim"):
    #arq = open('cabecalho.txt', 'r', encoding='utf-8')
    #cabecalho = arq.read()
        #cabecalho = file.read()
    #print("  [",cabecalho,"]")
    #ler(cabecalho)
    #data.clear()
    #with open('data.json', 'r', encoding='utf-8') as f:
    #    data = json.load(f)
    #    f.close()
    #pprint(data)
    #realizaInsercao()
    #realiazConsulta()
    d1 = {"PERSONS.MOTORISTS.NUMBER OF MOTORIST INJURED": "1"}
    d2 = {"DATE": "06/09/2018"}
    d3 = {"TIME": "9:59"}
    pprint(d1)
    pprint(d2)
    pprint(d3)
    print(d2,",",d3)
    num = collisions.count()
    #realizaExclusao()
    print(num)
    #cabecalho.clear()
    #lerCabecalho()
    #print(cabecalho)
    #realizaInsercao()
    data = leituraJSON()
    id = collisions.insert(data)
    print(id)
    #colection = collisions.find()
    #for doc in colection:
    #    pprint(doc)
    cliente.close()