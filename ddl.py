#!/usr/bin/python3
# -*- coding: utf-8 -*-
''' A DDL, Data Definition Language ou LInguagem de Definição de Dados,
apesar do nome não interage com os dados e sim com os objetos do banco.
São comandos desse tipo o CREATE, o ALTER e o DROP '''

import os
import sys
import csv
import time
import json
from pymongo import MongoClient

# Realizando conexão com o MongoDB.
host = 'localhost'
port = 27017
cliente = MongoClient('127.0.0.1', port)
print("|============================>>>>> Script DML no MongoDB <<<<<=============================|")
print("  [ CONEXÃO MONGODB ESTABELECIDA ] >> [ SERVIDOR:",host,"PORTA:",port,"]")
name = input("  [ INFORME O NOME DA DATABASE: ] >> ")
database = cliente.get_database(name)
print("  [ CONEXÃO ESTABELECIDA ] >> [ DATABASE:",name," ]")
colecao = input("  [ INFORME O NOME DA COLLECTION ] >> ")

'''>>>>>>>>>> Estrutura e funções de pré-processamento. <<<<<<<<<'''
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

# Grava o cabeçalho no arquivo 'cabecalho' externo para novos processamentos
def writeDocument (cabecalho):
    with open('cabecalho', 'w', encoding='utf-8') as file:
        for campo in cabecalho:
            file.write(campo)
            file.write(', ')
        file.close()

'''>>>>>>>>>> Função que insere o documento no MongoDB <<<<<<<<<<'''
def insertMongo (col):
    result = col.insert(data)
    return result
'''>>>>>>>>>> Fim da função que insere o documentos no MongoDB <<<<<<<<<<'''
def readCSV (cabecalho, conteudo, col):
    with open('base.csv', 'r', encoding='utf8') as file:
        dados = csv.reader(file, delimiter=',')
        # Leituras das linhas e colunas do arquivo CSV.
        flag = False
        for linhas in dados:
            for j, row in enumerate(linhas):
                if flag:
                    conteudo.insert(j, row)
                    if j == 28:
                        createDocument(cabecalho, conteudo)
                        insertMongo(col)
                        conteudo.clear()
                else:
                    cabecalho.insert(j, row)
                    if row == "VEHICLE TYPE CODE 5":
                        writeDocument(cabecalho)
                        flag = True
'''>>>>>>>>>> Acaba aqui as funções de pré-processamento. <<<<<<<<<'''
'''>>>>>>>>>> Função de criação de coleções de documentos <<<<<<<<<<'''
def creatCollection(col):
    database.create_collection(colecao)
    print("  [ COLLECTION:",colecao," CRIADA ]")
    col = database[colecao]
    print("  [ CONEXÃO ESTABELECIDA ] >> [ COLLECTION:",colecao,"]")
    time.sleep(2)
'''>>>>>>>>>> Fim da função de criação de coleções de documentos <<<<<<<<<<'''
'''>>>>>>>>>> Função de criação de coleções de documentos <<<<<<<<<<'''
def realizaDrop():
    print("  [ DESEJA REALIZAR DROP EM ( 1 - DATABASE ) OU ( 2 - COLECTIONS )? ]")
    op = int(input("  [ DIGITE OPÇÃO DESEJADA ] >> "))
    if op == 1:
        cliente.drop_database(database)
        print("  [ DAATABASE:",database," EXCLUIDA COM SUCESSO! ]")
    elif op == 2:
        database.drop_collection(colecao)
        print("  [ COLLECTION:",colecao," EXCLUIDA COM SUCESSO! ]")
    else:
        print("  [ OPÇÃO NÃO ENCONTRADA, VOLTANDO AO MENU! ]")
    time.sleep(3)
'''>>>>>>>>>> Fim da função de criação de coleções de documentos <<<<<<<<<<'''
'''>>>>>>>>>> MENU PRINCIPAL <<<<<<<<<<'''
def menu (col):
    os.system('cls' if os.name == 'nt' else 'clear')
    print("|============================>>>>> Script DML no MongoDB <<<<<=============================|")
    print("  [ ESCOLHA UMA DAS OPÇÕES ABAIXO ]")
    print("  [ PARA CRIAR UMA NOVA COLLECTION >> 1 ]")
    print("  [ PARA EXCLUIR UMA DATABASE >> 2 ]")
    print("  [ PARA ATUALIZAR DOCUMENTO(S) >> 3 ]")
    print("  [ PARA SAIR DO SCRIPT DIGITE >> 4 ]")
    res = int(input("  [ DIGITE A OPÇÃO DESEJADA ] >> "))
    if res == 1:
        creatCollection(col)
    elif res == 2:
        realizaDrop()
    elif res == 4:
        return False
    return True
'''>>>>>>>>>> FIM MENU PRINCIPAL <<<<<<<<<<'''

if __name__ == "__main__":
    retorno = True
    cabecalho = []
    conteudo = []

    if colecao:
        col = database[colecao]
        print("  [ CONEXÃO ESTABELECIDA ] >> [ COLLECTION:",colecao,"]")
    else:
        print("  [ CONEXÃO NÃO ESTABELECIDA, O NOME DA COLLECTION É NECESSÁRIA ]")
        colecao = input("  [ DIGITE O NOME DA COLLECTION A SER CRIADA OU JÁ EXISTENTE ] >> ")
        print("  [ O NOME DIGITADO É PARA ( 1 - UMA NOVA COLLECTION ) OU UMA ( 2 - EXISTENTE )? ]")
        p = int(input("  [ DIGITE O VALOR ] >> "))
        if p == 1:
            creatCollection(col)
        if p == 2:
            col = database[colecao]
            print("  [ CONEXÃO ESTABELECIDA ] >> [ COLLECTION:",colecao,"]")
              
    resp = input("  [ DESEJA CARREGAR A BASE DE DADOS DO ARQUIVO 'base.csv'? (yes ou no) ] >> ")
    if resp == 'yes':
        readCSV(cabecalho, conteudo, col)
        print("  [ CSV LIDO E INSERÇÕES REALIZADAS ]")
    time.sleep(2)
    while retorno:
        retorno = menu(col)
    print("|==========================>>>>> ATÉ MAIS, TENHA UM BOM DIA <<<<<===========================|")
    cliente.close()