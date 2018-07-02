#!/usr/bin/python3
# -*- coding: utf-8 -*-
''' A DDL, Data Definition Language ou LInguagem de Definição de Dados,
apesar do nome não interage com os dados e sim com os objetos do banco.
São comandos desse tipo o CREATE, o ALTER e o DROP '''

#import os
#import sys
#import pandas as pd
#import numpy as np
import csv
import json
from pymongo import MongoClient

# Realizando conexão com o MongoDB.
host = 'localhost'
port = 27017
cliente = MongoClient(host, port)
print("Conexao mongo feita ...")
database = cliente.nypd
print("'database' selecionado ...")
collisions = database.collisions
print("'collection' selecionada ...")

'''  -------------- Estrutura e funções de pré-processamento. ---------------- '''
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
def writeDocument (cabecalho):
    #with open('cabecalho.json', 'w') as file:
    #    json.dump(data, file)
    with open('cabecalho.txt', 'w', encoding='utf-8') as file:
        #file.write(cabecalho)
        for campo in cabecalho:
            file.write(campo)
            file.write('\n')
        file.close()

def readCSV (cabecalho, conteudo):
    with open('base.csv', 'r', encoding='utf8') as file:
        dados = csv.reader(file, delimiter=',')
        # Leituras das linhas e colunas do arquivo CSV.
        flag = False
        for linhas in dados:
            for j, row in enumerate(linhas):
                if flag:
                    break
                    conteudo.insert(j, row)
                    if j == 28:
                        createDocument(cabecalho, conteudo)
                        #writeDocument()
                        insertMongo()
                        conteudo.clear()
                else:
                    cabecalho.insert(j, row)
                    if row == "VEHICLE TYPE CODE 5":
                        writeDocument(cabecalho)
                        flag = True
                # Após ler todos os dados grava no arquivo e insere no banco.      
            if flag:
                break
        file.close()
'''  -------------- Acaba aqui as funções de pré-processamento. ---------------- '''

if __name__ == "__main__":
    #parameters = input("Insira a ação DDL: ")
    cabecalho = []
    conteudo = []
    print("O arquivo 'CSV' estar sendo carregado, por favor agarde alguns minutos...")
    print("AUARDE ALGUNS MINUTOS...")
    readCSV(cabecalho, conteudo)
