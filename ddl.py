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

cabecalho = []
conteudo = []
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
def createData (cabe, dado):
    data[cabe] = dado

def createLocation (cabe, dado):
    local[cabe] = dado

def createPerson (cabe, dado):
    person[cabe] = dado

def createCyclist (cabe, dado):
    cyclist[cabe] = dado

def createPedestrians (cabe, dado):
    pedestrians[cabe] = dado

def createMotorist (cabe, dado):
    motorist[cabe] = dado

def createVehicle (cabe, dado):
    vehicle[cabe] = dado

def createFactor (cabe, dado):
    factor[cabe] = dado

# Insere o conteúdo do arquivo nos locais devidos
def createDocument ():
    createCyclist(cabecalho[14], conteudo[14])
    createCyclist(cabecalho[15], conteudo[15])
    createMotorist(cabecalho[16], conteudo[16])
    createMotorist(cabecalho[17], conteudo[17])
    createPedestrians(cabecalho[12], conteudo[12])
    createPedestrians(cabecalho[13], conteudo[13])
    createPerson(cabecalho[10], conteudo[10])
    createPerson(cabecalho[11], conteudo[11])
    createPerson("CYCLISTS", cyclist)
    createPerson("MOTORISTS", motorist)
    createPerson("PEDESTRIANS", pedestrians)
    createFactor(cabecalho[18], conteudo[18])
    createFactor(cabecalho[19], conteudo[19])
    createFactor(cabecalho[20], conteudo[20])
    createFactor(cabecalho[21], conteudo[21])
    createFactor(cabecalho[22], conteudo[22])
    createVehicle(cabecalho[24], conteudo[24])
    createVehicle(cabecalho[25], conteudo[25])
    createVehicle(cabecalho[26], conteudo[26])
    createVehicle(cabecalho[27], conteudo[27])
    createVehicle(cabecalho[28], conteudo[28])
    createLocation(cabecalho[2], conteudo[2])
    createLocation(cabecalho[3], conteudo[3])
    createLocation(cabecalho[4], conteudo[4])
    createLocation(cabecalho[5], conteudo[5])
    createLocation(cabecalho[6], conteudo[6])
    createLocation(cabecalho[7], conteudo[7])
    createLocation(cabecalho[8], conteudo[8])
    createLocation(cabecalho[9], conteudo[9])
    createData("_id", conteudo[23])
    createData(cabecalho[0], conteudo[0])
    createData(cabecalho[1], conteudo[1])
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
'''>>>>>>>>>> Função pré-processamento dos dados <<<<<<<<<<'''
def readCSV (cabecalho, conteudo):
    with open('base.csv', 'r', encoding='utf8') as file:
        dados = csv.reader(file, delimiter=',')
        # Leituras das linhas e colunas do arquivo CSV.
        flag = False
        for linhas in dados:
            for j, row in enumerate(linhas):
                if flag:
                    conteudo.insert(j, row)
                    if j == 28:
                        createDocument()
                        insertMongo()
                        conteudo.clear()
                else:
                    cabecalho.insert(j, row)
                    if row == "VEHICLE TYPE CODE 5":
                        flag = True
                # Após ler todos os dados grava no arquivo e insere no banco.      
            if flag:
                break
        file.close()
'''-------------- Acaba aqui as funções de pré-processamento. ----------------'''
def dropDatabase ():
    print("Coleção: x será excluido")
    x = cillisions.drop()

#create collection and create database, ao criar a coleção o base de dados se não existir será criada;
def createCollection ():
    #http://api.mongodb.com/python/current/api/pymongo/collection.html

if __name__ == "__main__":
    #parameters = input("Insira a ação DDL: ")

    print("O arquivo 'CSV' estar sendo carregado, por favor agarde alguns minutos...")
    print("AUARDE ALGUNS MINUTOS...")
    readCSV(cabecalho, conteudo)
