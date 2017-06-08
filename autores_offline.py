#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Script para extraer URL de perfiles Google Scholar"""
__author__ = 'John J. Villavicencio Sarango'

import time
import re
import csv
import os
import os.path as path
import datetime
import codecs
from bs4 import BeautifulSoup
import requests
import MySQLdb

# Establecer conexión con base de datos
__db_host__ = 'localhost'
__db_user__ = 'root'
__db_pass__ = 'root'
__db_name__ = 'Google_Scholar'

__params__ = [__db_host__, __db_user__, __db_pass__, __db_name__]
__data_base__ = MySQLdb.connect(*__params__, charset='utf8', use_unicode=True)

__cursor__ = __data_base__.cursor()



def conexion_bd():
    """Consulta a base de datos semilla"""

    #Eliminar archivos temporales si existen
    if path.exists('autoresdat.csv'):
        os.remove('autoresdat.csv')
    if path.exists('articulosDat.csv'):
        os.remove('articulosDat.csv')
    if path.exists('autoresURL.csv'):
        os.remove('autoresURL.csv')

    #Crear archivo temporal para obtener posibles prefiles del autor
    csvsalida = open('autoresdat.csv', 'a')
    salida = csv.writer(csvsalida)
    salida.writerow(['CEDULA', 'CONT', 'USER_NAME', 'ID_USER', 'PREFERENCIAS'])
    del salida
    csvsalida.close()

    #Crear archivo temporal para obtener posibles articulos escritos por del autor
    csvsalida = open('articulosDat.csv', 'a')
    salida = csv.writer(csvsalida)
    salida.writerow(['ID_USER', 'ART_NAME', 'ART_URL', 'CANT_LETRAS', '%COINCIDENCIA'])
    del salida
    csvsalida.close()

    # SQL query para obtener autores sin perfil de Scholar
    sql = "SELECT APELLIDO, NOMBRE, EMAIL, TITULO, CEDULA, ID FROM autor_publicacion \
    WHERE ANALIZADO IS NOT NULL AND ANALIZADO_OFF IS NULL"

    #Ejecutar comando SQL
    __cursor__.execute(sql)
    # Convertir resultado a lista
    results = __cursor__.fetchall()

    # Crear diccionario con datos del autor
    autor = {}
    for row in results:
        autor['id'] = row[5]
        autor['nombre'] = row[1].split()
        autor['apellido'] = row[0].split()
        autor['email'] = row[2]
        autor['titulo'] = row[3]
        autor['cedula'] = row[4]
        # Consultar posibles perfiles del autor en google scholar
        obtener_autores(autor['nombre'][0]+'+'+autor['apellido'][0], autor['cedula'], autor)
        autor['titulo'] = autor['titulo'].replace("'",'"')
        print ("UPDATE autor_publicacion \
        SET ANALIZADO_OFF=%d WHERE TITULO='%s'"\
        % (1, autor['titulo']))

        #Insertar en Base de Datos la url del perfil
        __cursor__.execute("UPDATE autor_publicacion \
        SET ANALIZADO_OFF=%d WHERE TITULO='%s'"\
        % (1, autor['titulo']))

        #Guardar cambios en la Base de Datos
        __data_base__.commit()
    # Desconectar de la Base de Datos
    __data_base__.close()

def obtener_autores(autor, cedula, dat_autor):
    """Función obtener listado de coincidencia de autores"""


    # SQL query para obtener autores sin perfil de Scholar
    sql = ("SELECT id_html, id_autor, html, url FROM html \
    WHERE id_autor = %d") % (dat_autor['id'])

    #Ejecutar comando SQL
    __cursor__.execute(sql)
    # Convertir resultado a lista
    results = __cursor__.fetchall()

    for row in results:
        url_base = row[3]

        print '==================================='
        print u'Autor: %s \n' % autor
        print u'Link scrapy: %s \n' % url_base

        #Contador de posibles perfiles
        counter = 0
        #Variable para almacenar posibles perfiles
        lista = []
        # Pasamos el contenido HTML de la web a un objeto BeautifulSoup()
        html = BeautifulSoup(row[2])
        # Obtenemos todos los divs donde estan los perfiles
        entradas = html.find_all('div', {'class':'gsc_1usr gs_scl'})
        # Obtenemos los div con el botón a siguiete página
        # Recorremos todas las entradas para extraer el Nombre del perfil,
        # el link, y sus atributos
        for entrada in entradas:
            counter += 1
            #Armamos la URL de los posibles perfiles
            id_user = entrada.find('div', {'class' : 'gsc_1usr_photo'})
            id_user = id_user.find('a').get('href')
            id_user = 'https://scholar.google.es' + id_user

            #Extraemos el nombre de usuario
            user_name = entrada.find('h3', {'class' : 'gsc_1usr_name'})
            user_name = user_name.text

            #Extraemos los interéses del posible perfil
            intereses = entrada.find_all('a', {'class' : 'gsc_co_int'})
            atributo = ''
            for interes in intereses:
                coincidencias = re.search("(label:.*)", interes.get('href'))
                if coincidencias:
                    atributo = atributo + ' , ' + coincidencias.group(1)

            #Añadimos los posibles perfiles a una variable
            lista.append((cedula, counter, ''+unicode(user_name).encode("utf-8")+'', \
            ''+unicode(id_user).encode("utf-8")+'', atributo.encode('latin-1')))

        #Abrimos archivo temporal para almacenar posibles perfiles
        csvsalida = open('autoresdat.csv', 'a')
        salida = csv.writer(csvsalida)
        #Agregamos los posibles perfiles al archivo temporal
        salida.writerows(lista)
        del salida
        csvsalida.close()

    #Verificamos si hay posibles perfiles en el archivo temporal
    data_set = open('autoresdat.csv', 'r')
    __lineas__ = data_set.readlines()
    if len(__lineas__) >= 2:
        #Obtenemos los articulos escritos por los posibles perfiles
        obtener_articulos(dat_autor)
        #Consultamos cual es perfil que tiene mayor coincidencia
        obtener_perfil()


def obtener_articulos(autor):
    """Obtener artículos de cada posible perfil"""

    #Contador de artículos
    counter = 0
    #Leemos el archivo temporlar con la
    #informacion de los posibles perfiles
    reader = csv.reader(open('autoresdat.csv', 'rb'))
    lista = []
    next(reader, None)

    print u'Artículo semilla: %s \n' % autor['titulo']

    for row in enumerate(reader):
        #Pausamos dos segundos entre cada scrapeo
        time.sleep(2)
        #Obtenemos la URL del posible perfil para scrapear
        url_autor = row[1][3]
        req = requests.get(url_autor)
        status_code = req.status_code
        if status_code == 200:
            html = BeautifulSoup(req.text)
            entradas = html.find_all('tr', {'class':'gsc_a_tr'})
            lista = []
            for entrada in entradas:
                counter += 1
                articulo = entrada.find('td', {'class' : 'gsc_a_t'})
                #Obtener el nombre del artículo
                nombre_articulo = articulo.find('a').text
                #Obtener la URL del artículo
                articulo_url = 'https://scholar.google.es'+articulo.find('a').get('href')
                #Contador de palabras que coinciden
                cont_por = 0

                for letra in autor['titulo'].split():
                    if letra in unicode(nombre_articulo).encode("utf-8").split():
                        cont_por += 1
                #Porcentaje de coincidencia del articulo semilla con los articulos scrapeados
                porcentaje = cont_por * 100 / len(autor['titulo'].split())

                #Añadir articulos scrapeados a una lista
                lista.append((row[1][1], unicode(nombre_articulo).encode("utf-8"),\
                articulo_url, cont_por, porcentaje))

            #Crear archivo temporal con articulos scrapeados
            csvsalida = open('articulosDat.csv', 'a')
            salida = csv.writer(csvsalida)
            salida.writerows(lista)
            del salida
            csvsalida.close()


def obtener_perfil():
    """Obtener el perfil del articulo que tenga mayor porcentaje de coincidencias"""

    #abrir archivo temporal con el listado de artículos
    reader = csv.reader(open('articulosDat.csv', 'rb'))
    next(reader, None)
    num_mayor = 0
    url_user = ''
    id_user = ''
    articulo = ''
    cedula_user = ''
    datos = []

    #obtener el id del articulo que tiene el porcentaje mayor
    for row in enumerate(reader):
        if int(row[1][4]) > int(num_mayor):
            num_mayor = row[1][4]
            id_user = row[1][0]
            articulo = row[1][1]

    print u'Porcentaje coincidencia: %s \n' % str(num_mayor)
    print 'Articulo coincidencia: %s \n' % articulo

    #abrir acrivo temporal que tiene la información de los autores
    reader2 = csv.reader(open('autoresdat.csv', 'rb'))
    next(reader2, None)

    #Si esque hubo un articulo que coincidió y el porcentaje es mayor al 30%
    if id_user != '' and int(num_mayor) >= int(30):
        for row in enumerate(reader2):
            #Buscar cual es el autor del articulo que tuvo mayor coincidencia
            if int(row[1][1]) == int(id_user):
                url_user = row[1][3]
                cedula_user = row[1][0]

        #Si se identifico el perfil del articulo que coincidió
        if cedula_user != '':
            #SQL query to INSERT a record into the table FACTRESTTBL.
            print "SCHOLAR_URL: %s \n" % url_user

            #Insertar en Base de Datos la url del perfil
            __cursor__.execute("INSERT into perfil_google_off (CEDULA, SCHOLAR_ID, COINCIDENCIA,\
             ESTADO) values ('%s', '%s', %d, %d)"\
            % (cedula_user, url_user, int(num_mayor), 0))

            #Guardar cambios en la Base de Datos
            __data_base__.commit()

            #Crear archivo temporal con el perfil encontrado
            datos.append((id_user, cedula_user, num_mayor, url_user))
            csvsalida = open('autoresURL.csv', 'a')
            salida = csv.writer(csvsalida)
            salida.writerows(datos)
            del salida
            csvsalida.close()
        #Eliminar archivos temporales
        os.remove('autoresdat.csv')
        os.remove('articulosDat.csv')

        print "===================================="

        #Preparar archivos temporales para una nueva busqueda
        csvsalida = open('autoresdat.csv', 'a')
        salida = csv.writer(csvsalida)
        salida.writerow(['CEDULA', 'CONT', 'USER_NAME', 'ID_USER', 'PREFERENCIAS'])
        del salida
        csvsalida.close()

        csvsalida = open('articulosDat.csv', 'a')
        salida = csv.writer(csvsalida)
        salida.writerow(['ID_USER', 'ART_NAME', 'ART_URL', 'CANT_LETRAS', '%COINCIDENCIA'])
        del salida
        csvsalida.close()
    else:
        #Eliminar archivos temporales
        os.remove('autoresdat.csv')
        os.remove('articulosDat.csv')

#Ejecutar script
conexion_bd()
