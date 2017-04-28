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
__db_name__ = 'Extraccion_Data'

__params__ = [__db_host__, __db_user__, __db_pass__, __db_name__]
__data_base__ = MySQLdb.connect(*__params__, charset='utf8', use_unicode=True)

__cursor__ = __data_base__.cursor()



def conexion_bd():
    """Consulta a base de datos semilla"""

       
    # SQL query para obtener autores sin perfil de Scholar
    sql = "SELECT APELLIDO, NOMBRE, EMAIL, CEDULA FROM Autor WHERE busqueda_1 IS NULL "

    #Ejecutar comando SQL
    __cursor__.execute(sql)
    # Convertir resultado a lista
    results = __cursor__.fetchall()

    # Crear diccionario con datos del autor
    autor = {}
    for row in results:
        autor['apellido'] = row[0].split()
        autor['nombre'] = row[1].split()
        autor['email'] = row[2]
        autor['cedula'] = row[3]
        # Consultar posibles perfiles del autor en google scholar
        if len(autor['apellido'][0]) <= 2:
            apellido_autor = autor['apellido'][0]+'+'+autor['apellido'][1]
        else:
            apellido_autor = autor['apellido'][0]

        obtener_autores(
            autor['nombre'][0]+'+'+apellido_autor,
            autor['cedula'], 
            autor
        )

        print ("UPDATE Autor \
        SET busqueda_1=%d WHERE CEDULA='%s'"\
        % (1, autor['cedula']))

        #Insertar en Base de Datos la url del perfil
        __cursor__.execute("UPDATE Autor \
        SET busqueda_1=%d WHERE CEDULA='%s'"\
        % (1, autor['cedula']))

        #Guardar cambios en la Base de Datos
        __data_base__.commit()
    # Desconectar de la Base de Datos
    __data_base__.close()

def obtener_autores(autor, cedula, dat_autor):
    """Función obtener listado de coincidencia de autores"""

    #Armar url para scrapear posibles perfiles
    url_base_1 = 'https://scholar.google.es/citations?mauthors=autor%3A'
    url_base_2 = '+&hl=es&view_op=search_authors'
    url_base = url_base_1 + autor + url_base_2

    print ('===================================')
    print (u'Autor: %s \n' % autor)
    print (u'Link scrapy: %s \n' % url_base)

    #Contador de posibles perfiles
    counter = 0
    #Variable para almacenar posibles perfiles
    lista = []
    #Bandera para saber si hay mas de una página con resultados
    pagina = True
    #Scrapear mientras existan más páginas
    while pagina:
        
        #resetear variable con resultados
        lista = []
        # Realizamos la petición a la web
        time.sleep(2)
        req = requests.get(url_base)
        # Comprobamos que la petición nos devuelve un Status Code = 200
        status_code = req.status_code

        if status_code == 200:
            # Pasamos el contenido HTML de la web a un objeto BeautifulSoup()
            html = BeautifulSoup(req.text)
            # TODO: Almacenar en base

            #Generar fecha y hora
            fecha = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            #Codificar pagina scrapeada a utf-8
            #Escapar comillas simples y dobles para html
            html2 = codecs.decode(MySQLdb.escape_string(str(html)), 'utf-8')

            #Insertar en Base de Datos la url del perfil
            __cursor__.execute("INSERT into Perfiles (Perfiles_Cedula,\
             Perfiles_Nombre_Coincidencia, Perfiles_HTML, Perfiles_URL, Perfiles_Fecha_Extraccion)\
              values ('%s', '%s', '%s', '%s', '%s')"\
            % (cedula, autor, html2, url_base, fecha))

            #Guardar cambios en la Base de Datos
            __data_base__.commit()

            
            # Obtenemos los div con el botón a siguiete página
            siguiente = html.find('div', {'id':'gsc_authors_bottom_pag'})
            print ('despues')
            print (siguiente)

            #Si el botón a pagina siguiente esta habilitado
            if siguiente:
                siguiente = siguiente.find('button', {'aria-label':'Siguiente'})
                siguiente = siguiente.get('onclick')
                if siguiente:
                    #Obtenemos la URL de la siguiente página
                    url_base = 'https://scholar.google.es'+u'{0}'.format(
                        siguiente.replace('\\x3d', '=').replace('\\x26', '&').replace('\'', '')\
                        .replace('window.location=', ''))

                    print (u"Página" + "siguiente: %s" % url_base)
                else:
                    pagina = False
            else:
                print ('Pagína siguiente: no hay siguiente página')
                pagina = False


        else:
            print ('Error petición %s' % status_code)
#Ejecutar script
conexion_bd()
