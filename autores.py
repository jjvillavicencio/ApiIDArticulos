#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Script para pruebas de código"""
__author__ = 'John Villavicencio'

import time
import re
import csv
import os
from bs4 import BeautifulSoup
import requests
import MySQLdb

def conexion_bd():
    """Conectar con base de datos"""
    os.remove('autoresdat.csv')
    os.remove('articulosDat.csv')
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
    # Open database connection
    db_host = 'localhost'
    db_user = 'root'
    db_pass = 'root'
    db_name = 'Google_Scholar'

    params = [db_host, db_user, db_pass, db_name]
    data_base = MySQLdb.connect(*params, charset='utf8', use_unicode=True)
    # prepare a cursor object using cursor() method
    cursor = data_base.cursor()
    # Prepare SQL query to INSERT a record into the database.
    sql = "SELECT APELLIDO, NOMBRE, EMAIL, TITULO, CEDULA FROM autor_publicacion LIMIT 2"
    #sql = 'SELECT APELLIDO, NOMBRE, EMAIL, TITULO, CEDULA FROM autor_publicacion WHERE CEDULA = "1103428544" LIMIT 1'
    #try:
        # Execute the SQL command
    cursor.execute(sql)
    # Fetch all the rows in a list of lists.
    results = cursor.fetchall()
    # Diccionario con datos del autor
    autor = {}
    for row in results:
        autor['nombre'] = row[1].split()
        autor['apellido'] = row[0].split()
        autor['email'] = row[2]
        autor['titulo'] = row[3]
        autor['cedula'] = row[4]
        # Consultar autor en scholar
        obtener_autores(autor['nombre'][0]+'+'+autor['apellido'][0], autor['cedula'], autor)
    #except ValueError:
    #    print "Error: No se puede acceder a los datos BD"
    # disconnect from server
    data_base.close()

def obtener_articulos(autor):
    """Obtener articulos de cada autor"""
    counter = 0
    reader = csv.reader(open('autoresdat.csv', 'rb'))
    lista = []
    next(reader, None)
    for row in enumerate(reader):
        time.sleep(2)
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
                nombre_articulo = articulo.find('a').text
                articulo_url = 'https://scholar.google.es'+articulo.find('a').get('href')
                cont_por = 0
                for letra in autor['titulo'].split():
                    if letra in unicode(nombre_articulo).encode("utf-8").split():
                        cont_por += 1
                #print cont_por
                porcentaje = cont_por * 100 / len(autor['titulo'].split())
                #print porcentaje

                lista.append((row[1][1], unicode(nombre_articulo).encode("utf-8"),\
                articulo_url, cont_por, porcentaje))

            csvsalida = open('articulosDat.csv', 'a')
            salida = csv.writer(csvsalida)
            salida.writerows(lista)
            del salida
            csvsalida.close()


def obtener_autores(autor, cedula, dat_autor):
    """Funcion obtener listado de coincidencia de autores"""
    url_base_1 = 'https://scholar.google.es/citations?mauthors=autor%3A'
    url_base_2 = '+&hl=es&view_op=search_authors'
    url_base = url_base_1 + autor + url_base_2
    print url_base
    counter = 0
    lista = []
    pagina = True

    while pagina:
        lista = []
        # Realizamos la petición a la web
        req = requests.get(url_base)
        # Comprobamos que la petición nos devuelve un Status Code = 200
        status_code = req.status_code
        if status_code == 200:

            # Pasamos el contenido HTML de la web a un objeto BeautifulSoup()
            html = BeautifulSoup(req.text)

            # Obtenemos todos los divs donde estan las entradas
            entradas = html.find_all('div', {'class':'gsc_1usr gs_scl'})
            siguiente = html.find('div', {'id':'gsc_authors_bottom_pag'})
            if siguiente:
                siguiente = siguiente.find('button', {'aria-label':'Siguiente'})
                siguiente = siguiente.get('onclick')
                if siguiente:
                    print u'{0}'.format(siguiente.replace('\\x3d', '=').replace('\\x26', '&'))
                    url_base = 'https://scholar.google.es'+u'{0}'.format(
                        siguiente.replace('\\x3d', '=').replace('\\x26', '&').replace('\'', '')\
                        .replace('window.location=', ''))
                    print "base: %s" % url_base
                else:
                    pagina = False
            else:
                print 'no hay siguiente'
                pagina = False

            # Recorremos todas las entradas para extraer el título, autor y fecha
            for entrada in entradas:
                counter += 1
                id_user = entrada.find('div', {'class' : 'gsc_1usr_photo'})
                id_user = id_user.find('a').get('href')
                id_user = 'https://scholar.google.es' + id_user

                user_name = entrada.find('h3', {'class' : 'gsc_1usr_name'})
                user_name = user_name.text

                intereses = entrada.find_all('a', {'class' : 'gsc_co_int'})
                atributo = ''
                for interes in intereses:
                    coincidencias = re.search("(label:.*)", interes.get('href'))
                    if coincidencias:
                        # This is reached.
                        atributo = atributo + ' , ' + coincidencias.group(1)

                lista.append((cedula, counter, ''+unicode(user_name).encode("utf-8")+'', \
                ''+unicode(id_user).encode("utf-8")+'', atributo.encode('latin-1')))
            #print lista
                #print "%d - %s - %s" %(counter, id_user, atributo)

            csvsalida = open('autoresdat.csv', 'a')
            salida = csv.writer(csvsalida)
            salida.writerows(lista)
            del salida
            csvsalida.close()

    obtener_articulos(dat_autor)
    obtener_perfil()


def obtener_perfil():
    """Obtener el perfil del usuario que tenga mayor porcentaje de coincidencias"""

    reader = csv.reader(open('articulosDat.csv', 'rb'))
    next(reader, None)
    num_mayor = 0
    url_user = ''
    id_user = ''
    datos = []
    for row in enumerate(reader):
        if int(row[1][4]) > int(num_mayor):
            num_mayor = row[1][4]
            id_user = row[1][0]
    print str(num_mayor) + '\n' + url_user

    reader2 = csv.reader(open('autoresdat.csv', 'rb'))
    next(reader2, None)
    for row in enumerate(reader2):
        print str(row[1][1])+ " - "+ str(id_user)
        if int(row[1][1]) == int(id_user):
            url_user = row[1][3]

    datos.append((id_user, num_mayor, url_user))
    csvsalida = open('autoresURL.csv', 'a')
    salida = csv.writer(csvsalida)
    salida.writerows(datos)
    del salida
    csvsalida.close()
    os.remove('autoresdat.csv')
    os.remove('articulosDat.csv')
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
#obtener_perfil()
conexion_bd()
#obtener_autores('nelson+piedra')
#obtener_articulos()
