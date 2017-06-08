#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Script para extraer URL de perfiles Google Scholar"""
__author__ = 'John J. Villavicencio Sarango'

import time
import codecs
from bs4 import BeautifulSoup
import requests
import MySQLdb

# Establecer conexión con base de datos
__db_host__ = 'localhost'
__db_user__ = 'root'
__db_pass__ = 'root'
__db_name__ = 'Base_Autores'

__params__ = [__db_host__, __db_user__, __db_pass__, __db_name__]
__data_base__ = MySQLdb.connect(*__params__, charset='utf8', use_unicode=True)

__cursor__ = __data_base__.cursor()


def limpiarnombres(dato):
    ''' Limpieza de nombres y apellidos '''
    # lista de conectores
    conectores = [' a ', ' de ', ' De ', 'De La ', ' De La ', ' y ', ' con ', '-',\
     ' Del ', 'Del ', ' del ']

    for con in conectores:
        dato = dato.replace(con, ' ')

    dato = dato.split()
    return dato


def conexion_bd():
    """Consulta a base de datos semilla"""
    print('aqui')
    # SQL query para obtener autores sin perfil de Scholar
    sql = "SELECT \
                authApellido, \
                authNombre, \
                authid \
            FROM \
                AuthEcuador \
            WHERE \
                authVerificado = 'FALSE'"

    # Ejecutar comando SQL
    __cursor__.execute(sql)
    # Convertir resultado a lista
    results = __cursor__.fetchall()

    # Crear diccionario con datos del autor
    autor = {}
    for row in results:
        try:
            autor['apellido'] = limpiarnombres(row[0])
            autor['nombre'] = limpiarnombres(row[1])
            autor['authId'] = row[2]

            # Consultar posibles perfiles del autor en google scholar
            obtener_autores(autor)
        except AttributeError:
            continue

        # print ("UPDATE AuthEcuador \
        # SET authVerificado=%s WHERE authID='%s'"\
        # % ("TRUE", autor['authId']))

        #Insertar en Base de Datos la url del perfil
        __cursor__.execute("UPDATE AuthEcuador \
        SET authVerificado='%s' WHERE authID='%s'"\
        % ("TRUE", autor['authId']))

        #Guardar cambios en la Base de Datos
        __data_base__.commit()
    # Desconectar de la Base de Datos
    __data_base__.close()

def consultas_google(authId, autor):
    '''Realizar peticiones a Google Scholar'''
    # Armar url para scrapear posibles perfiles
    url_base_1 = 'https://scholar.google.es/citations?mauthors='
    url_base_2 = '+&hl=es&view_op=search_authors'

    url_base = url_base_1 + autor + url_base_2
    print(url_base)
    # Contador de páginas
    pagCounter = 1
    # Bandera para saber si hay mas de una página con resultados
    pagina = True
    # Scrapear mientras existan más páginas
    while pagina:
        if pagCounter > 2:
            pagina = False

        # Realizamos la petición a la web
        time.sleep(1)
        req = requests.get(url_base)
        # Comprobamos que la petición nos devuelve un Status Code = 200
        status_code = req.status_code

        if status_code == 200:
            # Pasamos el contenido HTML a un objeto BeautifulSoup()
            html = BeautifulSoup(req.text, "lxml")

            # Codificar pagina scrapeada a utf-8
            # Escapar comillas simples y dobles para html
            html2 = codecs.decode(MySQLdb.escape_string(str(html)), 'utf-8')

            # Verificamos que existan resultados
            noExisteResultado = html.find('div', {'class': 'gs_med'})
            if noExisteResultado != None:
                noExisteResultado = noExisteResultado.contents[0].text
                noExisteResultado = noExisteResultado[:19]
            else:
                noExisteResultado = 'existe'

            if noExisteResultado != 'No se ha encontrado':
                print('hay reusultados')
                #Insertar en Base de Datos la url del perfil

                __cursor__.execute("INSERT into Perfiles (Perfiles_AuthId,\
                Perfiles_Nombre_Coincidencia, Perfiles_HTML, Perfiles_URL)\
                values ('%d', '%s', '%s', '%s')"\
                % (int(authId), autor, html2, url_base))

                #Guardar cambios en la Base de Datos
                __data_base__.commit()

                # Obtenemos los div con el botón a siguiete página
                siguiente = html.find('div', {'id':'gsc_authors_bottom_pag'})

                #Si el botón a pagina siguiente esta habilitado
                if siguiente:
                    siguiente = siguiente.find('button', {'aria-label':'Siguiente'})
                    siguiente = siguiente.get('onclick')
                    if siguiente:
                        #Obtenemos la URL de la siguiente página
                        url_base = 'https://scholar.google.es'+u'{0}'.format(
                            siguiente.replace('\\x3d', '=').replace('\\x26', '&').replace('\'', '')\
                            .replace('window.location=', ''))

                        print(u"Página" + "siguiente: %s" % url_base)
                    else:
                        pagina = False
                else:
                    print('Pagína siguiente: no hay siguiente página')
                    pagina = False
            else:
                print(noExisteResultado)
                pagina = False

        else:
            print('Error petición %s' % status_code)



def obtener_autores(dat_autor):
    """Función obtener listado de coincidencia de autores"""

    nomb_count = len(dat_autor['nombre'])
    print(dat_autor['nombre'])
    print(nomb_count)
    apell_count = len(dat_autor['apellido'])
    print(dat_autor['apellido'])
    print(apell_count)

    if nomb_count >= 2 and apell_count >= 2:
        consultas_google(dat_autor['authId'], 'autor%3A'+dat_autor['nombre'][0]+'+autor%3A'+dat_autor['nombre'][1]+'+autor%3A'+dat_autor['apellido'][0]+'+autor%3A'+dat_autor['apellido'][1])
        consultas_google(dat_autor['authId'], 'autor%3A'+dat_autor['nombre'][0]+'+'+dat_autor['nombre'][1]+'+'+dat_autor['apellido'][0]+'+'+dat_autor['apellido'][1])
        consultas_google(dat_autor['authId'], 'autor%3A'+dat_autor['nombre'][0]+'+autor%3A'+dat_autor['nombre'][1]+'+autor%3A'+dat_autor['apellido'][0])
        consultas_google(dat_autor['authId'], 'autor%3A'+dat_autor['nombre'][0]+'+'+dat_autor['nombre'][1]+'+'+dat_autor['apellido'][0])
        consultas_google(dat_autor['authId'], 'autor%3A'+dat_autor['nombre'][0]+'+autor%3A'+dat_autor['apellido'][0])
        consultas_google(dat_autor['authId'], 'autor%3A'+dat_autor['nombre'][0]+'+'+dat_autor['apellido'][0])
    elif nomb_count >= 2 and apell_count == 1:
        consultas_google(dat_autor['authId'], 'autor%3A'+dat_autor['nombre'][0]+'+autor%3A'+dat_autor['nombre'][1]+'+autor%3A'+dat_autor['apellido'][0])
        consultas_google(dat_autor['authId'], 'autor%3A'+dat_autor['nombre'][0]+'+'+dat_autor['nombre'][1]+'+'+dat_autor['apellido'][0])
        consultas_google(dat_autor['authId'], 'autor%3A'+dat_autor['nombre'][0]+'+autor%3A'+dat_autor['apellido'][0])
        consultas_google(dat_autor['authId'], 'autor%3A'+dat_autor['nombre'][0]+'+'+dat_autor['apellido'][0])
    elif nomb_count == 1 and apell_count >= 2:
        consultas_google(dat_autor['authId'], 'autor%3A'+dat_autor['nombre'][0]+'+autor%3A'+dat_autor['apellido'][0]+'+autor%3A'+dat_autor['apellido'][1])
        consultas_google(dat_autor['authId'], 'autor%3A'+dat_autor['nombre'][0]+'+'+dat_autor['apellido'][0]+'+'+dat_autor['apellido'][1])
        consultas_google(dat_autor['authId'], 'autor%3A'+dat_autor['nombre'][0]+'+autor%3A'+dat_autor['apellido'][0])
        consultas_google(dat_autor['authId'], 'autor%3A'+dat_autor['nombre'][0]+'+'+dat_autor['apellido'][0])
    elif nomb_count == 1 and apell_count == 1:
        consultas_google(dat_autor['authId'], 'autor%3A'+dat_autor['nombre'][0]+'+autor%3A'+dat_autor['apellido'][0])
        consultas_google(dat_autor['authId'], 'autor%3A'+dat_autor['nombre'][0]+'+'+dat_autor['apellido'][0])
    else:
        print('Los datos no son suficientes')

    print('=========================')
# Ejecutar script

conexion_bd()
