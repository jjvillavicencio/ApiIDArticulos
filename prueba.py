#!/usr/bin/env python
# -- coding: utf-8 --
"""Pruebas"""
from difflib import SequenceMatcher

def similar(palabra_a, palabra_b):
    """Comparador de cadenas"""
    return SequenceMatcher(None, palabra_a, palabra_b).ratio()

print similar("The use of new communication systems: UTPL brand and digital campaignes [Uso de noevas formas de comunicación: La marca universitaria UTPL y campañas digitales]", "Uso de nuevas formas de comunicación: La marca universitaria UTPL y campañas digitales")
