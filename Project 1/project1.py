"""
Joey Vesche
Veschejo
A60985934
External Resources Used:
1 https://wiki.python.org/moin/MiniDom
2 https://docs.python.org/3/library/csv.html#module-csv
3 https://docs.python.org/3/library/json.html#module-json
...
"""

import csv
import json
import requests
import xml.etree.ElementTree as ET
from xml.etree.ElementTree import Element, tostring
from xml.dom import minidom
import os
from xml.dom.minidom import parse


def read_csv_file(filename):
    """
    Takes a filename denoting a CSV formatted file.
    Returns an object containing the data from the file.
    The specific representation of the object is up to you.
    The data object will be passed to the write_*_files functions.
    """

    with open(filename) as file:
        reader = csv.reader(file)
        data = list(reader)

    return data

def write_csv_file(filename, data):
    """
    Takes a filename (to be writen to) and a data object 
    (created by one of the read_*_file functions). 
    Writes the data in the CSV format.
    """
    with open(filename, 'w', newline= '') as file:
        writer = csv.writer(file)
        writer.writerows(data)


def read_json_file(filename):
    """
    Similar to read_csv_file, except works for JSON files.
    """
    with open(filename) as file:
        data = json.load(file)

    return data

def write_json_file(filename, data):
    """
    Writes JSON files. Similar to write_csv_file.
    """
    with open(filename, 'w') as file:
        json.dump(data, file)

def read_xml_file(filename):
    """
    You should know the drill by now...
    """
    tree = ET.parse(filename)
    root = tree.getroot()
    lst = []

    for i in root.iter():
        lst.append({i.tag: i.text})
    return lst


def write_xml_file(filename, data):
    """
    Feel free to write what you want here.
    """
    root = minidom.Document()

    temp = None

    for i in data:
        for key, value in i.items():
            xml = root.createElement(key)
            root.appendChild(xml)
            data.pop(0)
            break
        break

    for i in data:
        for key, value in i.items():
            if value is None:
                child = root.createElement(key)
                xml.appendChild(child)
                temp = child

            else:
                productChild = root.createElement(key)
                txt = root.createTextNode(value)
                productChild.appendChild(txt)

                temp.appendChild(productChild)

    xml_str = root.toprettyxml(indent ='\t')

    with open(filename, 'w') as file:
        file.write(xml_str)
