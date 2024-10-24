import os
import json
import ijson
import xml.etree.ElementTree as ET

class DataHandler:
    @staticmethod
    def read_data(filepath):
        with open(filepath, 'r') as file:
            for item in ijson.items(file, 'item'):
                yield item

    @staticmethod
    def save_as_json(data, output_filepath):
        output_directory = 'results'
        if not os.path.exists(output_directory):
            os.makedirs(output_directory)
            
        output_filepath = os.path.join(output_directory, output_filepath)
        
        with open(output_filepath, 'w') as file:
            json.dump(data, file, indent=4)

    @staticmethod
    def save_as_xml(data, output_filepath):
        output_directory = 'results'
        if not os.path.exists(output_directory):
            os.makedirs(output_directory)
            
        output_filepath = os.path.join(output_directory, output_filepath)
        
        root = ET.Element("root")
        for item in data:
            entry = ET.SubElement(root, "entry")
            for key, value in item.items():
                element = ET.SubElement(entry, key)
                element.text = str(value)

        tree = ET.ElementTree(root)
        tree.write(output_filepath, encoding='utf-8', xml_declaration=True)
