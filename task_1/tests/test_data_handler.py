import unittest
import os
import json
import xml.etree.ElementTree as ET
from unittest.mock import MagicMock, patch
from src.data_handler import DataHandler
from src.data_exporter import DataExporter
from unittest.mock import mock_open


class TestDataHandler(unittest.TestCase):

    def setUp(self):
        self.test_json_data = [
            {"id": 1, "name": "Room A"},
            {"id": 2, "name": "Room B"}
        ]
        self.test_xml_data = [
            {"id": 1, "name": "Student 1"},
            {"id": 2, "name": "Student 2"}
        ]
        self.json_output_path = 'results/test_output.json'
        self.xml_output_path = 'results/test_output.xml'

    def tearDown(self):
        if os.path.exists(self.json_output_path):
            os.remove(self.json_output_path)
        if os.path.exists(self.xml_output_path):
            os.remove(self.xml_output_path)
        
        if os.path.exists('results') and not os.listdir('results'):
            os.rmdir('results')

    @patch('builtins.open', new_callable=mock_open)
    @patch('ijson.items')
    def test_read_data(self, mock_ijson, mock_open):
        mock_open.return_value.read.side_effect = json.dumps(self.test_json_data) 
        mock_ijson.return_value = iter(self.test_json_data)
        
        result = list(DataHandler.read_data('dummy_path.json'))
        self.assertEqual(result, self.test_json_data)

    def test_save_as_json(self):
        DataHandler.save_as_json(self.test_json_data, 'test_output.json')
        with open(self.json_output_path, 'r') as f:
            data = json.load(f)
        self.assertEqual(data, self.test_json_data)

    def test_save_as_xml(self):
        DataHandler.save_as_xml(self.test_xml_data, 'test_output.xml')
        tree = ET.parse(self.xml_output_path)
        root = tree.getroot()
        
        self.assertEqual(len(root.findall('entry')), len(self.test_xml_data))
        for entry, expected in zip(root.findall('entry'), self.test_xml_data):
            self.assertEqual(entry.find('id').text, str(expected['id']))
            self.assertEqual(entry.find('name').text, expected['name'])