import os
import json
import ijson
import xml.etree.ElementTree as ET
from typing import Generator, Any, Dict, List

class DataHandler:
    @staticmethod
    def read_data(filepath: str) -> Generator[Dict[str, Any], None, None]:
        """
        Читает данные из файла в формате JSON с использованием ijson.

        Args:
            filepath (str): Путь к файлу для чтения.

        Yields:
            Generator[Dict[str, Any], None, None]: Генератор словарей с данными.
        """
        with open(filepath, 'r') as file:
            for item in ijson.items(file, 'item'):
                yield item

    @staticmethod
    def _prepare_output_filepath(filename: str) -> str:
        """
        Подготавливает путь для выходного файла, создавая директорию 'results', если она не существует.

        Args:
            filename (str): Имя выходного файла.

        Returns:
            str: Полный путь к выходному файлу.
        """
        output_directory = 'results'
        if not os.path.exists(output_directory):
            os.makedirs(output_directory)
        return os.path.join(output_directory, filename)

    @staticmethod
    def save_as_json(data: List[Dict[str, Any]], output_filepath: str) -> None:
        """
        Сохраняет данные в формате JSON в указанный выходной файл.

        Args:
            data (List[Dict[str, Any]]): Данные для сохранения.
            output_filepath (str): Имя выходного файла.
        """
        output_filepath = DataHandler._prepare_output_filepath(output_filepath)
        with open(output_filepath, 'w') as file:
            json.dump(data, file, indent=4)

    @staticmethod
    def save_as_xml(data: List[Dict[str, Any]], output_filepath: str) -> None:
        """
        Сохраняет данные в формате XML в указанный выходной файл.

        Args:
            data (List[Dict[str, Any]]): Данные для сохранения.
            output_filepath (str): Имя выходного файла.
        """
        output_filepath = DataHandler._prepare_output_filepath(output_filepath)

        root = ET.Element("root")
        for item in data:
            entry = ET.SubElement(root, "entry")
            for key, value in item.items():
                element = ET.SubElement(entry, key)
                element.text = str(value)

        tree = ET.ElementTree(root)
        tree.write(output_filepath, encoding='utf-8', xml_declaration=True)
