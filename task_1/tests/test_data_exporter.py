import unittest
from unittest.mock import MagicMock, patch
from src.data_exporter import DataExporter
from src.data_handler import DataHandler
from src.database_manager import DatabaseManager



class TestDataExporter(unittest.TestCase):

    @patch('src.data_handler.DataHandler.read_data')
    def test_populate_database(self, mock_read_data):
        mock_read_data.side_effect = [
            [{'id': 1, 'name': 'Room A'}, {'id': 2, 'name': 'Room B'}],  # комнаты
            [{'id': 1, 'name': 'Student 1', 'sex': 'M', 'birthday': '2000-01-01', 'room_id': 1},
             {'id': 2, 'name': 'Student 2', 'sex': 'F', 'birthday': '2001-01-01', 'room_id': 2}]
        ]

        db_manager = MagicMock()

        DataExporter.populate_database(db_manager, 'rooms.json', 'students.json', batch_size=2)

        self.assertEqual(db_manager.insert_rooms.call_count, 1)

        self.assertEqual(db_manager.insert_students.call_count, 1) 

    @patch('src.data_handler.DataHandler.save_as_json')
    def test_export_rooms_with_student_count(self, mock_save_as_json):
        db_manager = DatabaseManager()

        db_manager.get_rooms_with_student_count = MagicMock(return_value=[{'room_id': 1, 'room_name': 'Room A', 'student_count': 2}])

        DataExporter.export_rooms_with_student_count(db_manager)

        mock_save_as_json.assert_called_once_with([{'room_id': 1, 'room_name': 'Room A', 'student_count': 2}], 'rooms_with_student_count.json')

    @patch('src.data_handler.DataHandler.save_as_json')
    def test_export_lowest_average_age_rooms(self, mock_save_as_json):
        db_manager = DatabaseManager()

        db_manager.get_rooms_with_lowest_average_age = MagicMock(return_value=[{'room_id': 1, 'room_name': 'Room A'}, {'room_id': 2, 'room_name': 'Room B'}])

        DataExporter.export_lowest_average_age_rooms(db_manager)

        mock_save_as_json.assert_called_once_with([{'room_id': 1, 'room_name': 'Room A'}, {'room_id': 2, 'room_name': 'Room B'}], 'lowest_average_age_rooms.json')
    
    @patch('src.data_handler.DataHandler.save_as_json')
    def test_export_rooms_with_largest_age_difference(self, mock_save_as_json):
        db_manager = DatabaseManager()

        db_manager.get_rooms_with_largest_age_difference = MagicMock(return_value=[{'room_id': 1, 'room_name': 'Room A'}, {'room_id': 2, 'room_name': 'Room B'}])

        DataExporter.export_rooms_with_largest_age_difference(db_manager)

        mock_save_as_json.assert_called_once_with([{'room_id': 1, 'room_name': 'Room A'}, {'room_id': 2, 'room_name': 'Room B'}], 'rooms_with_largest_age_difference.json')
        
    @patch('src.data_handler.DataHandler.save_as_json')
    def test_export_rooms_with_mixed_gender_students(self, mock_save_as_json):
        db_manager = DatabaseManager()

        db_manager.get_rooms_with_mixed_gender_students = MagicMock(return_value=[{'room_id': 1, 'room_name': 'Room A'}, {'room_id': 2, 'room_name': 'Room B'}])

        DataExporter.export_rooms_with_mixed_gender_students(db_manager)

        mock_save_as_json.assert_called_once_with([{'room_id': 1, 'room_name': 'Room A'}, {'room_id': 2, 'room_name': 'Room B'}], 'rooms_with_mixed_gender_students.json')

if __name__ == '__main__':
    unittest.main()
