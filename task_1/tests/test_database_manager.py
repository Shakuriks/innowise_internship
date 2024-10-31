import unittest
from unittest.mock import patch, MagicMock
from psycopg2.extensions import connection, cursor
from src.database_manager import DatabaseManager


class TestDatabaseManager(unittest.TestCase):

    def setUp(self):
        # Патчим соединение с базой данных и курсор для тестирования без реальной БД
        self.patcher_conn = patch('psycopg2.connect')
        self.mock_connect = self.patcher_conn.start()
        
        # Настраиваем mock-соединение и mock-курсор
        self.mock_conn = MagicMock(spec=connection)
        self.mock_cursor = MagicMock(spec=cursor)
        
        self.mock_connect.return_value = self.mock_conn
        self.mock_conn.cursor.return_value = self.mock_cursor

        
        # Создаем экземпляр DatabaseManager
        self.db_manager = DatabaseManager()
        self.db_manager.cursor.mogrify = MagicMock(side_effect=lambda query, vars: f"({vars[0]}, '{vars[1]}')".encode('utf-8'))


    def tearDown(self):
        self.patcher_conn.stop()

    def test_tables_exist(self):
        # Устанавливаем, что метод fetchone() возвращает True
        self.mock_cursor.fetchone.return_value = (True,)
        
        result = self.db_manager._tables_exist()
        self.assertEqual(self.mock_cursor.execute.call_count, 2)
        self.assertTrue(result)

    def test_create_tables(self):
        # Проверка вызова execute() для создания таблиц и индекса
        self.db_manager.create_tables()
        self.assertEqual(self.mock_cursor.execute.call_count, 4)

    def test_insert_rooms(self):
        # Подготавливаем данные для вставки комнат
        rooms_data = [{'id': 1, 'name': 'Room A'}, {'id': 2, 'name': 'Room B'}]
        
        # Запускаем тестируемый метод
        self.db_manager.insert_rooms(rooms_data)
        
        # Проверка того, что execute был вызван один раз с правильным запросом
        self.mock_cursor.execute.assert_called()
        self.mock_conn.commit.assert_called_once()

    def test_insert_students_with_valid_rooms(self):
        # Подготавливаем данные для вставки студентов
        students_data = [
            {'id': 1, 'name': 'Student 1', 'sex': 'M', 'birthday': '2000-01-01', 'room': 1},
            {'id': 2, 'name': 'Student 2', 'sex': 'F', 'birthday': '2001-01-01', 'room': 2}
        ]
        
        # Имитация существующих комнат
        self.mock_cursor.fetchall.return_value = [(1,), (2,)]
        
        # Запускаем тестируемый метод
        self.db_manager.insert_students(students_data)
        
        # Проверка вызова execute и commit
        self.mock_cursor.execute.assert_called()
        self.mock_conn.commit.assert_called_once()

    def test_get_rooms_with_student_count(self):
        # Подготавливаем данные для возврата из запроса
        mock_result = [{'room_id': 1, 'room_name': 'Room A', 'student_count': 2}]
        self.mock_cursor.fetchall.return_value = mock_result

        # Запускаем тестируемый метод
        result = self.db_manager.get_rooms_with_student_count()

        # Проверяем, что execute был вызван один раз и данные совпадают
        self.mock_cursor.execute.assert_called()
        self.assertEqual(result, mock_result)

    def test_get_rooms_with_lowest_average_age(self):
        # Подготавливаем данные для возврата из запроса
        mock_result = [{'room_id': 1, 'room_name': 'Room A'}]
        self.mock_cursor.fetchall.return_value = mock_result

        # Запускаем тестируемый метод
        result = self.db_manager.get_rooms_with_lowest_average_age()

        # Проверка вызова execute и правильности данных
        self.mock_cursor.execute.assert_called()
        self.assertEqual(result, mock_result)

    def test_get_rooms_with_largest_age_difference(self):
        # Подготавливаем данные для возврата из запроса
        mock_result = [{'room_id': 1, 'room_name': 'Room A'}]
        self.mock_cursor.fetchall.return_value = mock_result

        # Запускаем тестируемый метод
        result = self.db_manager.get_rooms_with_largest_age_difference()

        # Проверка вызова execute и правильности данных
        self.mock_cursor.execute.assert_called()
        self.assertEqual(result, mock_result)

    def test_get_rooms_with_mixed_gender_students(self):
        # Подготавливаем данные для возврата из запроса
        mock_result = [{'room_id': 1, 'room_name': 'Room A'}]
        self.mock_cursor.fetchall.return_value = mock_result

        # Запускаем тестируемый метод
        result = self.db_manager.get_rooms_with_mixed_gender_students()

        # Проверка вызова execute и правильности данных
        self.mock_cursor.execute.assert_called()
        self.assertEqual(result, mock_result)

    @patch.object(DatabaseManager, 'print_explain_results')
    def test_with_explain_analyze_decorator(self, mock_print_explain_results):
        # Подготавливаем данные для проверки EXPLAIN ANALYZE
        explain_result = [{'QUERY PLAN': 'Some explain analyze output'}]
        self.mock_cursor.fetchall.return_value = explain_result

        # Запускаем метод с флагом analyze=True
        self.db_manager.get_rooms_with_student_count(analyze=True)

        # Проверка вызова EXPLAIN ANALYZE и печати результата
        self.assertTrue(mock_print_explain_results.called)
        self.mock_cursor.execute.assert_called()
