import psycopg2
from psycopg2.extras import DictCursor
from psycopg2.extensions import connection, cursor
from typing import Callable, Any, Optional, Tuple, List, Dict
from functools import wraps

class DatabaseManager:
    def __init__(self) -> None:
        """
        Инициализация класса DatabaseManager.

        Устанавливает соединение с базой данных и проверяет наличие необходимых таблиц.
        Если таблицы отсутствуют, создаёт их.
        """
        try:
            db_url = "postgresql://postgres:postgres@localhost:5435/db_innowise_task_1"
            self.conn: connection = psycopg2.connect(db_url)
            self.cursor: cursor = self.conn.cursor(cursor_factory=DictCursor) 
            if not self._tables_exist():
                self.create_tables()
        except Exception as e:
            print(f"Ошибка подключения к базе данных: {e}")

    def _tables_exist(self) -> bool:
        """
        Проверяет существование таблиц в базе данных.

        Returns:
            bool: True, если таблицы 'rooms' и 'students' существуют, иначе False.
        """
        check_tables_query: str = """
        SELECT EXISTS (
            SELECT 1 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name IN ('rooms', 'students')
        );
        """
        try:
            self.cursor.execute(check_tables_query)
            return self.cursor.fetchone()[0]
        except Exception as e:
            print(f"Ошибка при проверке существования таблиц: {e}")
            return False
    
    def create_tables(self) -> None:
        """
        Создаёт таблицы 'rooms' и 'students', если они не существуют.
        Также создаёт индекс на поле room_id в таблице students.
        """
        create_rooms_table: str = """
        CREATE TABLE IF NOT EXISTS rooms (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL
        );
        """

        create_students_table: str = """
        CREATE TABLE IF NOT EXISTS students (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            sex VARCHAR(1) NOT NULL,
            birthday TIMESTAMP NOT NULL,
            room_id INTEGER NOT NULL,
            FOREIGN KEY (room_id) REFERENCES rooms(id)
        );
        """

        create_index_query: str = """
        CREATE INDEX IF NOT EXISTS idx_students_room_id ON students(room_id);
        """
        
        try:
            self.cursor.execute(create_rooms_table)
            self.cursor.execute(create_students_table)
            self.cursor.execute(create_index_query)
            self.conn.commit()
        except Exception as e:
            print(f"Ошибка при создании таблиц или индекса: {e}")
            self.conn.rollback()

    def insert_rooms(self, rooms_data: List[dict]) -> None:
        """
        Вставляет данные о комнатах в таблицу 'rooms'.

        Args:
            rooms_data (List[dict]): Список словарей с данными о комнатах.
        """
        values_str = ",".join(
            self.cursor.mogrify("(%s, %s)", (room['id'], room['name'])).decode('utf-8') 
            for room in rooms_data
        )
        insert_room_query = f"""
        INSERT INTO rooms (id, name) VALUES {values_str} 
        ON CONFLICT (id) DO NOTHING;
        """
        
        try:
            self.cursor.execute(insert_room_query)
            self.conn.commit()
        except Exception as e:
            print(f"Ошибка при добавлении комнат: {e}")
            self.conn.rollback()

    def insert_students(self, students_data: List[dict]) -> None:
        """
        Вставляет данные о студентах в таблицу 'students'.

        Args:
            students_data (List[dict]): Список словарей с данными о студентах.
        """
        room_ids = {student['room'] for student in students_data}
        
        check_rooms_query = "SELECT id FROM rooms WHERE id = ANY(%s)"
        self.cursor.execute(check_rooms_query, (list(room_ids),))
        existing_rooms = {row[0] for row in self.cursor.fetchall()}
        
        valid_students = [student for student in students_data if student['room'] in existing_rooms]
        if not valid_students:
            print("Нет студентов с существующими комнатами для добавления.")
            return

        values_str = ",".join(
            self.cursor.mogrify("(%s, %s, %s, %s, %s)", (
                student['id'], 
                student['name'], 
                student['sex'], 
                student['birthday'], 
                student['room']
            )).decode('utf-8') 
            for student in valid_students
        )
        
        insert_student_query = f"""
        INSERT INTO students (id, name, sex, birthday, room_id) 
        VALUES {values_str} ON CONFLICT (id) DO NOTHING;
        """
        
        try:
            self.cursor.execute(insert_student_query)
            self.conn.commit()
        except Exception as e:
            print(f"Ошибка при добавлении студентов: {e}")
            self.conn.rollback()

    @staticmethod
    def with_explain_analyze(func: Callable[..., Tuple[str, Optional[tuple]]]) -> Callable[..., Optional[List[Dict[str, Any]]]]:
        """
        Декоратор для выполнения запроса с использованием EXPLAIN ANALYZE.

        Args:
            func (Callable[..., Tuple[str, Optional[tuple]]]): Функция запроса.

        Returns:
            Callable[..., Optional[List[Dict[str, Any]]]]: Обёртка для функции с возможностью выполнения EXPLAIN ANALYZE.
        """
        @wraps(func)
        def wrapper(self, *args: Any, analyze: bool = False, **kwargs: Any) -> Optional[List[Dict[str, Any]]]:
            query, params = func(self, *args, **kwargs)
            try:
                if analyze:
                    explain_query: str = f"EXPLAIN ANALYZE {query}"
                    self.cursor.execute(explain_query, params)
                    explain_results = self.cursor.fetchall()
                    self.print_explain_results(explain_results)

                self.cursor.execute(query, params)
                return [dict(row) for row in self.cursor.fetchall()]

            except Exception as e:
                print(f"Ошибка при выполнении запроса: {e}")
                return None
        return wrapper
                
    @with_explain_analyze
    def get_rooms_with_student_count(self) -> Tuple[str, Optional[tuple]]:
        """
        Получает список комнат с количеством студентов в каждой.

        Returns:
            Tuple[str, Optional[tuple]]: Запрос и параметры для выполнения.
        """
        query: str = """
        SELECT r.id AS room_id, r.name AS room_name, COUNT(s.id) AS student_count
        FROM rooms r
        LEFT JOIN students s ON r.id = s.room_id
        GROUP BY r.id, r.name
        ORDER BY r.id;
        """
        return query, None

    @with_explain_analyze
    def get_rooms_with_lowest_average_age(self, limit: int = 5) -> Tuple[str, tuple]:
        """
        Получает список комнат с самым низким средним возрастом студентов.

        Args:
            limit (int): Максимальное количество возвращаемых комнат.

        Returns:
            Tuple[str, tuple]: Запрос и параметры для выполнения.
        """
        query: str = """
        SELECT r.id AS room_id, r.name AS room_name
        FROM rooms r
        JOIN students s ON r.id = s.room_id
        GROUP BY r.id, r.name
        ORDER BY AVG(EXTRACT(YEAR FROM AGE(s.birthday))) ASC
        LIMIT %s;
        """
        return query, (limit,)

    @with_explain_analyze
    def get_rooms_with_largest_age_difference(self, limit: int = 5) -> Tuple[str, tuple]:
        """
        Получает список комнат с самой большой разницей в возрасте студентов.

        Args:
            limit (int): Максимальное количество возвращаемых комнат.

        Returns:
            Tuple[str, tuple]: Запрос и параметры для выполнения.
        """
        query: str = """
        SELECT r.id AS room_id, r.name AS room_name
        FROM rooms r
        JOIN students s ON r.id = s.room_id
        GROUP BY r.id, r.name
        ORDER BY MAX(s.birthday) - MIN(s.birthday) DESC
        LIMIT %s;
        """
        return query, (limit,)

    @with_explain_analyze
    def get_rooms_with_mixed_gender_students(self) -> Tuple[str, Optional[tuple]]:
        """
        Получает список комнат, в которых учатся студенты различных полов.

        Returns:
            Tuple[str, Optional[tuple]]: Запрос и параметры для выполнения.
        """
        query: str = """
        SELECT r.id AS room_id, r.name AS room_name
        FROM rooms r
        JOIN students s ON r.id = s.room_id
        GROUP BY r.id, r.name
        HAVING COUNT(DISTINCT s.sex) > 1;
        """
        return query, None

    def print_explain_results(self, explain_results: List[Dict[str, Any]]) -> None:
        """
        Печатает результаты выполнения EXPLAIN ANALYZE.

        Args:
            explain_results (List[Dict[str, Any]]): Результаты выполнения EXPLAIN ANALYZE.
        """
        print("EXPLAIN ANALYZE результат:")
        for row in explain_results:
            print(row)

    def close(self) -> None:
        """
        Закрывает соединение с базой данных и курсор.
        """
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

    def __del__(self) -> None:
        """
        Вызывает метод close() при удалении объекта класса DatabaseManager.
        """
        self.close()
