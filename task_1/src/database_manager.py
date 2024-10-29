import psycopg2
from psycopg2.extras import DictCursor
from functools import wraps

class DatabaseManager:
    def __init__(self):
        try:
            db_url = "postgresql://postgres:postgres@localhost:5435/db_innowise_task_1"
            self.conn = psycopg2.connect(db_url)
            self.cursor = self.conn.cursor(cursor_factory=DictCursor) 
            self.create_tables()
        except Exception as e:
            print(f"Ошибка подключения к базе данных: {e}")


    def create_tables(self):
        create_rooms_table = """
        CREATE TABLE IF NOT EXISTS rooms (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL
        );
        """

        create_students_table = """
        CREATE TABLE IF NOT EXISTS students (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            sex VARCHAR(1) NOT NULL,
            birthday TIMESTAMP NOT NULL,
            room_id INTEGER NOT NULL,
            FOREIGN KEY (room_id) REFERENCES rooms(id)
        );
        """

        #После анализа результатов EXPLAIN ANALYZE и просмотра количества сканов индексов
        #стало очевидно, что idx_students_birthday и idx_students_sex никак не оптимизируют выполнение запросов
        create_index_query = """
        CREATE INDEX IF NOT EXISTS idx_students_room_id ON students(room_id);
        --CREATE INDEX IF NOT EXISTS idx_students_birthday ON students(birthday);
        --CREATE INDEX IF NOT EXISTS idx_students_sex ON students(sex);
        """
        
        # test_index_profit_query = """
        # SET enable_seqscan = OFF;
        # """

        try:
            self.cursor.execute(create_rooms_table)
            self.cursor.execute(create_students_table)
            self.cursor.execute(create_index_query)
            #self.cursor.execute(test_index_profit_query)

            self.conn.commit()
        except Exception as e:
            print(f"Ошибка при создании таблиц или индекса: {e}")
            self.conn.rollback()
            
               
    def insert_room(self, room_data):
        insert_room_query = """
        INSERT INTO rooms (id, name) VALUES (%s, %s) ON CONFLICT (id) DO NOTHING;
        """
        try:
            self.cursor.execute(insert_room_query, (room_data['id'], room_data['name']))
            self.conn.commit()
            
        except Exception as e:
            print(f"Ошибка при добавлении комнаты: {e}")
            self.conn.rollback()

    def insert_student(self, student_data):
        insert_student_query = """
        INSERT INTO students (id, name, sex, birthday, room_id)
        VALUES (%s, %s, %s, %s, %s) ON CONFLICT (id) DO NOTHING;
        """
        try:
            self.cursor.execute("SELECT id FROM rooms WHERE id = %s", (student_data['room'],))
            room = self.cursor.fetchone()
            
            if room:
                self.cursor.execute(
                    insert_student_query, 
                    (student_data['id'], student_data['name'], student_data['sex'], student_data['birthday'], student_data['room'])
                )
                self.conn.commit()
            else:
                print(f"Комната с ID {student_data['room']} не найдена.")
        except Exception as e:
            print(f"Ошибка при добавлении студента: {e}")
            self.conn.rollback()
            
    @staticmethod
    def with_explain_analyze(func):
        @wraps(func)
        def wrapper(self, *args, analyze=False, **kwargs):
            query, params = func(self, *args, **kwargs)

            try:
                if analyze:
                    explain_query = f"EXPLAIN ANALYZE {query}"
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
    def get_rooms_with_student_count(self):
        query = """
        SELECT r.id AS room_id, r.name AS room_name, COUNT(s.id) AS student_count
        FROM rooms r
        LEFT JOIN students s ON r.id = s.room_id
        GROUP BY r.id, r.name
        ORDER BY r.id;
        """
        return query, None

    @with_explain_analyze
    def get_rooms_with_lowest_average_age(self, limit=5):
        query = """
        SELECT r.id AS room_id, r.name AS room_name
        FROM rooms r
        JOIN students s ON r.id = s.room_id
        GROUP BY r.id, r.name
        ORDER BY AVG(EXTRACT(YEAR FROM AGE(s.birthday))) ASC
        LIMIT %s;
        """
        return query, (limit,)

    @with_explain_analyze
    def get_rooms_with_largest_age_difference(self, limit=5):
        query = """
        SELECT r.id AS room_id, r.name AS room_name
        FROM rooms r
        JOIN students s ON r.id = s.room_id
        GROUP BY r.id, r.name
        ORDER BY MAX(s.birthday) - MIN(s.birthday) DESC
        LIMIT %s;
        """
        return query, (limit,)

    @with_explain_analyze
    def get_rooms_with_mixed_gender_students(self):
        query = """
        SELECT r.id AS room_id, r.name AS room_name
        FROM rooms r
        JOIN students s ON r.id = s.room_id
        GROUP BY r.id, r.name
        HAVING COUNT(DISTINCT s.sex) > 1;
        """
        return query, None

        
    def print_explain_results(self, explain_results):
        print("EXPLAIN ANALYZE результат:")
        for row in explain_results:
            print(row)
    
    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()


    def __del__(self):
        self.close()

