import psycopg2

class DatabaseManager:
    def __init__(self):
        try:
            db_url = "postgresql://postgres:postgres@localhost:5435/db_innowise_task_1"
            self.conn = psycopg2.connect(db_url)
            self.cursor = self.conn.cursor()
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

        create_index_query = """
        CREATE INDEX IF NOT EXISTS idx_students_room_id ON students(room_id);
        CREATE INDEX IF NOT EXISTS idx_students_birthday ON students(birthday);
        CREATE INDEX IF NOT EXISTS idx_students_sex ON students(sex);
        """

        try:
            self.cursor.execute(create_rooms_table)
            self.cursor.execute(create_students_table)
            self.cursor.execute(create_index_query)  # Создание индекса
            self.conn.commit()
        except Exception as e:
            print(f"Ошибка при создании таблиц или индекса: {e}")
            self.conn.rollback()
            
               
    def insert_room(self, room_data):
        # SQL запрос для добавления комнаты с заданным ID
        insert_room_query = """
        INSERT INTO rooms (id, name) VALUES (%s, %s) ON CONFLICT (id) DO NOTHING;
        """
        try:
            # Вставляем новую комнату
            self.cursor.execute(insert_room_query, (room_data['id'], room_data['name']))
            self.conn.commit()
            
        except Exception as e:
            print(f"Ошибка при добавлении комнаты: {e}")
            self.conn.rollback()


    def insert_student(self, student_data):
        # SQL запрос для добавления студента с заданным ID
        insert_student_query = """
        INSERT INTO students (id, name, sex, birthday, room_id)
        VALUES (%s, %s, %s, %s, %s) ON CONFLICT (id) DO NOTHING;
        """
        try:
            # Получаем ID комнаты
            self.cursor.execute("SELECT id FROM rooms WHERE id = %s", (student_data['room'],))
            room = self.cursor.fetchone()
            
            if room:
                # Вставляем нового студента
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
                
            
    def get_rooms_with_student_count(self):
        query = """
        SELECT r.id AS room_id, r.name AS room_name, COUNT(s.id) AS student_count
        FROM rooms r
        LEFT JOIN students s ON r.id = s.room_id
        GROUP BY r.id, r.name
        ORDER BY r.id;
        """
        try:
            self.cursor.execute(query)
            results = self.cursor.fetchall()
            
            # Преобразование результатов в список словарей
            rooms_with_counts = []
            for row in results:
                room_data = {
                    'room_id': row[0],
                    'room_name': row[1],
                    'student_count': row[2]
                }
                rooms_with_counts.append(room_data)

            return rooms_with_counts

        except Exception as e:
            print(f"Ошибка при получении данных о комнатах: {e}")
            return None
        
    def get_rooms_with_lowest_average_age(self, limit=5):
        query = """
        SELECT r.id AS room_id, r.name AS room_name
        FROM rooms r
        JOIN students s ON r.id = s.room_id
        GROUP BY r.id, r.name
        ORDER BY AVG(EXTRACT(YEAR FROM AGE(s.birthday))) ASC
        LIMIT %s;
        """
        try:
            self.cursor.execute(query, (limit,))
            results = self.cursor.fetchall()
            
            # Преобразование результатов в список словарей
            rooms_with_average_age = []
            for row in results:
                room_data = {
                    'room_id': row[0],
                    'room_name': row[1]
                }
                rooms_with_average_age.append(room_data)

            return rooms_with_average_age

        except Exception as e:
            print(f"Ошибка при получении данных о комнатах с самым маленьким средним возрастом студентов: {e}")
            return None


    def get_rooms_with_largest_age_difference(self, limit=5):
        query = """
        SELECT r.id AS room_id, r.name AS room_name
        FROM rooms r
        JOIN students s ON r.id = s.room_id
        GROUP BY r.id, r.name
        ORDER BY MAX(s.birthday) - MIN(s.birthday) DESC
        LIMIT %s;
        """
        try:
            self.cursor.execute(query, (limit,))
            results = self.cursor.fetchall()

            rooms = []
            for row in results:
                room_data = {
                    'room_id': row[0],
                    'room_name': row[1]
                }
                rooms.append(room_data)

            return rooms

        except Exception as e:
            print(f"Ошибка при получении данных о комнатах с самой большой разницей в возрасте студентов: {e}")
            return None

    
    def get_rooms_with_mixed_gender_students(self):
        query = """
        SELECT r.id AS room_id, r.name AS room_name
        FROM rooms r
        JOIN students s ON r.id = s.room_id
        GROUP BY r.id, r.name
        HAVING COUNT(DISTINCT s.sex) > 1;
        """
        try:
            self.cursor.execute(query)
            results = self.cursor.fetchall()
            
            # Преобразование результатов в список словарей
            rooms_with_mixed_genders = []
            for row in results:
                room_data = {
                    'room_id': row[0],
                    'room_name': row[1]
                }
                rooms_with_mixed_genders.append(room_data)

            return rooms_with_mixed_genders

        except Exception as e:
            print(f"Ошибка при получении данных о комнатах с разнополыми студентами: {e}")
            return None

        
    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()


    def __del__(self):
        self.close()

