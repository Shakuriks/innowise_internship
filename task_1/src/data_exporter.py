from .data_handler import DataHandler


class DataExporter:
    @staticmethod
    def populate_database(db_manager, rooms_filepath, students_filepath):
        for room in DataHandler.read_data(rooms_filepath):
            db_manager.insert_room(room)

        for student in DataHandler.read_data(students_filepath):
            db_manager.insert_student(student)
    
    @staticmethod
    def export_rooms_with_student_count(db_manager, file_format='json'):
        rooms_with_counts = db_manager.get_rooms_with_student_count()
        DataExporter._save_data(rooms_with_counts, 'rooms_with_student_count', file_format)

    @staticmethod
    def export_lowest_average_age_rooms(db_manager, file_format='json'):
        lowest_average_age_rooms = db_manager.get_rooms_with_lowest_average_age()
        DataExporter._save_data(lowest_average_age_rooms, 'lowest_average_age_rooms', file_format)

    @staticmethod
    def export_rooms_with_largest_age_difference(db_manager, file_format='json'):
        rooms_with_age_difference = db_manager.get_rooms_with_largest_age_difference()
        DataExporter._save_data(rooms_with_age_difference, 'rooms_with_largest_age_difference', file_format)

    @staticmethod
    def export_rooms_with_mixed_gender_students(db_manager, file_format='json'):
        rooms_with_mixed_genders = db_manager.get_rooms_with_mixed_gender_students()
        DataExporter._save_data(rooms_with_mixed_genders, 'rooms_with_mixed_genders', file_format)

    @staticmethod
    def _save_data(data, filename, file_format):
        output_filepath = f"{filename}.{file_format}"
        if data is not None:
            if file_format == 'json':
                DataHandler.save_as_json(data, output_filepath)
            elif file_format == 'xml':
                DataHandler.save_as_xml(data, output_filepath)
            else:
                print("Неподдерживаемый формат")
