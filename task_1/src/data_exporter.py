from .data_handler import DataHandler


class DataExporter:
    @staticmethod
    def populate_database(db_manager, rooms_filepath, students_filepath):
        for room in DataHandler.read_data(rooms_filepath):
            db_manager.insert_room(room)

        for student in DataHandler.read_data(students_filepath):
            db_manager.insert_student(student)
    
    @staticmethod
    def export_data_decorator(method):
        def wrapper(db_manager, file_format='json'):
            data = method(db_manager)
            filename = method.__name__.replace("export_", "")
            DataExporter._save_data(data, filename, file_format)
        return wrapper
    
    @staticmethod
    @export_data_decorator
    def export_rooms_with_student_count(db_manager):
        return db_manager.get_rooms_with_student_count()

    @staticmethod
    @export_data_decorator
    def export_lowest_average_age_rooms(db_manager):
        return db_manager.get_rooms_with_lowest_average_age()

    @staticmethod
    @export_data_decorator
    def export_rooms_with_largest_age_difference(db_manager):
        return db_manager.get_rooms_with_largest_age_difference()

    @staticmethod
    @export_data_decorator
    def export_rooms_with_mixed_gender_students(db_manager):
        return db_manager.get_rooms_with_mixed_gender_students()

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
