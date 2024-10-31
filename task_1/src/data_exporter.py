from .data_handler import DataHandler
from typing import Callable, Any, Optional, Dict, List

class DataExporter:
    @staticmethod
    def populate_database(db_manager: Any, rooms_filepath: str, students_filepath: str, batch_size: int = 100) -> None:
        """
        Заполняет базу данных данными о комнатах и студентах из указанных файлов.

        Args:
            db_manager (Any): Менеджер базы данных для выполнения операций.
            rooms_filepath (str): Путь к файлу с данными о комнатах.
            students_filepath (str): Путь к файлу с данными о студентах.
            batch_size (int, optional): Размер пакета для вставки. По умолчанию 100.
        """
        rooms_batch: List[Dict[str, Any]] = []
        students_batch: List[Dict[str, Any]] = []

        for room in DataHandler.read_data(rooms_filepath):
            rooms_batch.append(room)
            if len(rooms_batch) >= batch_size:
                db_manager.insert_rooms(rooms_batch)
                rooms_batch.clear()

        if rooms_batch:
            db_manager.insert_rooms(rooms_batch)

        for student in DataHandler.read_data(students_filepath):
            students_batch.append(student)
            if len(students_batch) >= batch_size:
                db_manager.insert_students(students_batch)
                students_batch.clear()

        if students_batch:
            db_manager.insert_students(students_batch)

    @staticmethod
    def export_data_decorator(method: Callable[[Any, bool], Any]) -> Callable[[Any, str, bool], None]:
        """
        Декоратор для экспорта данных с указанным форматом и анализом.

        Args:
            method (Callable[[Any, bool], Any]): Метод для экспорта данных.

        Returns:
            Callable[[Any, str, bool], None]: Обернутый метод с дополнительными аргументами.
        """
        def wrapper(db_manager: Any, file_format: str = 'json', analyze: bool = False) -> None:
            data = method(db_manager, analyze)
            filename = method.__name__.replace("export_", "")
            DataExporter._save_data(data, filename, file_format)
        return wrapper

    @staticmethod
    @export_data_decorator
    def export_rooms_with_student_count(db_manager: Any, analyze: bool) -> Optional[List[Dict[str, Any]]]:
        """
        Экспортирует данные о комнатах с количеством студентов.

        Args:
            db_manager (Any): Менеджер базы данных.
            analyze (bool): Флаг для выполнения анализа.

        Returns:
            Optional[List[Dict[str, Any]]]: Данные о комнатах с количеством студентов.
        """
        return db_manager.get_rooms_with_student_count(analyze=analyze)

    @staticmethod
    @export_data_decorator
    def export_lowest_average_age_rooms(db_manager: Any, analyze: bool) -> Optional[List[Dict[str, Any]]]:
        """
        Экспортирует данные о комнатах с самым низким средним возрастом студентов.

        Args:
            db_manager (Any): Менеджер базы данных.
            analyze (bool): Флаг для выполнения анализа.

        Returns:
            Optional[List[Dict[str, Any]]]: Данные о комнатах с самым низким средним возрастом.
        """
        return db_manager.get_rooms_with_lowest_average_age(analyze=analyze)

    @staticmethod
    @export_data_decorator
    def export_rooms_with_largest_age_difference(db_manager: Any, analyze: bool) -> Optional[List[Dict[str, Any]]]:
        """
        Экспортирует данные о комнатах с самой большой разницей в возрасте студентов.

        Args:
            db_manager (Any): Менеджер базы данных.
            analyze (bool): Флаг для выполнения анализа.

        Returns:
            Optional[List[Dict[str, Any]]]: Данные о комнатах с самой большой разницей в возрасте.
        """
        return db_manager.get_rooms_with_largest_age_difference(analyze=analyze)

    @staticmethod
    @export_data_decorator
    def export_rooms_with_mixed_gender_students(db_manager: Any, analyze: bool) -> Optional[List[Dict[str, Any]]]:
        """
        Экспортирует данные о комнатах с смешанным полом студентов.

        Args:
            db_manager (Any): Менеджер базы данных.
            analyze (bool): Флаг для выполнения анализа.

        Returns:
            Optional[List[Dict[str, Any]]]: Данные о комнатах с смешанным полом студентов.
        """
        return db_manager.get_rooms_with_mixed_gender_students(analyze=analyze)

    @staticmethod
    def _save_data(data: Optional[List[Dict[str, Any]]], filename: str, file_format: str) -> None:
        """
        Сохраняет данные в указанный формат и файл.

        Args:
            data (Optional[List[Dict[str, Any]]]): Данные для сохранения.
            filename (str): Имя выходного файла.
            file_format (str): Формат выходного файла (json или xml).
        """
        output_filepath = f"{filename}.{file_format}"
        if data is not None:
            if file_format == 'json':
                DataHandler.save_as_json(data, output_filepath)
            elif file_format == 'xml':
                DataHandler.save_as_xml(data, output_filepath)
            else:
                print("Неподдерживаемый формат")
