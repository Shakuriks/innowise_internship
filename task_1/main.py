import argparse
from src.database_manager import DatabaseManager
from src.data_exporter import DataExporter

def main():
    parser = argparse.ArgumentParser(description="Заполнение базы данных и экспорт данных комнат.")
    parser.add_argument('--students', required=True, help="Путь к файлу студентов (JSON).")
    parser.add_argument('--rooms', required=True, help="Путь к файлу комнат (JSON).")
    parser.add_argument('--format', choices=['json', 'xml'], default='json', help="Выходной формат: json или xml.")
    parser.add_argument('--analyze', action='store_true', help="Включить выполнение EXPLAIN ANALYZE для запросов.")

    args = parser.parse_args()

    db_manager = DatabaseManager()

    DataExporter.populate_database(db_manager, args.rooms, args.students)

    DataExporter.export_rooms_with_student_count(db_manager, args.format, analyze=args.analyze)
    DataExporter.export_lowest_average_age_rooms(db_manager, args.format, analyze=args.analyze)
    DataExporter.export_rooms_with_largest_age_difference(db_manager, args.format, analyze=args.analyze)
    DataExporter.export_rooms_with_mixed_gender_students(db_manager, args.format, analyze=args.analyze)

if __name__ == "__main__":
    main()
