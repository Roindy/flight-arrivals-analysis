import pandas as pd
from sqlalchemy import create_engine
from config import DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME
from pathlib import Path

def main():
    desktop = Path.home() / "Desktop"
    file_path = desktop / "report.xlsx"

    DATABASE_URL = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
    engine = create_engine(DATABASE_URL)

    # Запрос 1: Аэропорты по количеству прилётов
    query1 = """
        SELECT
            airport_name->>'ru' AS airport_name,
            COUNT(airport_name) AS flight_count
        FROM airports_data ad
        JOIN flights f ON ad.airport_code = f.arrival_airport
        GROUP BY airport_name
        ORDER BY flight_count DESC;
        LIMIT 1000;
    """

    # Запрос 2: Аэропорты по количеству прилётов по году и месяцу
    query2 = """
        SELECT
            airport_name->>'ru' AS airport_name,
            COUNT(*) AS arrival_flight_count,
            TO_CHAR(scheduled_arrival, 'YYYY.MM') AS year_month
        FROM airports_data ad
        JOIN flights f ON ad.airport_code = f.arrival_airport
        GROUP BY airport_name, TO_CHAR(scheduled_arrival, 'YYYY.MM')
        ORDER BY year_month DESC, arrival_flight_count DESC;
        LIMIT 1000;
    """

    # Запрос 3: Пассажиры и количество их прилётов по аэропорту
    query3 = """
        SELECT
            airport_name->>'ru' AS airport_name,
            t.passenger_name,
            COUNT(*) AS passenger_arrivals
        FROM airports_data ad
        JOIN flights f ON ad.airport_code = f.arrival_airport
        JOIN ticket_flights tf ON f.flight_id = tf.flight_id
        JOIN tickets t ON tf.ticket_no = t.ticket_no
        GROUP BY airport_name, t.passenger_name
        ORDER BY airport_name, passenger_arrivals DESC;
        LIMIT 1000;
    """

    # 3. Выгрузка в Date Frame
    try:
        df1 = pd.read_sql_query(query1, engine)
        df2 = pd.read_sql_query(query2, engine)
        df3 = pd.read_sql_query(query3, engine)
    except Exception as e:
        print(f"Ошибка при выполнении запроса: {e}")
        exit(1)

    # 5. Сохранение в Excel (разные листы)
    with pd.ExcelWriter(file_path) as writer:
        df1.to_excel(writer, sheet_name='Аэропорты по прилётам', index=False)
        df2.to_excel(writer, sheet_name='Прилёты по месяцам', index=False)
        df3.to_excel(writer, sheet_name='Пассажиры по аэропортам', index=False)

    if file_path.exists():
        print(f"Файл '{file_path}' успешно создан")
    else:
        print("Ошибка: Файл не создан")

if __name__ == "__main__":
    main()
