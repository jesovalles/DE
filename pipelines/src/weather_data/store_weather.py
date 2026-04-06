import sys
import psycopg2
import logging
from pathlib import Path
from airflow.hooks.base import BaseHook

# Ajuste de path para imports locales
sys.path.insert(0, str(Path(__file__).parent))

from fetch_weather import fetch_weather_data, save_to_csv


def get_postgres_connection():
    """
    Obtiene la conexión desde Airflow (Conn ID: postgres)
    """
    try:
        conn_info = BaseHook.get_connection("postgres")

        conn = psycopg2.connect(
            host=conn_info.host,
            port=conn_info.port,
            dbname=conn_info.schema,
            user=conn_info.login,
            password=conn_info.password,
        )

        return conn

    except Exception as e:
        logging.error(f"Database connection failed: {e}")
        raise


def store_weather_data():
    """
    Extrae datos del clima y los inserta/actualiza en PostgreSQL
    """

    # Conexión a la base de datos
    conn = get_postgres_connection()
    cur = conn.cursor()

    # Obtener datos
    weather_data = fetch_weather_data()

    if not weather_data:
        logging.warning("No data fetched. Skipping insert.")
        return

    # Guardar CSV 
    save_to_csv(weather_data)

    # Query de inserción con upsert
    insert_query = """
        INSERT INTO weather (city, temperature, humidity, weather_description, date)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (city, date)
        DO UPDATE SET
            temperature = EXCLUDED.temperature,
            humidity = EXCLUDED.humidity,
            weather_description = EXCLUDED.weather_description;
    """

    # Insertar datos
    for weather in weather_data:
        try:
            cur.execute(
                insert_query,
                (
                    weather["city"],
                    weather["temperature"],
                    weather["humidity"],
                    weather["description"],
                    weather["date"],
                ),
            )

            logging.info(f"Upserted weather for {weather['city']}")

        except Exception as e:
            logging.error(f"Error inserting {weather['city']}: {e}")

    # Commit
    try:
        conn.commit()
        logging.info("Transaction committed successfully")

    except Exception as e:
        logging.error(f"Commit failed: {e}")
        conn.rollback()

    # Cierre de conexión
    cur.close()
    conn.close()