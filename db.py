import psycopg2

import config


class DataBaseManager:
    def __init__(self, database=config.DB_NAME, user=config.DB_USER,
                 password=config.DB_PASSWORD, host=config.DB_HOST,
                 port=config.DB_PORT):
        self.connection = psycopg2.connect(
            database=database,
            user=user,
            password=password,
            host=host,
            port=port
        )
        self.connection.autocommit = True
        self.cursor = self.connection.cursor()
        self.create_table_company_forecasts()

    def create_table_company_forecasts(self):
        self.cursor.execute(
            """CREATE TABLE IF NOT EXISTS company_forecasts(
            id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
            company_id INTEGER NOT NULL,
            company_name VARCHAR(256) NOT NULL,
            fact_Qliq_data1 INTEGER NOT NULL,
            fact_Qliq_data2 INTEGER NOT NULL,
            fact_Qoil_data1 INTEGER NOT NULL,
            fact_Qoil_data2 INTEGER NOT NULL,
            forecast_Qliq_data1 INTEGER NOT NULL,
            forecast_Qliq_data2 INTEGER NOT NULL,
            forecast_Qoil_data1 INTEGER NOT NULL,
            forecast_Qoil_data2 INTEGER NOT NULL,
            date DATE NOT NULL
            );"""
        )

    def add_company_forecast(self, row):
        values = str()
        for idx, value in enumerate(row):
            values += f"'{value}'"
            if idx != len(row) - 1:
                values += ", "

        self.cursor.execute(
            f"""INSERT INTO company_forecasts (company_id, 
            company_name, fact_Qliq_data1, fact_Qliq_data2, 
            fact_Qoil_data1, fact_Qoil_data2, forecast_Qliq_data1, 
            forecast_Qliq_data2, forecast_Qoil_data1, forecast_Qoil_data2, 
            date) VALUES ({values});"""
        )

    def get_total(self):
        self.cursor.execute(
            """SELECT SUM(fact_Qliq_data1), SUM(fact_Qliq_data2), 
            SUM(fact_Qoil_data1), SUM(fact_Qoil_data2), 
            SUM(forecast_Qliq_data1), SUM(forecast_Qliq_data2), 
            SUM(forecast_Qoil_data1), SUM(forecast_Qoil_data2) 
            FROM company_forecasts GROUP BY date"""
        )
        return str(self.cursor.fetchall())
