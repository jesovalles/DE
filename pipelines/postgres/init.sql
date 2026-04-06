CREATE TABLE IF NOT EXISTS weather (
    id SERIAL PRIMARY KEY,
    city TEXT,
    temperature FLOAT,
    humidity INT,
    weather_description TEXT,
    date DATE
);

ALTER TABLE weather
ADD CONSTRAINT unique_city_date UNIQUE (city, date);