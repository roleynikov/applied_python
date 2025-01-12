import streamlit as st
import pandas as pd
import numpy as np
import requests
import plotly.express as px
import asyncio
import aiohttp

def detect_anomalies(data, data_grouped):
    anomalies = []
    for _, row in data_grouped.iterrows():
        mean = row['mean']
        std  = row['std']
        cur_season_city = data[data['season'] == row['season']]
        cur_season_city['anomaly'] = ~cur_season_city['temperature'].between(mean-2*std,mean +2*std)
        anomalies.append(cur_season_city)
    return pd.concat(anomalies)

async def get_current_temperature_async(city, api_key):
    url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if response.status == 200:
                data = await response.json()
                return data['main']['temp']
            elif response.status == 401:
                return {"error": "Invalid API key. Please see https://openweathermap.org/faq#error401 for more info."}
            else:
                return {"error": f"Failed to fetch data for {city}: {response.status}"}

st.title("Анализ температурных данных")
uploaded_file = st.file_uploader("Загрузите исторические данные", type=["csv"])
if uploaded_file:
    data = pd.read_csv(uploaded_file)
    cities = data['city'].unique()
    city = st.selectbox("Выбрать город", cities)
    city_data = data[data['city'] == city]
    st.subheader(f"Описательная статистика для города {city}")
    stats = city_data.groupby('season')['temperature'].agg(['mean', 'std']).reset_index()
    st.dataframe(stats)
    st.subheader("Временной ряд")
    anomalies = detect_anomalies(city_data, stats)
    fig = px.scatter(
        anomalies,
        x="timestamp",
        y="temperature",
        color="anomaly",
        title=f"Temperature time series for {city}",
        labels={"color": "Anomaly"}
    )
    st.plotly_chart(fig)
    st.subheader("Сезонные профили")
    fig_season = px.bar(
        stats,
        x="season",
        y="mean",
        error_y="std",
        title=f"Сезонные профили для города {city}",
        labels={"mean": "Средня температура"}
    )
    st.plotly_chart(fig_season)
    api_key = st.text_input("Введите свой OpenWeatherMap API ключ", type="password")
    if api_key:
        st.subheader("Текущая температура")
        try:
            current_temp = asyncio.run(get_current_temperature_async(city, api_key))
            if isinstance(current_temp, dict) and "error" in current_temp:
                st.error(current_temp["error"])
            else:
                st.write(f"Температура в городе {city}: {current_temp}°C")
                is_anomaly = detect_anomalies(pd.DataFrame({'temperature': [current_temp],'season': ['winter'],'city': [city]}), stats)['anomaly'].iloc[0]
                if is_anomaly:
                    st.warning("Текущая температура ненормальная для текущего сезона.")
                else:
                    st.success("Текущая температура нормальная для текущего сезона.")
        except Exception as e:
            st.error(f"Ошибка при получении текущей температуры: {str(e)}")
else:
    st.info("Прикрепите CSV файл для продолжения.")