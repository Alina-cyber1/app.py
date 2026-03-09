import streamlit as st
import numpy as np

@st.cache_data(ttl=3600)
def load_domain_data(domain_clean):
    print(f"🔍 Загрузка данных для домена: {domain_clean}")
    # Возвращаем тестовые данные
    dates = np.array(['2020-01', '2020-02', '2020-03'])
    papers = np.array([10, 15, 20])
    patents = np.array([5, 7, 9])
    metrics = {
        'papers_total': 100,
        'patents_total': 50,
        'papers_cited_avg': 12.5,
        'papers_growth': 10,
        'patents_growth': 8,
        'time_lag': 3.2,
        'time_lag_change': '+0.3',
        'trend_score': 75,
        'trend_status': 'Стабильный рост',
        'ai_share': 25,
        'top_assignees': ['Компания А', 'Компания Б', 'Компания В'],
        'assignee_values': [120, 80, 60],
        'countries': ['США', 'Китай', 'Германия'],
        'country_values': [50, 30, 20]
    }
    return dates, papers, patents, metrics
