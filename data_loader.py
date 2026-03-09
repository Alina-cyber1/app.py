import streamlit as st
import numpy as np

@st.cache_data(ttl=3600)
def load_domain_data(domain_clean):
    print(f"🔍 Загрузка данных для домена: {domain_clean} (тестовый режим)")
    
    # Генерируем случайные данные, но разные для каждого домена
    # Используем хеш названия домена как seed, чтобы данные были разными
    seed_value = hash(domain_clean) % 2**32
    np.random.seed(seed_value)
    
    # Создаём месяцы с 2015 по 2025
    dates = []
    for year in range(2015, 2026):
        for month in range(1, 13):
            dates.append(f"{year}-{month:02d}")
    
    # Случайные числа для публикаций и патентов
    papers = np.random.randint(5, 30, size=len(dates))
    patents = np.random.randint(2, 15, size=len(dates))
    
    # Метрики (все ключи, используемые в app.py)
    metrics = {
        'papers_total': int(np.sum(papers)),
        'patents_total': int(np.sum(patents)),
        'papers_cited_avg': 15.6,
        'papers_growth': 12,
        'patents_growth': 8,
        'time_lag': 3.5,
        'time_lag_change': '+0.4',
        'trend_score': 78,
        'trend_status': 'Стабильный рост',
        'ai_share': 32,
        'top_assignees': ['Компания А', 'Компания Б', 'Компания В'],
        'assignee_values': [120, 80, 60],
        'countries': ['США', 'Китай', 'Германия'],
        'country_values': [50, 30, 20]
    }
    
    print("✅ Тестовые метрики сгенерированы")
    return np.array(dates), papers, patents, metrics
