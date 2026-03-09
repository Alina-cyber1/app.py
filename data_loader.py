import streamlit as st
import numpy as np

@st.cache_data(ttl=3600)
def load_domain_data(domain_clean):
    """
    Возвращает тестовые данные для отладки.
    Позволяет приложению запуститься без реальных файлов.
    """
    print(f"🔍 Загрузка данных для домена: {domain_clean} (тестовый режим)")

    # Создаём месяцы с 2015 по 2025
    dates = []
    for year in range(2015, 2026):
        for month in range(1, 13):
            dates.append(f"{year}-{month:02d}")

    # Генерируем случайные данные для публикаций и патентов
    np.random.seed(42)  # для воспроизводимости
    papers = np.random.randint(50, 200, size=len(dates))
    patents = np.random.randint(20, 100, size=len(dates))

    # Сортируем по дате (на всякий случай)
    sorted_idx = np.argsort(dates)
    dates = np.array(dates)[sorted_idx]
    papers = papers[sorted_idx]
    patents = patents[sorted_idx]

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
        'top_assignees': ['Компания А', 'Компания Б', 'Компания В', 'Компания Г', 'Компания Д'],
        'assignee_values': [120, 85, 60, 45, 30],
        'countries': ['США', 'Китай', 'Япония', 'Германия', 'Южная Корея'],
        'country_values': [48, 32, 12, 5, 3]
    }

    print("✅ Тестовые метрики сгенерированы")
    return dates, papers, patents, metrics
