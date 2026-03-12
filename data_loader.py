import streamlit as st
import pandas as pd
import numpy as np
from pathlib import Path

# Путь к summary-файлам (лежат в корневой папке проекта, но мы поднимемся на уровень выше)
project_root = Path(__file__).parent.parent
SUMMARY_DIR = project_root / "data" / "summary"

# Маппинг отображаемых имён на ключи доменов
DOMAIN_MAP = {
    "Полупроводники": "semiconductors",
    "Генная инженерия": "gene_engineering"
}

@st.cache_data(ttl=3600)
def load_domain_data(domain_clean):
    """
    Загружает данные для домена из предрасчитанных summary-файлов.
    Если файл не найден, генерирует тестовые данные (но такого не должно быть).
    """
    print(f"🔍 Загрузка данных для домена: {domain_clean}")
    
    domain_key = DOMAIN_MAP.get(domain_clean)
    if not domain_key:
        return _generate_fallback_data(domain_clean, "Неизвестный домен")
    
    summary_file = SUMMARY_DIR / f"{domain_key}_summary.parquet"
    if not summary_file.exists():
        # Если файла нет, пробуем загрузить временные ряды отдельно (если сохраняли)
        ts_file = SUMMARY_DIR / f"{domain_key}_timeseries.parquet"
        if ts_file.exists():
            # Загружаем временные ряды, а метрики возьмём тестовые
            ts_df = pd.read_parquet(ts_file)
            dates = ts_df['date'].tolist()
            papers = ts_df['papers'].tolist()
            # Патенты пока тестовые
            patents = [0] * len(dates)
            metrics = _generate_fallback_data(domain_clean)[3]  # только метрики
            return np.array(dates), np.array(papers), np.array(patents), metrics
        else:
            return _generate_fallback_data(domain_clean, f"Файл {summary_file} не найден")
    
    # Загружаем summary
    df = pd.read_parquet(summary_file)
    row = df.iloc[0]
    
    dates = row['dates']
    papers = row['papers']
    patents = row['patents']  # может быть список нулей или реальные данные
    
    metrics = {
        'papers_total': row['papers_total'],
        'patents_total': row['patents_total'],
        'papers_cited_avg': row['papers_cited_avg'],
        'papers_growth': row['papers_growth'],
        'patents_growth': row['patents_growth'],
        'time_lag': row['time_lag'],
        'time_lag_change': row['time_lag_change'],
        'trend_score': row['trend_score'],
        'trend_status': row['trend_status'],
        'ai_share': row['ai_share'],
        'top_assignees': row['top_assignees'],
        'assignee_values': row['assignee_values'],
        'countries': row['countries'],
        'country_values': row['country_values']
    }
    
    print("✅ Данные загружены из summary-файла")
    return np.array(dates), np.array(papers), np.array(patents), metrics

def _generate_fallback_data(domain_clean, error_msg=""):
    """Генерирует тестовые данные (как в вашем исходном коде)."""
    print(f"⚠️ Использую ТЕСТОВЫЕ данные для {domain_clean}. Причина: {error_msg}")
    dates = pd.date_range(start='2020-01-01', end='2025-12-01', freq='MS').strftime('%Y-%m').tolist()
    papers = np.random.poisson(lam=50, size=len(dates)).cumsum()
    patents = np.random.poisson(lam=30, size=len(dates)).cumsum()
    metrics = {
        'papers_total': int(papers[-1]),
        'patents_total': int(patents[-1]),
        'papers_cited_avg': round(np.random.uniform(10, 25), 1),
        'papers_growth': round(np.random.uniform(5, 15), 1),
        'patents_growth': round(np.random.uniform(8, 20), 1),
        'time_lag': round(np.random.uniform(2.5, 4.5), 1),
        'time_lag_change': f"+{round(np.random.uniform(0.1, 0.5), 1)}",
        'trend_score': np.random.randint(60, 95),
        'trend_status': np.random.choice(['Взрывной рост', 'Стабильный рост', 'Созревание']),
        'ai_share': np.random.randint(15, 45),
        'top_assignees': ['Тест-Компания А', 'Тест-Компания Б', 'Тест-Компания В'],
        'assignee_values': [150, 90, 45],
        'countries': ['США', 'Китай', 'Германия'],
        'country_values': [48, 32, 20]
    }
    return np.array(dates), np.array(papers), np.array(patents), metrics
