import streamlit as st
import pandas as pd
import numpy as np
import duckdb
from pathlib import Path
import gdown

# Директория для хранения скачанных файлов
DATA_DIR = Path(__file__).parent / "data" / "processed"
DATA_DIR.mkdir(parents=True, exist_ok=True)

# Словарь с именами файлов и их Google Drive ID
# Используем те же имена, что создаёт create_data.py (без "_full")
FILES = {
    "semiconductors_clean.parquet": "1CwfeO6WY7gKqov5mAtaD1ffvsxBzdjR5",
    "gene_engineering_clean.parquet": "1mLx-mh1k4M4zNAOATLQiFXy06s0tfZHl"
}

def download_all_files():
    """Скачивает файлы, если они ещё не существуют на диске."""
    for filename, file_id in FILES.items():
        dest = DATA_DIR / filename
        if not dest.exists():
            with st.spinner(f"Скачивание {filename}..."):
                url = f"https://drive.google.com/uc?id={file_id}"
                gdown.download(url, str(dest), quiet=False)
                print(f"✅ Скачан {filename}")

# Вызываем скачивание при импорте модуля
download_all_files()

@st.cache_data(ttl=3600)
def load_domain_data(domain_clean):
    """
    Загружает данные для указанного домена.
    Возвращает:
        dates: np.array месяцев в формате 'YYYY-MM'
        papers: np.array количества публикаций по месяцам
        patents: np.array количества патентов по месяцам
        metrics: dict с общими метриками
    """
    print(f"🔍 Загрузка данных для домена: {domain_clean}")

    # Определяем файл для данного домена (в нём есть и публикации, и патенты)
    if domain_clean == "Полупроводники":
        data_file = DATA_DIR / "semiconductors_clean.parquet"
    elif domain_clean == "Генная инженерия":
        data_file = DATA_DIR / "gene_engineering_clean.parquet"
    else:
        return np.array([]), np.array([]), np.array([]), {}

    if not data_file.exists():
        st.error(f"❌ Файл данных не найден: {data_file}")
        return np.array([]), np.array([]), np.array([]), {}

    con = duckdb.connect()

    # ----- Публикации: помесячная статистика -----
    query_papers = f"""
        SELECT 
            strftime(publication_date, '%Y-%m') as month,
            COUNT(*) as count
        FROM read_parquet('{data_file}')
        WHERE publication_date IS NOT NULL AND type = 'publication'
        GROUP BY month
        ORDER BY month
    """
    try:
        df_papers = con.execute(query_papers).df()
        dates_papers = df_papers['month'].tolist()
        papers_counts = df_papers['count'].tolist()
        print(f"📊 Найдено {len(dates_papers)} месяцев с публикациями")
    except Exception as e:
        print(f"Ошибка при обработке публикаций: {e}")
        dates_papers, papers_counts = [], []

    # Общее число публикаций
    try:
        papers_total = con.execute(f"SELECT COUNT(*) FROM read_parquet('{data_file}') WHERE type = 'publication'").fetchone()[0]
    except:
        papers_total = 0

    # Среднее цитирование (только для публикаций)
    try:
        cited_avg = con.execute(f"SELECT AVG(citations) FROM read_parquet('{data_file}') WHERE type = 'publication' AND citations IS NOT NULL").fetchone()[0]
        cited_avg = round(cited_avg, 2) if cited_avg else 0
    except:
        cited_avg = 0

    # ----- Патенты: помесячная статистика -----
    query_patents = f"""
        SELECT 
            strftime(publication_date, '%Y-%m') as month,
            COUNT(*) as count
        FROM read_parquet('{data_file}')
        WHERE publication_date IS NOT NULL AND type = 'patent'
        GROUP BY month
        ORDER BY month
    """
    try:
        df_patents = con.execute(query_patents).df()
        dates_patents = df_patents['month'].tolist()
        patents_counts = df_patents['count'].tolist()
        print(f"📊 Найдено {len(dates_patents)} месяцев с патентами")
    except Exception as e:
        print(f"Ошибка при обработке патентов: {e}")
        dates_patents, patents_counts = [], []

    # Общее число патентов
    try:
        patents_total = con.execute(f"SELECT COUNT(*) FROM read_parquet('{data_file}') WHERE type = 'patent'").fetchone()[0]
    except:
        patents_total = 0

    # ----- Объединение временных рядов -----
    all_months = sorted(set(dates_papers) | set(dates_patents))
    if not all_months:
        print("⚠️ Нет данных ни по одному из источников, возвращаем заглушки.")
        empty_metrics = {
            'papers_total': 0,
            'patents_total': 0,
            'papers_cited_avg': 0,
            'papers_growth': 0,
            'patents_growth': 0,
            'time_lag': 0,
            'time_lag_change': '0',
            'trend_score': 0,
            'trend_status': 'Нет данных',
            'ai_share': 0,
            'top_assignees': ['Нет данных'],
            'assignee_values': [0],
            'countries': ['Нет данных'],
            'country_values': [0]
        }
        return np.array([]), np.array([]), np.array([]), empty_metrics

    papers_dict = dict(zip(dates_papers, papers_counts))
    patents_dict = dict(zip(dates_patents, patents_counts))

    papers_aligned = [papers_dict.get(month, 0) for month in all_months]
    patents_aligned = [patents_dict.get(month, 0) for month in all_months]

    # ----- Расчёт дополнительных метрик (заглушки, пока нет реальных данных) -----
    if papers_total > 0 or patents_total > 0:
        papers_growth = round(np.random.uniform(5, 15), 1)      # пример
        patents_growth = round(np.random.uniform(8, 20), 1)     # пример
        time_lag = round(np.random.uniform(2.5, 4.5), 1)        # пример
        time_lag_change = f"+{round(np.random.uniform(0.1, 0.5), 1)}" if np.random.rand() > 0.5 else f"-{round(np.random.uniform(0.1, 0.5), 1)}"
        trend_score = np.random.randint(60, 95)
        if trend_score >= 80:
            trend_status = "Взрывной рост"
        elif trend_score >= 60:
            trend_status = "Стабильный рост"
        else:
            trend_status = "Созревание"
        ai_share = np.random.randint(15, 45)
        top_assignees = ['Компания А', 'Компания Б', 'Компания В', 'Компания Г', 'Компания Д']
        assignee_values = [np.random.randint(50, 200) for _ in range(5)]
        countries = ['США', 'Китай', 'Япония', 'Южная Корея', 'Германия']
        country_values = [45, 30, 15, 7, 3]
    else:
        papers_growth = patents_growth = 0
        time_lag = 0
        time_lag_change = '0'
        trend_score = 0
        trend_status = 'Нет данных'
        ai_share = 0
        top_assignees = ['Нет данных']
        assignee_values = [0]
        countries = ['Нет данных']
        country_values = [100]

    metrics = {
        'papers_total': papers_total,
        'patents_total': patents_total,
        'papers_cited_avg': cited_avg,
        'papers_growth': papers_growth,
        'patents_growth': patents_growth,
        'time_lag': time_lag,
        'time_lag_change': time_lag_change,
        'trend_score': trend_score,
        'trend_status': trend_status,
        'ai_share': ai_share,
        'top_assignees': top_assignees,
        'assignee_values': assignee_values,
        'countries': countries,
        'country_values': country_values
    }

    print(f"✅ Метрики: {metrics}")
    print(f"📊 Всего месяцев в рядах: {len(all_months)}")

    return np.array(all_months), np.array(papers_aligned), np.array(patents_aligned), metrics
