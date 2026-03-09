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
FILES = {
    "semiconductors_clean_full.parquet": "1CwfeO6WY7gKqov5mAtaD1ffvsxBzdjR5",
    "gene_engineering_clean_full.parquet": "1mLx-mh1k4M4zNAOATLQiFXy06s0tfZHl",
    "patents.parquet": "1xI60lbOCbY7BQ_Wq9tX-cs6Zzvme8B9L"
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

    # Определяем файл публикаций в зависимости от домена
    if domain_clean == "Полупроводники":
        papers_file = DATA_DIR / "semiconductors_clean_full.parquet"
    elif domain_clean == "Генная инженерия":
        papers_file = DATA_DIR / "gene_engineering_clean_full.parquet"
    else:
        return np.array([]), np.array([]), np.array([]), {}

    patents_file = DATA_DIR / "patents.parquet"

    # Проверяем наличие файлов
    if not papers_file.exists():
        st.error(f"❌ Файл публикаций не найден: {papers_file}")
        return np.array([]), np.array([]), np.array([]), {}
    if not patents_file.exists():
        st.warning("⚠️ Файл патентов отсутствует, статистика по патентам будет нулевой.")
        # Продолжаем без патентов

    con = duckdb.connect()

    # ----- Публикации: помесячная статистика -----
    # Предполагаем, что в данных есть колонка publication_date
    query_papers = f"""
        SELECT 
            strftime(publication_date, '%Y-%m') as month,
            COUNT(*) as count
        FROM read_parquet('{papers_file}')
        WHERE publication_date IS NOT NULL
        GROUP BY month
        ORDER BY month
    """
    try:
        df_papers = con.execute(query_papers).df()
        dates_papers = df_papers['month'].tolist()
        papers_counts = df_papers['count'].tolist()
    except Exception as e:
        print(f"Ошибка при обработке публикаций: {e}")
        dates_papers, papers_counts = [], []

    # Общее число публикаций
    try:
        papers_total = con.execute(f"SELECT COUNT(*) FROM read_parquet('{papers_file}')").fetchone()[0]
    except:
        papers_total = 0

    # Среднее цитирование (если есть колонка cited_by_count)
    try:
        cited_avg = con.execute(f"SELECT AVG(cited_by_count) FROM read_parquet('{papers_file}') WHERE cited_by_count IS NOT NULL").fetchone()[0]
        cited_avg = round(cited_avg, 2) if cited_avg else 0
    except:
        cited_avg = 0

    # ----- Патенты: помесячная статистика -----
    dates_patents, patents_counts = [], []
    patents_total = 0

    if patents_file.exists():
        query_patents = f"""
            SELECT 
                strftime(publication_date, '%Y-%m') as month,
                COUNT(*) as count
            FROM read_parquet('{patents_file}')
            WHERE publication_date IS NOT NULL
            GROUP BY month
            ORDER BY month
        """
        try:
            df_patents = con.execute(query_patents).df()
            dates_patents = df_patents['month'].tolist()
            patents_counts = df_patents['count'].tolist()
            patents_total = con.execute(f"SELECT COUNT(*) FROM read_parquet('{patents_file}')").fetchone()[0]
        except Exception as e:
            print(f"Ошибка при обработке патентов: {e}")

    # ----- Объединение временных рядов -----
    all_months = sorted(set(dates_papers) | set(dates_patents))
    if not all_months:
        # Нет данных ни по одному из источников
        return np.array([]), np.array([]), np.array([]), {}

    papers_dict = dict(zip(dates_papers, papers_counts))
    patents_dict = dict(zip(dates_patents, patents_counts))

    papers_aligned = [papers_dict.get(month, 0) for month in all_months]
    patents_aligned = [patents_dict.get(month, 0) for month in all_months]

    # ----- Метрики -----
    metrics = {
        'papers_total': papers_total,
        'patents_total': patents_total,
        'papers_cited_avg': cited_avg
    }

    print(f"✅ Метрики: {metrics}")
    print(f"📊 Всего месяцев в рядах: {len(all_months)}")

    return np.array(all_months), np.array(papers_aligned), np.array(patents_aligned), metrics
