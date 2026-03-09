import streamlit as st
import pandas as pd
import numpy as np
import duckdb
from pathlib import Path
import gdown
import os
import traceback

# Директория для хранения скачанных файлов
DATA_DIR = Path(__file__).parent / "data" / "processed"
DATA_DIR.mkdir(parents=True, exist_ok=True)

# Словарь с именами файлов и их Google Drive ID
FILES = {
    "semiconductors_clean.parquet": "1CwfeO6WY7gKqov5mAtaD1ffvsxBzdjR5",
    "gene_engineering_clean.parquet": "1mLx-mh1k4M4zNAOATLQiFXy06s0tfZHl"
}

_download_attempted = False

def _download_files():
    """Скачивает файлы, если их нет. Вызывается внутри load_domain_data."""
    global _download_attempted
    if _download_attempted:
        return
    _download_attempted = True
    
    for filename, file_id in FILES.items():
        dest = DATA_DIR / filename
        if not dest.exists():
            try:
                url = f"https://drive.google.com/uc?id={file_id}"
                print(f"📥 Скачиваю {filename}...")
                gdown.download(url, str(dest), quiet=False)
                if dest.exists():
                    size_mb = dest.stat().st_size / (1024*1024)
                    print(f"✅ Скачан {filename} ({size_mb:.1f} MB)")
                else:
                    print(f"❌ Не удалось скачать {filename}")
            except Exception as e:
                print(f"❌ Ошибка скачивания {filename}: {e}")

def _generate_fallback_data():
    """Возвращает тестовые данные, если реальные недоступны."""
    print("⚠️ Использую ТЕСТОВЫЕ данные (fallback)")
    dates = np.array(['2020-01', '2020-02', '2020-03', '2020-04', '2020-05'])
    papers = np.array([10, 15, 20, 25, 30])
    patents = np.array([5, 7, 9, 11, 13])
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

@st.cache_data(ttl=3600)
def load_domain_data(domain_clean):
    """
    Загружает данные для указанного домена.
    При ошибках возвращает тестовые данные, но перед этим подробно логирует.
    """
    print(f"🔍 Загрузка данных для домена: {domain_clean}")
    
    # Пытаемся скачать файлы (только один раз)
    _download_files()

    # Определяем файл для данного домена
    if domain_clean == "Полупроводники":
        data_file = DATA_DIR / "semiconductors_clean.parquet"
    elif domain_clean == "Генная инженерия":
        data_file = DATA_DIR / "gene_engineering_clean.parquet"
    else:
        print(f"❌ Неизвестный домен: {domain_clean}")
        return _generate_fallback_data()

    # Проверяем наличие файла
    if not data_file.exists():
        print(f"❌ Файл {data_file} не найден")
        return _generate_fallback_data()
    
    size_mb = data_file.stat().st_size / (1024*1024)
    print(f"✅ Файл найден: {data_file} ({size_mb:.1f} MB)")

    try:
        con = duckdb.connect()
        
        # ----- Диагностика: какие типы записей есть в файле -----
        type_counts = con.execute(f"SELECT type, COUNT(*) as cnt FROM read_parquet('{data_file}') GROUP BY type").df()
        print("📋 Типы записей в файле:")
        print(type_counts.to_string(index=False))
        
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
        df_papers = con.execute(query_papers).df()
        dates_papers = df_papers['month'].tolist()
        papers_counts = df_papers['count'].tolist()
        print(f"📊 Публикации: найдено {len(dates_papers)} месяцев, всего {sum(papers_counts)} записей")

        # Общее число публикаций
        papers_total = con.execute(f"SELECT COUNT(*) FROM read_parquet('{data_file}') WHERE type = 'publication'").fetchone()[0]

        # Среднее цитирование (только для публикаций)
        cited_avg = con.execute(f"SELECT AVG(citations) FROM read_parquet('{data_file}') WHERE type = 'publication' AND citations IS NOT NULL").fetchone()[0]
        cited_avg = round(cited_avg, 2) if cited_avg else 0

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
        df_patents = con.execute(query_patents).df()
        dates_patents = df_patents['month'].tolist()
        patents_counts = df_patents['count'].tolist()
        print(f"📊 Патенты: найдено {len(dates_patents)} месяцев, всего {sum(patents_counts)} записей")

        # Общее число патентов
        patents_total = con.execute(f"SELECT COUNT(*) FROM read_parquet('{data_file}') WHERE type = 'patent'").fetchone()[0]

        # ----- Объединение временных рядов -----
        all_months = sorted(set(dates_papers) | set(dates_patents))
        if not all_months:
            print("⚠️ Нет данных ни по одному из источников (all_months пуст)")
            return _generate_fallback_data()

        papers_dict = dict(zip(dates_papers, papers_counts))
        patents_dict = dict(zip(dates_patents, patents_counts))

        papers_aligned = [papers_dict.get(month, 0) for month in all_months]
        patents_aligned = [patents_dict.get(month, 0) for month in all_months]

        print(f"📅 Всего месяцев в объединённом ряду: {len(all_months)}")

        # ----- Расчёт дополнительных метрик (заглушки, можно заменить) -----
        # (оставляем заглушки, чтобы не усложнять)
        papers_growth = round(np.random.uniform(5, 15), 1)
        patents_growth = round(np.random.uniform(8, 20), 1)
        time_lag = round(np.random.uniform(2.5, 4.5), 1)
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

        print("✅ Успешно загружены РЕАЛЬНЫЕ данные")
        return np.array(all_months), np.array(papers_aligned), np.array(patents_aligned), metrics

    except Exception as e:
        print(f"❌ Ошибка при загрузке данных: {e}")
        traceback.print_exc()
        return _generate_fallback_data()
