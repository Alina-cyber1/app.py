
import streamlit as st
import pandas as pd
import numpy as np
import duckdb
from pathlib import Path
import gdown
import traceback

# Директория для хранения скачанных файлов
DATA_DIR = Path(__file__).parent / "data" / "processed"
DATA_DIR.mkdir(parents=True, exist_ok=True)

# Словарь с именами файлов и их Google Drive ID
FILES = {
    "semiconductors_clean_full.parquet": "1CwfeO6WY7gKqov5mAtaD1ffvsxBzdjR5",
    "gene_engineering_clean_full.parquet": "1mLx-mh1k4M4zNAOATLQiFXy06s0tfZHl",
    "patents.parquet": "1xI60lbOCbY7BQ_Wq9tX-cs6Zzvme8B9L"
}

_download_attempted = False

def _download_files():
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
    Использует semiconductors_clean_full.parquet или gene_engineering_clean_full.parquet для публикаций,
    и patents.parquet для патентов.
    """
    print(f"🔍 Загрузка данных для домена: {domain_clean}")
    
    _download_files()

    # Определяем файл публикаций
    if domain_clean == "Полупроводники":
        papers_file = DATA_DIR / "semiconductors_clean_full.parquet"
    elif domain_clean == "Генная инженерия":
        papers_file = DATA_DIR / "gene_engineering_clean_full.parquet"
    else:
        print(f"❌ Неизвестный домен: {domain_clean}")
        return _generate_fallback_data()

    patents_file = DATA_DIR / "patents.parquet"

    papers_available = papers_file.exists()
    patents_available = patents_file.exists()
    
    if not papers_available:
        print(f"❌ Файл публикаций {papers_file} не найден")
    if not patents_available:
        print(f"⚠️ Файл патентов {patents_file} не найден (патенты будут нулевыми)")

    if not papers_available and not patents_available:
        return _generate_fallback_data()

    con = duckdb.connect()
    dates_papers, papers_counts = [], []
    dates_patents, patents_counts = [], []
    papers_total, patents_total = 0, 0
    cited_avg = 0

    # ----- Публикации (если файл есть) -----
    if papers_available:
        try:
            size_mb = papers_file.stat().st_size / (1024*1024)
            print(f"✅ Файл публикаций: {papers_file} ({size_mb:.1f} MB)")

            query_papers = f"""
                SELECT 
                    strftime(CAST(publication_date AS DATE), '%Y-%m') as month,
                    COUNT(*) as count
                FROM read_parquet('{papers_file}')
                WHERE publication_date IS NOT NULL
                GROUP BY month
                ORDER BY month
            """
            df_papers = con.execute(query_papers).df()
            dates_papers = df_papers['month'].tolist()
            papers_counts = df_papers['count'].tolist()
            print(f"📊 Публикации: найдено {len(dates_papers)} месяцев, всего {sum(papers_counts)} записей")

            papers_total = con.execute(f"SELECT COUNT(*) FROM read_parquet('{papers_file}')").fetchone()[0]

            try:
                cited_avg = con.execute(f"SELECT AVG(citations) FROM read_parquet('{papers_file}') WHERE citations IS NOT NULL").fetchone()[0]
                cited_avg = round(cited_avg, 2) if cited_avg else 0
            except:
                cited_avg = 0

        except Exception as e:
            print(f"❌ Ошибка при обработке публикаций: {e}")
            traceback.print_exc()
            # В случае ошибки обнуляем, но продолжаем

    # ----- Патенты (если файл есть) -----
    if patents_available:
        try:
            size_mb = patents_file.stat().st_size / (1024*1024)
            print(f"✅ Файл патентов: {patents_file} ({size_mb:.1f} MB)")

            # Проверим тип колонки publication_date
            sample = con.execute(f"SELECT publication_date FROM read_parquet('{patents_file}') LIMIT 1").df()
            if sample.empty:
                print("⚠️ Файл патентов пуст")
            else:
                # ========== ДИАГНОСТИКА ==========
                sample_dates = con.execute(f"SELECT publication_date FROM read_parquet('{patents_file}') LIMIT 10").df()
                print("📅 Примеры publication_date из патентов:")
                print(sample_dates.to_string(index=False))
                
                min_max = con.execute(f"SELECT MIN(publication_date), MAX(publication_date) FROM read_parquet('{patents_file}')").df()
                print("📅 Мин и макс даты в патентах:")
                print(min_max.to_string(index=False))
                # ===============================

                val = sample.iloc[0, 0]
                # Определяем тип и строим соответствующий запрос
                if isinstance(val, (int, np.integer)):
                    # Предполагаем, что это Unix timestamp в миллисекундах -> делим на 1000
                    print("ℹ️ publication_date в патентах - целое число, интерпретируем как миллисекунды (делим на 1000)")
                    query_patents = f"""
                        SELECT 
                            strftime(CAST(to_timestamp(publication_date / 1000) AS DATE), '%Y-%m') as month,
                            COUNT(*) as count
                        FROM read_parquet('{patents_file}')
                        WHERE publication_date IS NOT NULL
                        GROUP BY month
                        ORDER BY month
                    """
                else:
                    # Если строка или дата, используем CAST
                    print("ℹ️ publication_date в патентах - не целое, используем CAST AS DATE")
                    query_patents = f"""
                        SELECT 
                            strftime(CAST(publication_date AS DATE), '%Y-%m') as month,
                            COUNT(*) as count
                        FROM read_parquet('{patents_file}')
                        WHERE publication_date IS NOT NULL
                        GROUP BY month
                        ORDER BY month
                    """
                df_patents = con.execute(query_patents).df()
                dates_patents = df_patents['month'].tolist()
                patents_counts = df_patents['count'].tolist()
                print(f"📊 Патенты: найдено {len(dates_patents)} месяцев, всего {sum(patents_counts)} записей")
                patents_total = con.execute(f"SELECT COUNT(*) FROM read_parquet('{patents_file}')").fetchone()[0]
        except Exception as e:
            print(f"❌ Ошибка при обработке патентов: {e}")
            traceback.print_exc()
            # Оставляем патенты нулевыми

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

    # ----- Заглушки для дополнительных метрик -----
    if papers_total > 0 or patents_total > 0:
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
    else:
        return _generate_fallback_data()

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

    print("✅ Данные успешно загружены")
    return np.array(all_months), np.array(papers_aligned), np.array(patents_aligned), metrics
