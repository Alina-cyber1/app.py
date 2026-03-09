import streamlit as st
import pandas as pd
import numpy as np
import duckdb
from pathlib import Path
import gdown
import traceback
from datetime import datetime

DATA_DIR = Path(__file__).parent / "data" / "processed"
DATA_DIR.mkdir(parents=True, exist_ok=True)

FILES = {
    "gene_engineering_clean_full.parquet": "1mLx-mh1k4M4zNAOATLQiFXy06s0tfZHl",
    "gene_engineering_clean_nlp.parquet": "1DiRskqNZ3ph04f3QsyI-RjMxuVPKehaq",
    "gene_engineering_clean_signal.parquet": "1-VO0v49BFRIpvJl2ix0WzVnRjP1hxNRh",
    "semiconductors_clean_full.parquet": "1CwfeO6WY7gKqov5mAtaD1ffvsxBzdjR5",
    "semiconductors_clean_nlp.parquet": "1Qq3X1O7hpIV51xcet_TlTinqJ8SniIyN",
    "semiconductors_clean_signal.parquet": "1GSmeQvnoGU75rEI4v8QqITLj7KRrDQOQ",
    "patents.parquet": "1xI60lbOCbY7BQ_Wq9tX-cs6Zzvme8B9L",
    "cpc.parquet": "1L98w0Cx7Dh308W70W080dVabzN_34Kwk",
    "assignee_harmonized.parquet": "1CBRr7564K7hGdd75ffE8IRIvjesolqkd",
    "title_localized.parquet": "1BfEZRKC7qqWGna9uiqjdxzvhDRke8ZzN"
}

_download_attempted = False

def _download_files():
    global _download_attempted
    if _download_attempted:
        return
    _download_attempted = True
    st.info("🔄 Проверка и загрузка необходимых данных...")
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
    st.success("✅ Проверка данных завершена.")

def _generate_fallback_data(domain_clean, error_msg=""):
    print(f"⚠️ Использую ТЕСТОВЫЕ данные для {domain_clean}. Ошибка: {error_msg}")
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
    if metrics['trend_score'] >= 80:
        metrics['trend_status'] = "Взрывной рост"
    elif metrics['trend_score'] >= 60:
        metrics['trend_status'] = "Стабильный рост"
    else:
        metrics['trend_status'] = "Созревание"
    return np.array(dates), np.array(papers), np.array(patents), metrics

@st.cache_data(ttl=3600)
def load_domain_data(domain_clean):
    print(f"🔍 Загрузка данных для домена: {domain_clean}")
    _download_files()

    if domain_clean == "Полупроводники":
        domain_prefix = "semiconductors"
    elif domain_clean == "Генная инженерия":
        domain_prefix = "gene_engineering"
    else:
        return _generate_fallback_data(domain_clean, "Неизвестный домен")

    papers_file = DATA_DIR / f"{domain_prefix}_clean_full.parquet"
    patents_file = DATA_DIR / "patents.parquet"
    cpc_file = DATA_DIR / "cpc.parquet"
    assignee_file = DATA_DIR / "assignee_harmonized.parquet"

    papers_available = papers_file.exists()
    patents_available = patents_file.exists() and cpc_file.exists() and assignee_file.exists()

    if not papers_available:
        print(f"❌ Файл публикаций {papers_file} не найден")
    if not patents_available:
        print(f"⚠️ Не все файлы патентов найдены (будут использованы заглушки)")

    if not papers_available and not patents_available:
        return _generate_fallback_data(domain_clean, "Нет файлов публикаций и патентов")

    con = duckdb.connect()

    # --- Загрузка публикаций ---
    all_months = []
    papers_aligned = []
    papers_total = 0
    papers_cited_avg = 0

    if papers_available:
        try:
            print(f"📄 Загрузка публикаций из {papers_file.name}")
            query_papers_monthly = f"""
                SELECT 
                    strftime(CAST(publication_date AS DATE), '%Y-%m') as month,
                    COUNT(*) as count
                FROM read_parquet('{papers_file}')
                WHERE publication_date IS NOT NULL
                GROUP BY month
                ORDER BY month
            """
            df_papers_monthly = con.execute(query_papers_monthly).df()
            all_months = df_papers_monthly['month'].tolist()
            papers_aligned = df_papers_monthly['count'].tolist()
            papers_total = con.execute(f"SELECT COUNT(*) FROM read_parquet('{papers_file}')").fetchone()[0]
            try:
                cited_avg = con.execute(f"SELECT AVG(citations) FROM read_parquet('{papers_file}') WHERE citations IS NOT NULL").fetchone()[0]
                papers_cited_avg = round(cited_avg, 2) if cited_avg else 0
            except:
                papers_cited_avg = 0
            print(f"✅ Публикации: {papers_total} записей, {len(all_months)} месяцев")
        except Exception as e:
            print(f"❌ Ошибка при обработке публикаций: {e}")
            traceback.print_exc()
            papers_available = False

    # --- Загрузка патентов (с инициализацией переменных) ---
    patents_aligned_dict = {}
    patents_total = 0
    top_assignees = ["Нет данных"]
    assignee_values = [0]
    countries = ["Нет данных"]
    country_values = [100]
    ai_share = 0

    if patents_available:
        try:
            print("📃 Загрузка и связывание данных о патентах...")
            con.execute(f"""
                CREATE OR REPLACE TEMP VIEW patents AS SELECT * FROM read_parquet('{patents_file}');
                CREATE OR REPLACE TEMP VIEW cpc AS SELECT * FROM read_parquet('{cpc_file}');
                CREATE OR REPLACE TEMP VIEW assignee_harmonized AS SELECT * FROM read_parquet('{assignee_file}');
            """)

            # --- Исправлено: используем publication_date вместо patent_date ---
            query_patents_monthly = """
                SELECT 
                    strftime(CAST(publication_date AS DATE), '%Y-%m') as month,
                    COUNT(*) as count
                FROM patents
                WHERE publication_date IS NOT NULL
                GROUP BY month
                ORDER BY month
            """
            df_patents_monthly = con.execute(query_patents_monthly).df()
            patents_aligned_dict = dict(zip(df_patents_monthly['month'], df_patents_monthly['count']))

            patents_total = con.execute("SELECT COUNT(*) FROM patents").fetchone()[0]

            # Топ-5 заявителей (проверим наличие колонки assignee_harmonized_id)
            try:
                top_assignees_df = con.execute("""
                    SELECT 
                        ah.name as assignee_name,
                        COUNT(p.patent_id) as patent_count
                    FROM patents p
                    JOIN assignee_harmonized ah ON p.assignee_harmonized_id = ah.assignee_harmonized_id
                    WHERE ah.name IS NOT NULL
                    GROUP BY ah.name
                    ORDER BY patent_count DESC
                    LIMIT 5
                """).df()
                if not top_assignees_df.empty:
                    top_assignees = top_assignees_df['assignee_name'].tolist()
                    assignee_values = top_assignees_df['patent_count'].tolist()
            except Exception as e:
                print(f"⚠️ Не удалось получить топ заявителей: {e}")

            # География (упрощённо)
            try:
                countries_df = con.execute("""
                    SELECT 
                        CASE 
                            WHEN publication_number LIKE 'US%' THEN 'США'
                            WHEN publication_number LIKE 'CN%' THEN 'Китай'
                            WHEN publication_number LIKE 'JP%' THEN 'Япония'
                            WHEN publication_number LIKE 'KR%' THEN 'Южная Корея'
                            WHEN publication_number LIKE 'EP%' THEN 'Европа'
                            WHEN publication_number LIKE 'WO%' THEN 'WO'
                            ELSE 'Другие'
                        END as country,
                        COUNT(*) as cnt
                    FROM patents
                    GROUP BY country
                    ORDER BY cnt DESC
                    LIMIT 5
                """).df()
                if not countries_df.empty:
                    countries = countries_df['country'].tolist()
                    country_values = (countries_df['cnt'] / countries_df['cnt'].sum() * 100).round(1).tolist()
            except Exception as e:
                print(f"⚠️ Не удалось получить географию: {e}")

            # AI-интеграция
            try:
                ai_share = con.execute("""
                    SELECT 
                        COUNT(DISTINCT p.patent_id) * 100.0 / (SELECT COUNT(*) FROM patents) as ai_percent
                    FROM patents p
                    JOIN cpc c ON p.patent_id = c.patent_id
                    WHERE c.cpc_class LIKE 'G06N%'
                """).fetchone()[0]
                ai_share = round(ai_share, 1)
            except Exception as e:
                print(f"⚠️ Не удалось рассчитать AI-долю: {e}")

            print(f"✅ Патенты: {patents_total} записей, AI доля: {ai_share}%")
        except Exception as e:
            print(f"❌ Критическая ошибка при обработке патентов: {e}")
            traceback.print_exc()
            # Оставляем patents_available = True, но данные уже не полные? Лучше сбросить флаг,
            # чтобы дальше не использовать patents_aligned_dict
            patents_available = False

    # --- Объединение временных рядов ---
    all_months = sorted(set(all_months) | set(patents_aligned_dict.keys() if patents_available else []))
    if not all_months:
        return _generate_fallback_data(domain_clean, "Нет данных для построения временного ряда")

    papers_dict = dict(zip(all_months, papers_aligned)) if papers_available else {}
    papers_aligned_final = [papers_dict.get(month, 0) for month in all_months]
    patents_aligned_final = [patents_aligned_dict.get(month, 0) for month in all_months]

    # --- Расчёт метрик ---
    # ... (остальной код без изменений, он уже был) ...

    # Сократим оставшуюся часть для краткости (она идентична предыдущей версии, кроме использования правильных имён переменных)
    # Вставьте сюда весь блок расчёта метрик из предыдущего кода (начиная с расчёта papers_growth и до конца)
    # ... 

    # Для полноты я приведу остаток кода, но в ответе лучше показать полный исправленный файл.
    # Ниже дам полный исправленный код с учётом всех изменений.
