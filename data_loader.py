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

    # --- Загрузка патентов ---
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
            
            # Загружаем данные
            con.execute(f"""
                CREATE OR REPLACE TEMP VIEW patents AS SELECT * FROM read_parquet('{patents_file}');
                CREATE OR REPLACE TEMP VIEW cpc AS SELECT * FROM read_parquet('{cpc_file}');
                CREATE OR REPLACE TEMP VIEW assignee_harmonized AS SELECT * FROM read_parquet('{assignee_file}');
            """)
            
            # ДИАГНОСТИКА - узнаем реальные имена колонок
            patents_cols = con.execute("DESCRIBE patents").df()['column_name'].tolist()
            assignee_cols = con.execute("DESCRIBE assignee_harmonized").df()['column_name'].tolist()
            cpc_cols = con.execute("DESCRIBE cpc").df()['column_name'].tolist()
            
            print("📊 Колонки в patents:", patents_cols)
            print("📊 Колонки в assignee_harmonized:", assignee_cols)
            print("📊 Колонки в cpc:", cpc_cols)
            
            # Определяем правильные имена колонок для связки
            patent_id_col = 'publication_number'  # По умолчанию
            if 'patent_id' in patents_cols:
                patent_id_col = 'patent_id'
            elif 'publication_number' in patents_cols:
                patent_id_col = 'publication_number'
            
            assignee_id_col = patent_id_col  # Обычно такое же имя

            # ========== ИСПРАВЛЕНО: загрузка патентов по месяцам ==========
            try:
                # Проверяем тип данных publication_date
                date_sample = con.execute("SELECT publication_date FROM patents WHERE publication_date IS NOT NULL LIMIT 1").fetchone()
                
                if date_sample and date_sample[0] is not None:
                    print(f"📅 Пример даты в patents: {date_sample[0]} (тип: {type(date_sample[0])})")
                    
                    # Пробуем разные форматы дат
                    if isinstance(date_sample[0], (int, np.integer)):
                        # Unix timestamp в секундах или миллисекундах
                        if date_sample[0] > 10000000000:  # > 10 млрд - скорее миллисекунды
                            query_patents_monthly = """
                                SELECT 
                                    strftime(CAST(to_timestamp(publication_date / 1000) AS DATE), '%Y-%m') as month,
                                    COUNT(*) as count
                                FROM patents
                                WHERE publication_date IS NOT NULL AND publication_date > 0
                                GROUP BY month
                                ORDER BY month
                            """
                        else:  # секунды
                            query_patents_monthly = """
                                SELECT 
                                    strftime(CAST(to_timestamp(publication_date) AS DATE), '%Y-%m') as month,
                                    COUNT(*) as count
                                FROM patents
                                WHERE publication_date IS NOT NULL AND publication_date > 0
                                GROUP BY month
                                ORDER BY month
                            """
                    else:
                        # Дата в строковом формате
                        query_patents_monthly = """
                            SELECT 
                                strftime(CAST(publication_date AS DATE), '%Y-%m') as month,
                                COUNT(*) as count
                            FROM patents
                            WHERE publication_date IS NOT NULL AND publication_date != ''
                            GROUP BY month
                            ORDER BY month
                        """
                    
                    df_patents_monthly = con.execute(query_patents_monthly).df()
                    
                    if len(df_patents_monthly) > 0:
                        patents_aligned_dict = dict(zip(df_patents_monthly['month'], df_patents_monthly['count']))
                        print(f"✅ Найдено {len(patents_aligned_dict)} месяцев с патентами")
                        print(f"📊 Примеры: {list(patents_aligned_dict.items())[:5]}")
                        
                        # Проверяем сумму патентов
                        total_patents_in_months = sum(patents_aligned_dict.values())
                        print(f"📊 Сумма патентов по месяцам: {total_patents_in_months}")
                    else:
                        print("⚠️ Нет данных по месяцам для патентов")
                        
                else:
                    print("⚠️ Нет примеров дат в patents")
                    
            except Exception as e:
                print(f"❌ Ошибка при группировке патентов по месяцам: {e}")
                traceback.print_exc()
                patents_aligned_dict = {}

            patents_total = con.execute("SELECT COUNT(*) FROM patents").fetchone()[0]
            print(f"📊 Всего патентов в таблице: {patents_total}")

            # Топ-5 заявителей
            try:
                # Проверяем, есть ли нужные колонки в assignee_harmonized
                if 'name' in assignee_cols:
                    name_col = 'name'
                elif 'assignee_name' in assignee_cols:
                    name_col = 'assignee_name'
                else:
                    name_col = assignee_cols[0] if assignee_cols else 'name'
                
                top_assignees_df = con.execute(f"""
                    SELECT 
                        ah.{name_col} as assignee_name,
                        COUNT(p.{patent_id_col}) as patent_count
                    FROM patents p
                    JOIN assignee_harmonized ah ON p.{patent_id_col} = ah.{assignee_id_col}
                    WHERE ah.{name_col} IS NOT NULL
                    GROUP BY ah.{name_col}
                    ORDER BY patent_count DESC
                    LIMIT 5
                """).df()
                if not top_assignees_df.empty:
                    top_assignees = top_assignees_df['assignee_name'].tolist()
                    assignee_values = top_assignees_df['patent_count'].tolist()
                    print("✅ Топ заявителей получен")
            except Exception as e:
                print(f"⚠️ Не удалось получить топ заявителей: {e}")

            # География (по publication_number)
            try:
                countries_df = con.execute(f"""
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
                    total = countries_df['cnt'].sum()
                    country_values = (countries_df['cnt'] / total * 100).round(1).tolist()
                    print("✅ География получена")
            except Exception as e:
                print(f"⚠️ Не удалось получить географию: {e}")

            # AI-интеграция
            try:
                # Определяем колонку с CPC кодом
                if 'cpc_class' in cpc_cols:
                    cpc_code_col = 'cpc_class'
                elif 'cpc_code' in cpc_cols:
                    cpc_code_col = 'cpc_code'
                elif 'code' in cpc_cols:
                    cpc_code_col = 'code'
                else:
                    cpc_code_col = cpc_cols[0] if cpc_cols else 'code'
                
                ai_result = con.execute(f"""
                    SELECT 
                        COUNT(DISTINCT p.{patent_id_col}) * 100.0 / NULLIF((SELECT COUNT(*) FROM patents), 0) as ai_percent
                    FROM patents p
                    JOIN cpc c ON p.{patent_id_col} = c.{patent_id_col}
                    WHERE c.{cpc_code_col} LIKE 'G06N%'
                """).fetchone()
                
                if ai_result and ai_result[0] is not None:
                    ai_share = round(ai_result[0], 1)
                else:
                    ai_share = 0
                print(f"✅ AI-доля: {ai_share}%")
            except Exception as e:
                print(f"⚠️ Не удалось рассчитать AI-долю: {e}")

            print(f"✅ Патенты: {patents_total} записей, AI доля: {ai_share}%")
        except Exception as e:
            print(f"❌ Критическая ошибка при обработке патентов: {e}")
            traceback.print_exc()
            patents_available = False

    # --- Объединение временных рядов ---
    all_months = sorted(set(all_months) | set(patents_aligned_dict.keys()))
    
    if not all_months:
        print("⚠️ Нет месяцев для построения временного ряда")
        return _generate_fallback_data(domain_clean, "Нет данных для построения временного ряда")

    papers_dict = dict(zip(all_months, papers_aligned)) if papers_available else {}
    papers_aligned_final = [papers_dict.get(month, 0) for month in all_months]
    patents_aligned_final = [patents_aligned_dict.get(month, 0) for month in all_months]

    # ========== ДИАГНОСТИКА ДАННЫХ ==========
    print("\n📊 ДИАГНОСТИКА ДАННЫХ:")
    print(f"Всего месяцев: {len(all_months)}")
    print(f"Первые 5 месяцев: {all_months[:5]}")
    print(f"Последние 5 месяцев: {all_months[-5:]}")
    print(f"Публикации (первые 5): {papers_aligned_final[:5]}")
    print(f"Патенты (первые 5): {patents_aligned_final[:5]}")
    print(f"Сумма публикаций: {sum(papers_aligned_final)}")
    print(f"Сумма патентов: {sum(patents_aligned_final)}")
    print(f"Макс публикаций: {max(papers_aligned_final) if papers_aligned_final else 0}")
    print(f"Макс патентов: {max(patents_aligned_final) if patents_aligned_final else 0}")

    # --- Расчёт метрик роста ---
    try:
        if len(papers_aligned_final) >= 24:
            recent_papers = sum(papers_aligned_final[-12:])
            prev_papers = sum(papers_aligned_final[-24:-12])
            papers_growth = round(((recent_papers - prev_papers) / prev_papers) * 100, 1) if prev_papers > 0 else 0
        else:
            papers_growth = 0

        if len(patents_aligned_final) >= 24:
            recent_patents = sum(patents_aligned_final[-12:])
            prev_patents = sum(patents_aligned_final[-24:-12])
            patents_growth = round(((recent_patents - prev_patents) / prev_patents) * 100, 1) if prev_patents > 0 else 0
        else:
            patents_growth = 0

        # --- Trend Score ---
        if len(papers_aligned_final) >= 12 and len(patents_aligned_final) >= 12:
            years = min(3, len(all_months)//12)
            papers_slopes = []
            for y in range(years):
                y_data = papers_aligned_final[-(y+1)*12:-(y)*12] if y>0 else papers_aligned_final[-12:]
                if len(y_data) > 1:
                    x = np.arange(len(y_data))
                    slope = np.polyfit(x, y_data, 1)[0]
                    papers_slopes.append(max(0, slope))
            avg_papers_slope = np.mean(papers_slopes) if papers_slopes else 0

            patents_slopes = []
            for y in range(years):
                y_data = patents_aligned_final[-(y+1)*12:-(y)*12] if y>0 else patents_aligned_final[-12:]
                if len(y_data) > 1:
                    x = np.arange(len(y_data))
                    slope = np.polyfit(x, y_data, 1)[0]
                    patents_slopes.append(max(0, slope))
            avg_patents_slope = np.mean(patents_slopes) if patents_slopes else 0

            max_slope = max(avg_papers_slope, avg_patents_slope, 1)
            trend_score = int(min(100, (avg_papers_slope + avg_patents_slope) / max_slope * 50 + 50))
        else:
            trend_score = 50

        if trend_score >= 80:
            trend_status = "Взрывной рост"
        elif trend_score >= 60:
            trend_status = "Стабильный рост"
        else:
            trend_status = "Созревание"

        # --- Time Lag ---
        try:
            if len(papers_aligned_final) > 0 and len(patents_aligned_final) > 0:
                years_list = [int(m[:4]) for m in all_months]
                weighted_year_papers = np.average(years_list, weights=papers_aligned_final)
                weighted_year_patents = np.average(years_list, weights=patents_aligned_final)
                time_lag = round(abs(weighted_year_patents - weighted_year_papers), 1)
            else:
                time_lag = 3.0
        except:
            time_lag = 3.0

        # Изменение time lag
        try:
            if len(all_months) >= 48:
                recent_mask = [m >= all_months[-24] for m in all_months]
                prev_mask = [m < all_months[-24] and m >= all_months[-48] for m in all_months]
                if any(recent_mask) and any(prev_mask):
                    recent_papers_weights = [p for p, m in zip(papers_aligned_final, recent_mask) if m]
                    recent_patents_weights = [p for p, m in zip(patents_aligned_final, recent_mask) if m]
                    recent_years = [int(m[:4]) for m, m_flag in zip(all_months, recent_mask) if m_flag]

                    prev_papers_weights = [p for p, m in zip(papers_aligned_final, prev_mask) if m]
                    prev_patents_weights = [p for p, m in zip(patents_aligned_final, prev_mask) if m]
                    prev_years = [int(m[:4]) for m, m_flag in zip(all_months, prev_mask) if m_flag]

                    if recent_years and prev_years:
                        recent_lag = abs(np.average(recent_years, weights=recent_patents_weights) - np.average(recent_years, weights=recent_papers_weights))
                        prev_lag = abs(np.average(prev_years, weights=prev_patents_weights) - np.average(prev_years, weights=prev_papers_weights))
                        lag_change = round(recent_lag - prev_lag, 1)
                        time_lag_change = f"+{lag_change}" if lag_change > 0 else str(lag_change)
                    else:
                        time_lag_change = "0"
                else:
                    time_lag_change = "0"
            else:
                time_lag_change = "0"
        except:
            time_lag_change = "0"
            
    except Exception as e:
        print(f"⚠️ Ошибка при расчёте метрик: {e}")
        papers_growth = 0
        patents_growth = 0
        trend_score = 50
        trend_status = "Стабильный рост"
        time_lag = 3.0
        time_lag_change = "0"

    # Сбор метрик
    metrics = {
        'papers_total': papers_total,
        'patents_total': patents_total,
        'papers_cited_avg': papers_cited_avg,
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

    print("✅ Данные успешно загружены и обработаны")
    return np.array(all_months), np.array(papers_aligned_final), np.array(patents_aligned_final), metrics
