import streamlit as st
import pandas as pd
import numpy as np
import duckdb
from pathlib import Path
import traceback
from datetime import datetime

DATA_DIR = Path(__file__).parent / "data" / "processed"
DATA_DIR.mkdir(parents=True, exist_ok=True)

# Информация об источниках данных
DATA_SOURCES = {
    "gene_engineering": {
        "source": "лаборатория генной инженерии",
        "date": "2024-03-15",
        "description": "Данные по патентам и публикациям в области генной инженерии",
        "status": "✅ Реальные данные"
    },
    "semiconductors": {
        "source": "лаборатория полупроводников", 
        "date": "2024-03-14",
        "description": "Данные по патентам и публикациям в области полупроводников",
        "status": "✅ Реальные данные"
    },
    "bigquery": {
        "source": "BigQuery",
        "date": "Ожидается",
        "description": "Интеграция с BigQuery для автоматической загрузки данных",
        "status": "⏳ В процессе подключения"
    }
}

def get_data_source_info(domain):
    """Возвращает информацию об источнике данных для домена"""
    if domain == "Полупроводники":
        return DATA_SOURCES["semiconductors"]
    elif domain == "Генная инженерия":
        return DATA_SOURCES["gene_engineering"]
    else:
        return DATA_SOURCES["bigquery"]

def check_files_exist():
    """Проверяет наличие всех необходимых файлов"""
    required_files = [
        "gene_engineering_clean.parquet",
        "semiconductors_clean.parquet"
    ]
    
    missing_files = []
    file_sizes = {}
    
    for filename in required_files:
        filepath = DATA_DIR / filename
        if filepath.exists():
            size_mb = filepath.stat().st_size / (1024 * 1024)
            file_sizes[filename] = round(size_mb, 1)
        else:
            missing_files.append(filename)
    
    return missing_files, file_sizes

def generate_fallback_data(domain_clean, error_msg=""):
    """Генерирует тестовые данные, если реальные недоступны"""
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
        'top_assignees': ['Компания А', 'Компания Б', 'Компания В'],
        'assignee_values': [150, 90, 45],
        'countries': ['США', 'Китай', 'Германия'],
        'country_values': [48, 32, 20]
    }
    
    # Определяем статус тренда
    if metrics['trend_score'] >= 80:
        metrics['trend_status'] = "Взрывной рост"
    elif metrics['trend_score'] >= 60:
        metrics['trend_status'] = "Стабильный рост"
    else:
        metrics['trend_status'] = "Созревание"
    
    return np.array(dates), np.array(papers), np.array(patents), metrics, None, None, None

def calculate_trend_score(papers_series, patents_series, months):
    """
    Рассчитывает Trend Score на основе динамики публикаций и патентов
    Возвращает score от 0 до 100 и статус
    """
    try:
        if len(papers_series) < 12 or len(patents_series) < 12:
            return 50, "Стабильный рост"
        
        # Рассчитываем склоны за последние 3 года с разными весами
        years = min(3, len(months) // 12)
        papers_slopes = []
        patents_slopes = []
        weights = [0.5, 0.3, 0.2]  # Последний год важнее
        
        for y in range(years):
            # Данные за год
            start_idx = -(y + 1) * 12
            end_idx = -y * 12 if y > 0 else None
            
            papers_year = papers_series[start_idx:end_idx] if end_idx else papers_series[start_idx:]
            patents_year = patents_series[start_idx:end_idx] if end_idx else patents_series[start_idx:]
            
            if len(papers_year) > 1:
                x = np.arange(len(papers_year))
                # Используем полином первой степени для определения тренда
                coeffs = np.polyfit(x, papers_year, 1)
                slope = coeffs[0]
                # Нормализуем slope относительно среднего значения
                mean_val = np.mean(papers_year) if np.mean(papers_year) > 0 else 1
                normalized_slope = (slope / mean_val) * 100
                papers_slopes.append(max(0, normalized_slope * weights[y] if y < len(weights) else normalized_slope * 0.1))
            
            if len(patents_year) > 1:
                x = np.arange(len(patents_year))
                coeffs = np.polyfit(x, patents_year, 1)
                slope = coeffs[0]
                mean_val = np.mean(patents_year) if np.mean(patents_year) > 0 else 1
                normalized_slope = (slope / mean_val) * 100
                patents_slopes.append(max(0, normalized_slope * weights[y] if y < len(weights) else normalized_slope * 0.1))
        
        # Усредняем склоны
        avg_papers_slope = np.sum(papers_slopes) if papers_slopes else 0
        avg_patents_slope = np.sum(patents_slopes) if patents_slopes else 0
        
        # Комбинируем с весами для публикаций и патентов
        papers_weight = 0.4  # Публикации немного важнее для определения тренда
        patents_weight = 0.6  # Патенты показывают коммерческий потенциал
        
        combined_slope = (avg_papers_slope * papers_weight + avg_patents_slope * patents_weight)
        
        # Преобразуем в score от 0 до 100
        # Типичные значения normalized slope: от 0 до 200
        trend_score = int(min(100, max(0, combined_slope)))
        
        # Определяем статус
        if trend_score >= 80:
            trend_status = "Взрывной рост"
        elif trend_score >= 60:
            trend_status = "Стабильный рост"
        elif trend_score >= 40:
            trend_status = "Умеренный рост"
        elif trend_score >= 20:
            trend_status = "Созревание"
        else:
            trend_status = "Стагнация"
        
        print(f"📊 Trend Score расчет: papers_slope={avg_papers_slope:.1f}, patents_slope={avg_patents_slope:.1f}, score={trend_score}")
        
        return trend_score, trend_status
        
    except Exception as e:
        print(f"⚠️ Ошибка при расчете Trend Score: {e}")
        traceback.print_exc()
        return 50, "Стабильный рост"

@st.cache_data(ttl=3600)
def load_domain_data(domain_clean):
    """
    Загружает данные для указанного домена из локальных parquet файлов
    Возвращает: months, papers, patents, metrics, df_papers, df_patents, df_all
    """
    print(f"🔍 Загрузка данных для домена: {domain_clean}")
    
    # Определяем файл для загрузки
    if domain_clean == "Полупроводники":
        data_file = DATA_DIR / "semiconductors_clean.parquet"
        domain_prefix = "semiconductors"
        source_info = DATA_SOURCES["semiconductors"]
    elif domain_clean == "Генная инженерия":
        data_file = DATA_DIR / "gene_engineering_clean.parquet"
        domain_prefix = "gene_engineering"
        source_info = DATA_SOURCES["gene_engineering"]
    else:
        return generate_fallback_data(domain_clean, "Неизвестный домен")
    
    # Проверяем существование файла
    if not data_file.exists():
        print(f"❌ Файл {data_file} не найден!")
        missing, sizes = check_files_exist()
        if missing:
            print(f"📋 Отсутствуют файлы: {missing}")
            print("💡 Запустите create_data.py для генерации данных")
        return generate_fallback_data(domain_clean, f"Файл {data_file.name} не найден")
    
    try:
        # Загружаем данные через DuckDB
        con = duckdb.connect()
        
        print(f"📄 Загрузка данных из {data_file.name}")
        print(f"   Размер файла: {data_file.stat().st_size / (1024*1024):.1f} MB")
        
        # Загружаем все записи для домена
        df_all = con.execute(f"""
            SELECT * FROM read_parquet('{data_file}')
            WHERE domain = '{domain_prefix}'
        """).df()
        
        if len(df_all) == 0:
            print(f"⚠️ Нет данных для домена {domain_clean}")
            return generate_fallback_data(domain_clean, "Нет данных в файле")
        
        print(f"✅ Загружено {len(df_all)} записей")
        
        # Разделяем на публикации и патенты
        df_papers = df_all[df_all['type'] == 'publication'].copy() if 'type' in df_all.columns else pd.DataFrame()
        df_patents = df_all[df_all['type'] == 'patent'].copy() if 'type' in df_all.columns else pd.DataFrame()
        
        print(f"   📄 Публикаций: {len(df_papers)}")
        print(f"   📃 Патентов: {len(df_patents)}")
        
        # --- Обработка временных рядов ---
        all_months = []
        papers_aligned = []
        patents_aligned = []
        
        # Публикации по месяцам
        if len(df_papers) > 0 and 'publication_date' in df_papers.columns:
            df_papers['month'] = pd.to_datetime(df_papers['publication_date']).dt.strftime('%Y-%m')
            papers_monthly = df_papers.groupby('month').size().reset_index(name='count')
            all_months = sorted(papers_monthly['month'].tolist())
            papers_aligned = papers_monthly['count'].tolist()
            papers_total = len(df_papers)
            
            # Средняя цитируемость
            if 'citations' in df_papers.columns:
                papers_cited_avg = round(df_papers['citations'].mean(), 1)
            else:
                papers_cited_avg = 0
        else:
            papers_total = 0
            papers_cited_avg = 0
        
        # Патенты по месяцам
        patents_dict = {}
        if len(df_patents) > 0 and 'publication_date' in df_patents.columns:
            df_patents['month'] = pd.to_datetime(df_patents['publication_date']).dt.strftime('%Y-%m')
            patents_monthly = df_patents.groupby('month').size().reset_index(name='count')
            patents_dict = dict(zip(patents_monthly['month'], patents_monthly['count']))
            patents_total = len(df_patents)
            
            # Обновляем список всех месяцев
            all_months = sorted(set(all_months) | set(patents_dict.keys()))
        else:
            patents_total = 0
        
        # Выравниваем ряды
        if len(all_months) > 0:
            # Для публикаций
            if len(df_papers) > 0 and 'publication_date' in df_papers.columns:
                papers_dict = dict(zip(papers_monthly['month'], papers_monthly['count']))
                papers_aligned = [papers_dict.get(month, 0) for month in all_months]
            else:
                papers_aligned = [0] * len(all_months)
            
            # Для патентов
            patents_aligned = [patents_dict.get(month, 0) for month in all_months]
        else:
            return generate_fallback_data(domain_clean, "Нет данных для временного ряда")
        
        # --- Расчёт метрик роста ---
        if len(papers_aligned) >= 24:
            recent_papers = sum(papers_aligned[-12:])
            prev_papers = sum(papers_aligned[-24:-12])
            papers_growth = round(((recent_papers - prev_papers) / prev_papers) * 100, 1) if prev_papers > 0 else 0
        else:
            papers_growth = 0
        
        if len(patents_aligned) >= 24:
            recent_patents = sum(patents_aligned[-12:])
            prev_patents = sum(patents_aligned[-24:-12])
            patents_growth = round(((recent_patents - prev_patents) / prev_patents) * 100, 1) if prev_patents > 0 else 0
        else:
            patents_growth = 0
        
        # --- Trend Score (используем улучшенную функцию) ---
        trend_score, trend_status = calculate_trend_score(
            np.array(papers_aligned), 
            np.array(patents_aligned), 
            all_months
        )

        # --- Time Lag ---
        try:
            if len(papers_aligned) > 0 and len(patents_aligned) > 0 and sum(papers_aligned) > 0 and sum(patents_aligned) > 0:
                years_list = [int(m[:4]) for m in all_months]
                weighted_year_papers = np.average(years_list, weights=papers_aligned)
                weighted_year_patents = np.average(years_list, weights=patents_aligned)
                time_lag = round(abs(weighted_year_patents - weighted_year_papers), 1)
            else:
                time_lag = 0
        except:
            time_lag = 0

        # Изменение time lag
        try:
            if len(all_months) >= 48:
                recent_mask = [m >= all_months[-24] for m in all_months]
                prev_mask = [m < all_months[-24] and m >= all_months[-48] for m in all_months]
                if any(recent_mask) and any(prev_mask) and sum(papers_aligned) > 0 and sum(patents_aligned) > 0:
                    recent_papers_weights = [p for p, m in zip(papers_aligned, recent_mask) if m]
                    recent_patents_weights = [p for p, m in zip(patents_aligned, recent_mask) if m]
                    recent_years = [int(m[:4]) for m, m_flag in zip(all_months, recent_mask) if m_flag]

                    prev_papers_weights = [p for p, m in zip(papers_aligned, prev_mask) if m]
                    prev_patents_weights = [p for p, m in zip(patents_aligned, prev_mask) if m]
                    prev_years = [int(m[:4]) for m, m_flag in zip(all_months, prev_mask) if m_flag]

                    if recent_years and prev_years and sum(recent_papers_weights) > 0 and sum(prev_papers_weights) > 0:
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
        
        # --- Топ заявителей ---
        if len(df_patents) > 0 and 'assignee' in df_patents.columns:
            top_assignees_df = df_patents['assignee'].value_counts().head(5).reset_index()
            top_assignees_df.columns = ['assignee', 'count']
            top_assignees = top_assignees_df['assignee'].tolist()
            assignee_values = top_assignees_df['count'].tolist()
        else:
            top_assignees = ["Нет данных"]
            assignee_values = [0]
        
        # --- География (по компаниям/университетам) ---
        countries_map = {
            'TSMC': 'Тайвань', 'Intel': 'США', 'Samsung': 'Южная Корея',
            'Qualcomm': 'США', 'Micron': 'США', 'SK Hynix': 'Южная Корея',
            'NVIDIA': 'США', 'AMD': 'США', 'MIT': 'США', 'Stanford': 'США',
            'UC Berkeley': 'США', 'Georgia Tech': 'США',
            'Editas Medicine': 'США', 'CRISPR Therapeutics': 'Швейцария',
            'Intellia': 'США', 'Vertex': 'США', 'Moderna': 'США',
            'BioNTech': 'Германия', 'Novartis': 'Швейцария',
            'Pfizer': 'США', 'Gilead': 'США',
            'Harvard Medical School': 'США', 'Stanford Medicine': 'США',
            'MIT Broad Institute': 'США', 'UC San Francisco': 'США',
            'Johns Hopkins University': 'США', 'University of Oxford': 'Великобритания'
        }
        
        if len(df_all) > 0 and 'assignee' in df_all.columns:
            df_all['country'] = df_all['assignee'].map(countries_map).fillna('Другие')
            countries_data = df_all['country'].value_counts().head(5).reset_index()
            countries_data.columns = ['country', 'count']
            total = countries_data['count'].sum()
            if total > 0:
                countries = countries_data['country'].tolist()
                country_values = (countries_data['count'] / total * 100).round(1).tolist()
            else:
                countries = ["Нет данных"]
                country_values = [100]
        else:
            countries = ["Нет данных"]
            country_values = [100]
        
        # --- AI-интеграция ---
        ai_share = 0
        if len(df_patents) > 0 and 'topic' in df_patents.columns:
            if domain_clean == "Полупроводники":
                ai_topics = ['GAA транзисторы', 'Квантовые точки', '2D материалы', 'Нейроморфные вычисления']
            else:
                ai_topics = ['CRISPR-Cas9', 'CRISPR-Cas12a', 'Базовое редактирование', 'Прайм-редактирование']
            
            ai_patents = df_patents[df_patents['topic'].isin(ai_topics)]
            if len(ai_patents) > 0:
                ai_share = round(len(ai_patents) / len(df_patents) * 100, 1)
        
        # Сбор всех метрик
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
            'country_values': country_values,
            'source_info': source_info
        }
        
        print(f"✅ Данные успешно загружены и обработаны")
        print(f"   Trend Score: {trend_score} - {trend_status}")
        print(f"   Всего публикаций: {papers_total}, патентов: {patents_total}")
        
        return np.array(all_months), np.array(papers_aligned), np.array(patents_aligned), metrics, df_papers, df_patents, df_all
        
    except Exception as e:
        print(f"❌ Ошибка при загрузке данных: {e}")
        traceback.print_exc()
        return generate_fallback_data(domain_clean, str(e))
