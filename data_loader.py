import pandas as pd
import numpy as np
import os
from datetime import datetime

DATA_DIR = os.path.join(os.path.dirname(__file__), 'data', 'processed')

def load_domain_data(domain_clean):
    """
    Загружает данные для выбранного домена.
    Если файл не найден или поврежден - использует синтетические данные.
    """
    domain_map = {
        'Полупроводники': 'semiconductors',
        'Генная инженерия': 'gene_engineering'
    }
    domain_key = domain_map.get(domain_clean, domain_clean.lower())
    file_path = os.path.join(DATA_DIR, f"{domain_key}_clean.parquet")
    
    print(f"🔍 Загрузка данных из: {file_path}")
    
    # Пробуем загрузить данные из файла
    if os.path.exists(file_path):
        try:
            # Пробуем прочитать с разными движками
            try:
                df = pd.read_parquet(file_path, engine='pyarrow')
                print(f"✅ Файл прочитан через pyarrow, строк: {len(df)}")
            except:
                try:
                    df = pd.read_parquet(file_path, engine='fastparquet')
                    print(f"✅ Файл прочитан через fastparquet, строк: {len(df)}")
                except Exception as e:
                    print(f"❌ Ошибка чтения файла: {e}")
                    return _generate_synthetic_data(domain_clean)
            
            # Обрабатываем DataFrame
            return _process_dataframe(df, domain_clean)
            
        except Exception as e:
            print(f"❌ Ошибка обработки файла {file_path}: {e}")
            print("🔄 Использую синтетические данные...")
            return _generate_synthetic_data(domain_clean)
    else:
        print(f"❌ Файл {file_path} не найден.")
        print("🔄 Генерирую синтетические данные...")
        return _generate_synthetic_data(domain_clean)

def _process_dataframe(df, domain_clean):
    """Обрабатывает загруженный DataFrame и возвращает данные для отображения"""
    
    # Проверяем наличие нужных колонок
    if 'publication_date' not in df.columns:
        print("⚠️ В файле нет колонки publication_date, использую синтетические данные")
        return _generate_synthetic_data(domain_clean)
    
    df['publication_date'] = pd.to_datetime(df['publication_date'], errors='coerce')
    df = df.dropna(subset=['publication_date'])
    
    if len(df) == 0:
        print("⚠️ Нет данных после обработки дат")
        return _generate_synthetic_data(domain_clean)
    
    df['month'] = df['publication_date'].dt.to_period('M')
    monthly = df.groupby('month').size().reset_index(name='papers')
    monthly['month'] = monthly['month'].dt.to_timestamp()
    
    if not monthly.empty:
        all_months = pd.date_range(
            start=monthly['month'].min(),
            end=monthly['month'].max(),
            freq='MS'
        )
        monthly = monthly.set_index('month').reindex(all_months, fill_value=0).rename_axis('month').reset_index()
        monthly.columns = ['month', 'papers']
        dates = monthly['month'].values
        papers = monthly['papers'].values
    else:
        dates = pd.date_range(start='2020-01-01', end='2025-01-01', freq='M')
        papers = np.zeros(len(dates))
    
    patents = np.zeros_like(papers)
    metrics = _compute_metrics(domain_clean, df, monthly)
    return dates, papers, patents, metrics

def _generate_synthetic_data(domain_clean):
    """Генерирует синтетические данные для демонстрации"""
    print("📊 Генерирую синтетические данные...")
    
    np.random.seed(42 if "Полупроводники" in domain_clean else 123)
    
    # Создаем временной ряд
    dates = pd.date_range(start='2018-01-01', end='2025-01-01', freq='M')
    
    if "Полупроводники" in domain_clean:
        # Данные для полупроводников
        papers = 40 + np.cumsum(np.random.randn(len(dates)) * 1.5 + 1.5)
        patents = 20 + np.cumsum(np.random.randn(len(dates)) * 1.2 + 1.0)
        metrics = {
            'papers_total': 1234,
            'papers_growth': 12.5,
            'patents_total': 892,
            'patents_growth': 8.3,
            'time_lag': 3.2,
            'time_lag_change': -0.5,
            'trend_score': 78,
            'trend_status': '📈 Emerging',
            'top_assignees': ['TSMC', 'Intel', 'Samsung', 'Qualcomm', 'Micron'],
            'assignee_values': [234, 189, 156, 98, 76],
            'countries': ['US', 'CN', 'JP', 'KR', 'EP'],
            'country_values': [45, 25, 12, 10, 8],
            'ai_share': 32
        }
    else:
        # Данные для генной инженерии
        papers = 30 + np.cumsum(np.random.randn(len(dates)) * 1.8 + 2.0)
        patents = 15 + np.cumsum(np.random.randn(len(dates)) * 1.5 + 1.2)
        metrics = {
            'papers_total': 2156,
            'papers_growth': 28.4,
            'patents_total': 743,
            'patents_growth': 35.2,
            'time_lag': 4.8,
            'time_lag_change': -1.2,
            'trend_score': 92,
            'trend_status': '🔥 Hot',
            'top_assignees': ['Editas Medicine', 'CRISPR Therapeutics', 'Intellia', 'Vertex', 'Moderna'],
            'assignee_values': [145, 132, 98, 67, 54],
            'countries': ['US', 'CN', 'EP', 'JP', 'KR'],
            'country_values': [58, 18, 12, 7, 5],
            'ai_share': 18
        }
    
    # Округляем до целых чисел
    papers = np.maximum(0, np.round(papers)).astype(int)
    patents = np.maximum(0, np.round(patents)).astype(int)
    
    print(f"✅ Сгенерировано {len(dates)} точек данных")
    return dates, papers, patents, metrics

def _compute_metrics(domain_clean, df, monthly):
    """Вычисляет метрики на основе данных"""
    total_papers = len(df)
    
    if 'year' in df.columns:
        df['year'] = df['publication_date'].dt.year
        yearly = df.groupby('year').size()
        years = sorted(yearly.index)
        
        if len(yearly) >= 2:
            last_year = years[-1]
            prev_year = years[-2]
            papers_growth = ((yearly[last_year] / yearly[prev_year]) - 1) * 100
        else:
            papers_growth = 0.0
    else:
        papers_growth = 0.0
    
    norm_total = min(100, total_papers / 5000 * 100)
    norm_growth = min(100, max(0, papers_growth * 2))
    trend_score = int((norm_total + norm_growth) / 2)
    
    if trend_score > 80:
        trend_status = '🔥 Hot'
    elif trend_score > 60:
        trend_status = '📈 Emerging'
    else:
        trend_status = '💤 Mature'
    
    if 'Полупроводники' in domain_clean:
        top_assignees = ['TSMC', 'Intel', 'Samsung', 'Qualcomm', 'Micron']
        assignee_values = [234, 189, 156, 98, 76]
        countries = ['US', 'CN', 'JP', 'KR', 'EP']
        country_values = [45, 25, 12, 10, 8]
        ai_share = 32
    else:
        top_assignees = ['Editas Medicine', 'CRISPR Therapeutics', 'Intellia', 'Vertex', 'Moderna']
        assignee_values = [145, 132, 98, 67, 54]
        countries = ['US', 'CN', 'EP', 'JP', 'KR']
        country_values = [58, 18, 12, 7, 5]
        ai_share = 18
    
    metrics = {
        'papers_total': total_papers,
        'papers_growth': round(papers_growth, 1),
        'patents_total': 0,
        'patents_growth': 0,
        'time_lag': 0,
        'time_lag_change': 0,
        'trend_score': trend_score,
        'trend_status': trend_status,
        'top_assignees': top_assignees,
        'assignee_values': assignee_values,
        'countries': countries,
        'country_values': country_values,
        'ai_share': ai_share
    }
    return metrics

# Для обратной совместимости
_fallback_domain_data = _generate_synthetic_data
