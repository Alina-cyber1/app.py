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
        # Пробуем разные движки для чтения
        engines = ['pyarrow', 'fastparquet']
        
        for engine in engines:
            try:
                df = pd.read_parquet(file_path, engine=engine)
                print(f"✅ Файл прочитан через {engine}, строк: {len(df)}")
                
                # Обрабатываем DataFrame
                return _process_dataframe(df, domain_clean)
                
            except ImportError:
                print(f"⚠️ {engine} не установлен, пробуем другой...")
                continue
            except Exception as e:
                print(f"❌ Ошибка чтения через {engine}: {e}")
                continue
        
        # Если ни один движок не сработал
        print("❌ Не удалось прочитать файл ни одним движком")
        print("🔄 Использую синтетические данные...")
        return _generate_synthetic_data(domain_clean)
    else:
        print(f"❌ Файл {file_path} не найден")
        print("🔄 Генерирую синтетические данные...")
        return _generate_synthetic_data(domain_clean)

def _process_dataframe(df, domain_clean):
    """Обрабатывает загруженный DataFrame"""
    
    # Проверяем наличие колонки publication_date
    if 'publication_date' not in df.columns:
        print("⚠️ Нет колонки publication_date, ищем альтернативы...")
        # Ищем колонку с датой
        date_cols = [col for col in df.columns if 'date' in col.lower() or 'year' in col.lower()]
        if date_cols:
            df['publication_date'] = pd.to_datetime(df[date_cols[0]], errors='coerce')
        else:
            print("⚠️ Не найдено колонок с датами")
            return _generate_synthetic_data(domain_clean)
    
    df['publication_date'] = pd.to_datetime(df['publication_date'], errors='coerce')
    df = df.dropna(subset=['publication_date'])
    
    if len(df) == 0:
        print("⚠️ Нет данных после обработки дат")
        return _generate_synthetic_data(domain_clean)
    
    # Группируем по месяцам
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
        dates = pd.date_range(start='2020-01-01', end='2025-01-01', freq='MS')
        papers = np.zeros(len(dates))
    
    patents = np.zeros_like(papers)
    metrics = _compute_metrics(domain_clean, df)
    return dates, papers, patents, metrics

def _generate_synthetic_data(domain_clean):
    """Генерирует синтетические данные"""
    print("📊 Генерирую синтетические данные...")
    
    np.random.seed(42 if "Полупроводники" in domain_clean else 123)
    
    dates = pd.date_range(start='2018-01-01', end='2025-01-01', freq='MS')
    
    if "Полупроводники" in domain_clean:
        papers = 40 + np.cumsum(np.random.randn(len(dates)) * 1.5 + 1.5)
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
        papers = 30 + np.cumsum(np.random.randn(len(dates)) * 1.8 + 2.0)
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
    
    papers = np.maximum(0, np.round(papers)).astype(int)
    patents = np.zeros_like(papers)
    
    return dates, papers, patents, metrics

def _compute_metrics(domain_clean, df):
    """Вычисляет метрики"""
    total_papers = len(df)
    
    # Годовой рост
    df['year'] = df['publication_date'].dt.year
    yearly = df.groupby('year').size()
    
    if len(yearly) >= 2:
        years = sorted(yearly.index)
        last_year = years[-1]
        prev_year = years[-2]
        papers_growth = ((yearly[last_year] / yearly[prev_year]) - 1) * 100
    else:
        papers_growth = 0.0
    
    # Trend score
    norm_total = min(100, total_papers / 5000 * 100)
    norm_growth = min(100, max(0, papers_growth * 2))
    trend_score = int((norm_total + norm_growth) / 2)
    
    if trend_score > 80:
        trend_status = '🔥 Hot'
    elif trend_score > 60:
        trend_status = '📈 Emerging'
    else:
        trend_status = '💤 Mature'
    
    # Данные для отображения
    if 'Полупроводники' in domain_clean:
        metrics = {
            'papers_total': total_papers,
            'papers_growth': round(papers_growth, 1),
            'patents_total': 0,
            'patents_growth': 0,
            'time_lag': 3.2,
            'time_lag_change': -0.5,
            'trend_score': trend_score,
            'trend_status': trend_status,
            'top_assignees': ['TSMC', 'Intel', 'Samsung', 'Qualcomm', 'Micron'],
            'assignee_values': [234, 189, 156, 98, 76],
            'countries': ['US', 'CN', 'JP', 'KR', 'EP'],
            'country_values': [45, 25, 12, 10, 8],
            'ai_share': 32
        }
    else:
        metrics = {
            'papers_total': total_papers,
            'papers_growth': round(papers_growth, 1),
            'patents_total': 0,
            'patents_growth': 0,
            'time_lag': 4.8,
            'time_lag_change': -1.2,
            'trend_score': trend_score,
            'trend_status': trend_status,
            'top_assignees': ['Editas Medicine', 'CRISPR Therapeutics', 'Intellia', 'Vertex', 'Moderna'],
            'assignee_values': [145, 132, 98, 67, 54],
            'countries': ['US', 'CN', 'EP', 'JP', 'KR'],
            'country_values': [58, 18, 12, 7, 5],
            'ai_share': 18
        }
    
    return metrics
