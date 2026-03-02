import pandas as pd
import numpy as np
import os
from datetime import datetime

DATA_DIR = os.path.join(os.path.dirname(__file__), 'data', 'processed')

def load_domain_data(domain_clean):
    domain_map = {
        'Полупроводники': 'semiconductors',
        'Генная инженерия': 'gene_engineering'
    }
    domain_key = domain_map.get(domain_clean, domain_clean.lower())
    file_path = os.path.join(DATA_DIR, f"{domain_key}_clean.parquet")

    if not os.path.exists(file_path):
        print(f"Файл {file_path} не найден. Используются синтетические данные.")
        return _fallback_domain_data(domain_clean)

    df = pd.read_parquet(file_path)
    df['publication_date'] = pd.to_datetime(df['publication_date'], errors='coerce')
    df = df.dropna(subset=['publication_date'])
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

def _compute_metrics(domain_clean, df, monthly):
    total_papers = len(df)
    df['year'] = df['publication_date'].dt.year
    yearly = df.groupby('year').size()
    years = sorted(yearly.index)
    if len(yearly) >= 2:
        last_year = years[-1]
        prev_year = years[-2]
        papers_growth = ((yearly[last_year] / yearly[prev_year]) - 1) * 100
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

def _fallback_domain_data(domain_clean):
    if "Полупроводники" in domain_clean:
        dates = pd.date_range(start='2018-01-01', end='2025-01-01', freq='M')
        np.random.seed(42)
        papers = 40 + np.cumsum(np.random.randn(len(dates)) * 1.5 + 1.5)
        patents = 20 + np.cumsum(np.random.randn(len(dates)) * 1.2 + 1.0)
        metrics = {
            'papers_total': 1234, 'papers_growth': 12, 'patents_total': 892,
            'patents_growth': 8, 'time_lag': 3.2, 'time_lag_change': -0.5,
            'trend_score': 78, 'trend_status': '📈 Emerging',
            'top_assignees': ['TSMC', 'Intel', 'Samsung', 'Qualcomm', 'Micron'],
            'assignee_values': [234, 189, 156, 98, 76],
            'countries': ['US', 'CN', 'JP', 'KR', 'EP'],
            'country_values': [45, 25, 12, 10, 8],
            'ai_share': 32
        }
    else:
        dates = pd.date_range(start='2018-01-01', end='2025-01-01', freq='M')
        np.random.seed(123)
        papers = 30 + np.cumsum(np.random.randn(len(dates)) * 1.8 + 2.0)
        patents = 15 + np.cumsum(np.random.randn(len(dates)) * 1.5 + 1.2)
        metrics = {
            'papers_total': 2156, 'papers_growth': 28, 'patents_total': 743,
            'patents_growth': 35, 'time_lag': 4.8, 'time_lag_change': -1.2,
            'trend_score': 92, 'trend_status': '🔥 Hot',
            'top_assignees': ['Editas Medicine', 'CRISPR Therapeutics', 'Intellia', 'Vertex', 'Moderna'],
            'assignee_values': [145, 132, 98, 67, 54],
            'countries': ['US', 'CN', 'EP', 'JP', 'KR'],
            'country_values': [58, 18, 12, 7, 5],
            'ai_share': 18
        }
    return dates, papers, patents, metrics