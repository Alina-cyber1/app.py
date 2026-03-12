import os
import pandas as pd
import numpy as np
from pathlib import Path
import sys

# Добавляем корень проекта в sys.path, чтобы импортировать модули коллег
project_root = Path(__file__).parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# Импортируем функции коллег
from etl.preprocessing import clean_domain_data  # если нужно запустить очистку с нуля
from etl.metrics import calc_cagr, calc_yoy, calc_acceleration

# Папки
RAW_DIR = project_root / "data" / "raw"           # сырые батчи (если есть)
PROCESSED_DIR = project_root / "data" / "processed"  # очищенные файлы
SUMMARY_DIR = project_root / "data" / "summary"
SUMMARY_DIR.mkdir(parents=True, exist_ok=True)

# Домены
DOMAINS = {
    "semiconductors": "Полупроводники",
    "gene_engineering": "Генная инженерия"
}

def build_timeseries_from_clean(clean_file):
    """Загружает очищенный parquet, группирует по месяцам, возвращает dates и counts."""
    df = pd.read_parquet(clean_file)
    # Предполагаем, что есть колонка 'publication_date' с датой
    if 'publication_date' not in df.columns:
        raise ValueError(f"В файле {clean_file} нет колонки publication_date")
    
    # Преобразуем в datetime, если ещё не
    df['date'] = pd.to_datetime(df['publication_date'])
    df['year_month'] = df['date'].dt.to_period('M').astype(str)
    
    # Группировка
    monthly = df.groupby('year_month').size().reset_index(name='count')
    monthly = monthly.sort_values('year_month')
    
    dates = monthly['year_month'].tolist()
    papers = monthly['count'].tolist()
    
    # Общее количество
    total_papers = len(df)
    
    # Среднее цитирование (если есть колонка citations)
    if 'citations' in df.columns:
        avg_citations = df['citations'].mean()
        avg_citations = round(avg_citations, 2) if pd.notna(avg_citations) else 0
    else:
        avg_citations = 0
    
    return dates, papers, total_papers, avg_citations

def compute_metrics_from_timeseries(dates, papers):
    """Вычисляет CAGR, YoY, ускорение на основе годовых агрегатов."""
    # Преобразуем месячные данные в годовые
    df_monthly = pd.DataFrame({'year_month': dates, 'papers': papers})
    df_monthly['year'] = df_monthly['year_month'].str[:4].astype(int)
    yearly = df_monthly.groupby('year')['papers'].sum().reset_index()
    
    if len(yearly) < 2:
        return {
            'cagr': None,
            'yoy_last': None,
            'acceleration': None
        }
    
    first_year = yearly['year'].min()
    last_year = yearly['year'].max()
    first_count = yearly.loc[yearly['year'] == first_year, 'papers'].values[0]
    last_count = yearly.loc[yearly['year'] == last_year, 'papers'].values[0]
    
    cagr = calc_cagr(first_count, last_count, last_year - first_year)
    yoy_last = calc_yoy(yearly, last_year)
    accel = calc_acceleration(yearly)
    
    return {
        'cagr': round(cagr, 1) if cagr is not None else None,
        'yoy_last': round(yoy_last, 1) if yoy_last is not None else None,
        'acceleration': accel
    }

def main():
    for domain_key, domain_label in DOMAINS.items():
        print(f"\n🔄 Обработка домена: {domain_label}")
        
        # Путь к очищенному файлу (предполагаем, что он уже создан коллегами)
        clean_file = PROCESSED_DIR / f"{domain_key}_clean.parquet"
        if not clean_file.exists():
            print(f"⚠️ Очищенный файл {clean_file} не найден. Пропускаем.")
            continue
        
        # Строим временные ряды
        dates, papers, total_papers, avg_citations = build_timeseries_from_plain(clean_file)
        
        # Вычисляем метрики
        metrics = compute_metrics_from_timeseries(dates, papers)
        
        # Тестовые данные для патентов и остального (пока заглушки)
        # Позже можно заменить реальными, когда появятся данные по патентам
        patents_total = np.random.randint(5000, 20000)
        patents_growth = round(np.random.uniform(5, 25), 1)
        time_lag = round(np.random.uniform(2.5, 4.5), 1)
        time_lag_change = f"+{round(np.random.uniform(0.1, 0.8), 1)}"
        trend_score = np.random.randint(60, 95)
        ai_share = np.random.randint(10, 50)
        top_assignees = ['Компания А (тест)', 'Компания Б (тест)', 'Компания В (тест)']
        assignee_values = [120, 80, 40]
        countries = ['США', 'Китай', 'Япония']
        country_values = [45, 30, 15]
        
        # Формируем итоговый словарь
        summary = {
            'domain_key': domain_key,
            'domain_label': domain_label,
            'dates': dates,
            'papers': papers,
            'patents': [0] * len(dates),  # пока пусто
            'papers_total': total_papers,
            'patents_total': patents_total,
            'papers_cited_avg': avg_citations,
            'papers_growth': metrics['yoy_last'] if metrics['yoy_last'] else 0,
            'patents_growth': patents_growth,
            'time_lag': time_lag,
            'time_lag_change': time_lag_change,
            'trend_score': trend_score,
            'trend_status': 'Взрывной рост' if trend_score >= 80 else 'Стабильный рост' if trend_score >= 60 else 'Созревание',
            'ai_share': ai_share,
            'top_assignees': top_assignees,
            'assignee_values': assignee_values,
            'countries': countries,
            'country_values': country_values
        }
        
        # Сохраняем в Parquet
        out_file = SUMMARY_DIR / f"{domain_key}_summary.parquet"
        pd.DataFrame([summary]).to_parquet(out_file, index=False)
        print(f"✅ Сохранён summary: {out_file}")
        
        # Также можно сохранить временные ряды отдельно (опционально)
        ts_file = SUMMARY_DIR / f"{domain_key}_timeseries.parquet"
        ts_df = pd.DataFrame({'date': dates, 'papers': papers})
        ts_df.to_parquet(ts_file, index=False)
        print(f"✅ Сохранён временной ряд: {ts_file}")

if __name__ == "__main__":
    main()
