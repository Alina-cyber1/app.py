import streamlit as st
import pandas as pd
import numpy as np
from pathlib import Path
import gdown

# ---------- Константы ----------
DATA_DIR = Path(__file__).parent / "data" / "processed"
DATA_DIR.mkdir(parents=True, exist_ok=True)

# ID файлов на Google Drive (clean-датасеты)
FILES = {
    "semiconductors_clean_full.parquet": "1CwfeO6WY7gKqov5mAtaD1ffvsxBzdjR5",
    "gene_engineering_clean_full.parquet": "1mLx-mh1k4M4zNAOATLQiFXy06s0tfZHl"
}

# ---------- Автоматическое скачивание ----------
def download_files():
    for filename, file_id in FILES.items():
        dest = DATA_DIR / filename
        if not dest.exists():
            print(f"📥 Скачивание {filename}...")
            url = f"https://drive.google.com/uc?id={file_id}"
            gdown.download(url, str(dest), quiet=False)

download_files()

@st.cache_data(ttl=3600)
def load_domain_data(domain_clean):
    """
    Загружает clean-данные для домена.
    Возвращает (dates, papers, patents, metrics) в формате, ожидаемом app.py.
    """
    print(f"🔍 Загрузка данных для домена: {domain_clean}")

    # Выбираем файл в зависимости от домена
    if domain_clean == "Полупроводники":
        file_name = "semiconductors_clean_full.parquet"
    elif domain_clean == "Генная инженерия":
        file_name = "gene_engineering_clean_full.parquet"
    else:
        return np.array([]), np.array([]), np.array([]), {}

    file_path = DATA_DIR / file_name

    if not file_path.exists():
        print(f"❌ Файл {file_name} не найден")
        return np.array([]), np.array([]), np.array([]), {}

    # Читаем данные
    df = pd.read_parquet(file_path, engine='fastparquet')
    print(f"✅ Загружено строк: {len(df)}")
    print(f"📋 Колонки: {list(df.columns)}")

    # Разделяем публикации и патенты (ожидается колонка 'type')
    if 'type' in df.columns:
        publications = df[df['type'] == 'publication'].copy()
        patents_df = df[df['type'] == 'patent'].copy()
    else:
        publications = df.copy()
        patents_df = pd.DataFrame()

    print(f"📄 Публикаций: {len(publications)}")
    print(f"📃 Патентов: {len(patents_df)}")

    # --- Обработка публикаций ---
    if len(publications) > 0:
        if 'publication_date' not in publications.columns:
            # Возможно колонка называется 'date'
            if 'date' in publications.columns:
                publications.rename(columns={'date': 'publication_date'}, inplace=True)
            else:
                raise ValueError("Нет колонки с датой публикации")

        publications['publication_date'] = pd.to_datetime(publications['publication_date'], errors='coerce')
        publications = publications.dropna(subset=['publication_date'])
        publications['month'] = publications['publication_date'].dt.to_period('M')
        monthly_pubs = publications.groupby('month').size().reset_index(name='papers')
        monthly_pubs['month'] = monthly_pubs['month'].dt.to_timestamp()
    else:
        monthly_pubs = pd.DataFrame(columns=['month', 'papers'])

    # --- Обработка патентов ---
    if len(patents_df) > 0:
        if 'publication_date' not in patents_df.columns:
            if 'date' in patents_df.columns:
                patents_df.rename(columns={'date': 'publication_date'}, inplace=True)
            else:
                patents_df = pd.DataFrame()

    if len(patents_df) > 0:
        patents_df['publication_date'] = pd.to_datetime(patents_df['publication_date'], errors='coerce')
        patents_df = patents_df.dropna(subset=['publication_date'])
        patents_df['month'] = patents_df['publication_date'].dt.to_period('M')
        monthly_patents = patents_df.groupby('month').size().reset_index(name='patents')
        monthly_patents['month'] = monthly_patents['month'].dt.to_timestamp()
    else:
        monthly_patents = pd.DataFrame(columns=['month', 'patents'])

    # --- Объединяем по месяцам ---
    # Определяем общий диапазон месяцев
    all_months = pd.date_range(
        start=min(monthly_pubs['month'].min() if not monthly_pubs.empty else pd.Timestamp('2015-01-01'),
                  monthly_patents['month'].min() if not monthly_patents.empty else pd.Timestamp('2015-01-01')),
        end=max(monthly_pubs['month'].max() if not monthly_pubs.empty else pd.Timestamp.now(),
                monthly_patents['month'].max() if not monthly_patents.empty else pd.Timestamp.now()),
        freq='MS'
    )

    monthly = pd.DataFrame({'month': all_months})

    if not monthly_pubs.empty:
        monthly = monthly.merge(monthly_pubs[['month', 'papers']], on='month', how='left')
        monthly['papers'] = monthly['papers'].fillna(0).astype(int)
    else:
        monthly['papers'] = 0

    if not monthly_patents.empty:
        monthly = monthly.merge(monthly_patents[['month', 'patents']], on='month', how='left')
        monthly['patents'] = monthly['patents'].fillna(0).astype(int)
    else:
        monthly['patents'] = 0

    # Если совсем нет данных, возвращаем пустые массивы
    if monthly['papers'].sum() == 0 and monthly['patents'].sum() == 0:
        return np.array([]), np.array([]), np.array([]), {}

    dates = monthly['month'].values
    papers = monthly['papers'].values
    patents = monthly['patents'].values

    total_papers = papers.sum()
    total_patents = patents.sum()

    # --- Метрики роста ---
    # Годовой рост публикаций
    yearly_pubs = monthly.groupby(monthly['month'].dt.year)['papers'].sum()
    if len(yearly_pubs) >= 2:
        last_year = yearly_pubs.index[-1]
        prev_year = yearly_pubs.index[-2]
        papers_growth = ((yearly_pubs[last_year] / yearly_pubs[prev_year]) - 1) * 100
    else:
        papers_growth = 0.0

    # Годовой рост патентов
    yearly_patents = monthly.groupby(monthly['month'].dt.year)['patents'].sum()
    if len(yearly_patents) >= 2:
        last_year = yearly_patents.index[-1]
        prev_year = yearly_patents.index[-2]
        patents_growth = ((yearly_patents[last_year] / yearly_patents[prev_year]) - 1) * 100
    else:
        patents_growth = 0.0

    # --- Топ заявителей (если есть колонка assignee) ---
    if len(patents_df) > 0 and 'assignee' in patents_df.columns:
        top_assignees_data = patents_df['assignee'].value_counts().head(5)
        top_assignees = top_assignees_data.index.tolist()
        assignee_values = top_assignees_data.values.tolist()
    else:
        # Заглушки
        if domain_clean == "Полупроводники":
            top_assignees = ["TSMC", "Intel", "Samsung", "Qualcomm", "Micron"]
            assignee_values = [234, 189, 156, 98, 76]
        else:
            top_assignees = ["Editas Medicine", "CRISPR Therapeutics", "Intellia", "Vertex", "Moderna"]
            assignee_values = [145, 132, 98, 67, 54]

    # --- География (если есть country) ---
    if len(patents_df) > 0 and 'country' in patents_df.columns:
        geo_data = patents_df['country'].value_counts().head(5)
        countries = geo_data.index.tolist()
        country_values = geo_data.values.tolist()
    else:
        if domain_clean == "Полупроводники":
            countries = ['US', 'CN', 'JP', 'KR', 'EP']
            country_values = [45, 25, 12, 10, 8]
        else:
            countries = ['US', 'CN', 'EP', 'JP', 'KR']
            country_values = [58, 18, 12, 7, 5]

    # Trend score (по патентам, как раньше)
    norm_total = min(100, total_patents / 5000 * 100)
    norm_growth = min(100, max(0, patents_growth * 2))
    trend_score = int((norm_total + norm_growth) / 2)
    if trend_score > 80:
        trend_status = '🔥 Hot'
    elif trend_score > 60:
        trend_status = '📈 Emerging'
    else:
        trend_status = '💤 Mature'

    metrics = {
        'papers_total': int(total_papers),
        'papers_growth': round(papers_growth, 1),
        'patents_total': int(total_patents),
        'patents_growth': round(patents_growth, 1),
        'time_lag': 3.2 if domain_clean == "Полупроводники" else 4.8,
        'time_lag_change': -0.5 if domain_clean == "Полупроводники" else -1.2,
        'trend_score': trend_score,
        'trend_status': trend_status,
        'top_assignees': top_assignees,
        'assignee_values': assignee_values,
        'countries': countries,
        'country_values': country_values,
        'ai_share': 32 if domain_clean == "Полупроводники" else 18
    }

    print(f"📈 Всего публикаций: {total_papers}")
    print(f"📊 Всего патентов: {total_patents}")
    print(f"📊 Trend Score: {trend_score} - {trend_status}")

    return dates, papers, patents, metrics
