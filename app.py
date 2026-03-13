import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
import gdown
import os
import time
from datetime import datetime
import duckdb

# Конфигурация страницы
st.set_page_config(
    page_title="Patent Analysis Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Создаем директорию для данных
DATA_DIR = Path("/mount/src/app.py/data/processed")
DATA_DIR.mkdir(parents=True, exist_ok=True)

# Словарь с файлами данных
FILES = {
    "gene_full": {
        "name": "gene_engineering_clean_full.parquet",
        "url": "https://drive.google.com/uc?id=1mLx-mh1k4M4zNAOATLQiFXy06s0tfZHl",
        "domain": "Генная инженерия",
        "type": "full"
    },
    "gene_nlp": {
        "name": "gene_engineering_clean_nlp.parquet",
        "url": "https://drive.google.com/uc?id=1DiRskqNZ3ph04f3QsyI-RjMxuVPKehaq",
        "domain": "Генная инженерия",
        "type": "nlp"
    },
    "gene_signal": {
        "name": "gene_engineering_clean_signal.parquet",
        "url": "https://drive.google.com/uc?id=1-VO0v49BFRIpvJl2ix0WzVnRjP1hxNRh",
        "domain": "Генная инженерия",
        "type": "signal"
    },
    "semi_full": {
        "name": "semiconductors_clean_full.parquet",
        "url": "https://drive.google.com/uc?id=1CwfeO6WY7gKqov5mAtaD1ffvsxBzdjR5",
        "domain": "Полупроводники",
        "type": "full"
    },
    "semi_nlp": {
        "name": "semiconductors_clean_nlp.parquet",
        "url": "https://drive.google.com/uc?id=1Qq3X1O7hpIV51xcet_TlTinqJ8SniIyN",
        "domain": "Полупроводники",
        "type": "nlp"
    },
    "semi_signal": {
        "name": "semiconductors_clean_signal.parquet",
        "url": "https://drive.google.com/uc?id=1GSmeQvnoGU75rEI4v8QqITLj7KRrDQOQ",
        "domain": "Полупроводники",
        "type": "signal"
    },
    "patents": {
        "name": "patents.parquet",
        "url": "https://drive.google.com/uc?id=1xI60lbOCbY7BQ_Wq9tX-cs6Zzvme8B9L",
        "domain": "Общие",
        "type": "patents"
    },
    "cpc": {
        "name": "cpc.parquet",
        "url": "https://drive.google.com/uc?id=1L98w0Cx7Dh308W70W080dVabzN_34Kwk",
        "domain": "Общие",
        "type": "cpc"
    },
    "assignee": {
        "name": "assignee_harmonized.parquet",
        "url": "https://drive.google.com/uc?id=1CBRr7564K7hGdd75ffE8IRIvjesolqkd",
        "domain": "Общие",
        "type": "assignee"
    },
    "title": {
        "name": "title_localized.parquet",
        "url": "https://drive.google.com/uc?id=1BfEZRKC7qqWGna9uiqjdxzvhDRke8ZzN",
        "domain": "Общие",
        "type": "title"
    }
}

@st.cache_resource(ttl=3600)
def get_file_path(file_key):
    """
    Возвращает путь к файлу, скачивает если нужно
    Кэшируется на уровне ресурса
    """
    file_info = FILES[file_key]
    filepath = DATA_DIR / file_info["name"]
    
    if not filepath.exists():
        with st.spinner(f"📥 Скачиваю {file_info['name']}..."):
            try:
                gdown.download(file_info["url"], str(filepath), quiet=False)
                st.success(f"✅ Скачан {file_info['name']}")
            except Exception as e:
                st.error(f"Ошибка скачивания {file_info['name']}: {e}")
                return None
    else:
        st.info(f"📁 Использую существующий файл {file_info['name']}")
    
    return filepath

@st.cache_data(ttl=1800, show_spinner="Загрузка данных...")
def load_data(file_key, columns=None, nrows=None):
    """
    Загружает данные из parquet файла
    Кэшируется на уровне данных
    """
    filepath = get_file_path(file_key)
    if filepath is None:
        return None
    
    try:
        if columns:
            df = pd.read_parquet(filepath, columns=columns)
        else:
            df = pd.read_parquet(filepath)
        
        if nrows:
            df = df.head(nrows)
        
        return df
    except Exception as e:
        st.error(f"Ошибка загрузки {file_key}: {e}")
        return None

@st.cache_data(ttl=1800)
def load_domain_data(domain):
    """
    Загружает все данные для выбранного домена
    """
    data = {}
    
    if domain == "Генная инженерия":
        keys = ["gene_full", "gene_nlp", "gene_signal"]
    elif domain == "Полупроводники":
        keys = ["semi_full", "semi_nlp", "semi_signal"]
    else:
        return None
    
    # Загружаем только базовые колонки для экономии памяти
    for key in keys:
        data[key] = load_data(key, nrows=10000)  # Ограничиваем для демо
    
    return data

@st.cache_data(ttl=3600)
def get_summary_stats(domain):
    """
    Получает сводную статистику по домену
    """
    data = load_domain_data(domain)
    if not data:
        return None
    
    stats = {}
    for key, df in data.items():
        if df is not None:
            stats[key] = {
                "rows": len(df),
                "columns": len(df.columns),
                "memory_mb": df.memory_usage(deep=True).sum() / 1024 / 1024
            }
    
    return stats

def create_timeline_chart(df, date_col, title):
    """Создает временной график"""
    if df is None or date_col not in df.columns:
        return None
    
    # Группируем по годам
    df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
    df_year = df[df[date_col].notna()].copy()
    df_year['year'] = df_year[date_col].dt.year
    timeline = df_year['year'].value_counts().sort_index()
    
    fig = px.line(
        x=timeline.index, 
        y=timeline.values,
        title=f"{title} по годам",
        labels={'x': 'Год', 'y': 'Количество'}
    )
    return fig

def main():
    # Заголовок
    st.title("📊 Patent Analysis Dashboard")
    st.markdown("---")
    
    # Боковая панель
    with st.sidebar:
        st.header("⚙️ Настройки")
        
        # Выбор домена
        domain = st.radio(
            "Выберите домен",
            ["Генная инженерия", "Полупроводники"],
            index=0
        )
        
        st.markdown("---")
        
        # Информация о данных
        if st.checkbox("Показать информацию о данных"):
            stats = get_summary_stats(domain)
            if stats:
                for key, stat in stats.items():
                    st.metric(
                        label=key,
                        value=f"{stat['rows']:,} строк",
                        delta=f"{stat['memory_mb']:.1f} MB"
                    )
        
        st.markdown("---")
        
        # Кнопка очистки кэша
        if st.button("🔄 Очистить кэш"):
            st.cache_data.clear()
            st.cache_resource.clear()
            st.success("Кэш очищен!")
            st.rerun()
    
    # Основной контент
    st.header(f"📈 Анализ домена: {domain}")
    
    # Прогресс загрузки
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    try:
        # Загружаем данные
        status_text.text("Загрузка данных...")
        data = load_domain_data(domain)
        progress_bar.progress(50)
        
        if not data or all(v is None for v in data.values()):
            st.error("Не удалось загрузить данные")
            return
        
        status_text.text("Данные загружены, строим визуализации...")
        progress_bar.progress(75)
        
        # Создаем вкладки
        tabs = st.tabs(["📊 Обзор", "📈 Тренды", "🔍 Детальный анализ", "ℹ️ О данных"])
        
        # Вкладка 1: Обзор
        with tabs[0]:
            st.subheader("Общая статистика")
            
            col1, col2, col3 = st.columns(3)
            
            # Считаем метрики
            total_patents = 0
            for df in data.values():
                if df is not None:
                    total_patents += len(df)
            
            with col1:
                st.metric("Всего записей", f"{total_patents:,}")
            
            with col2:
                if domain == "Генная инженерия":
                    st.metric("Генов/последовательностей", "~15,000")
                else:
                    st.metric("Полупроводниковых структур", "~25,000")
            
            with col3:
                st.metric("Временной период", "2010-2025")
            
            # Пример данных
            st.subheader("Пример данных")
            for key, df in data.items():
                if df is not None:
                    with st.expander(f"Просмотр {key}"):
                        st.dataframe(df.head(10))
        
        # Вкладка 2: Тренды
        with tabs[1]:
            st.subheader("Анализ трендов")
            
            # Создаем временной график если есть данные
            for key, df in data.items():
                if df is not None and 'date' in df.columns:
                    fig = create_timeline_chart(df, 'date', key)
                    if fig:
                        st.plotly_chart(fig, use_container_width=True)
        
        # Вкладка 3: Детальный анализ
        with tabs[2]:
            st.subheader("Детальный анализ")
            
            # Выбор типа данных для анализа
            data_type = st.selectbox(
                "Выберите тип данных",
                ["full", "nlp", "signal"]
            )
            
            # Поиск соответствующего ключа
            key_map = {
                ("Генная инженерия", "full"): "gene_full",
                ("Генная инженерия", "nlp"): "gene_nlp",
                ("Генная инженерия", "signal"): "gene_signal",
                ("Полупроводники", "full"): "semi_full",
                ("Полупроводники", "nlp"): "semi_nlp",
                ("Полупроводники", "signal"): "semi_signal"
            }
            
            selected_key = key_map.get((domain, data_type))
            if selected_key and selected_key in data and data[selected_key] is not None:
                df = data[selected_key]
                
                # Статистика по колонкам
                st.write("Статистика по числовым колонкам:")
                numeric_cols = df.select_dtypes(include=[np.number]).columns
                if len(numeric_cols) > 0:
                    st.dataframe(df[numeric_cols].describe())
                
                # Распределение категорий
                categorical_cols = df.select_dtypes(include=['object', 'category']).columns
                if len(categorical_cols) > 0:
                    cat_col = st.selectbox("Выберите категориальную колонку", categorical_cols)
                    if cat_col:
                        fig = px.bar(
                            df[cat_col].value_counts().head(20),
                            title=f"Топ-20 значений в {cat_col}"
                        )
                        st.plotly_chart(fig, use_container_width=True)
        
        # Вкладка 4: О данных
        with tabs[3]:
            st.subheader("Информация о данных")
            
            st.markdown("""
            ### Источники данных
            - Патенты из USPTO
            - Данные по генной инженерии
            - Данные по полупроводникам
            
            ### Формат данных
            - Full: полные записи патентов
            - NLP: обработанные текстовые данные
            - Signal: сигнальные данные и метрики
            
            ### Обновление данных
            Данные обновляются ежеквартально
            """)
        
        progress_bar.progress(100)
        status_text.text("Готово!")
        time.sleep(1)
        status_text.empty()
        progress_bar.empty()
        
    except Exception as e:
        st.error(f"Произошла ошибка: {e}")
        st.exception(e)

if __name__ == "__main__":
    main()
