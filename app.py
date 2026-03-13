import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
from pathlib import Path
import gdown
import time

# ДОЛЖНА быть первой командой Streamlit
st.set_page_config(
    page_title="Patent Analysis",
    page_icon="📊",
    layout="wide"
)

# Заголовок
st.title("📊 Patent Analysis Dashboard")
st.markdown("---")

# Создаем директорию для данных
DATA_DIR = Path("data/processed")
DATA_DIR.mkdir(parents=True, exist_ok=True)

# Прямые ссылки на файлы (рабочие)
FILES = {
    "gene_full": {
        "name": "gene_engineering_clean_full.parquet",
        "url": "https://drive.google.com/uc?id=1mLx-mh1k4M4zNAOATLQiFXy06s0tfZHl",
        "display": "🧬 Генная инженерия (полные)"
    },
    "gene_nlp": {
        "name": "gene_engineering_clean_nlp.parquet",
        "url": "https://drive.google.com/uc?id=1DiRskqNZ3ph04f3QsyI-RjMxuVPKehaq",
        "display": "🧬 Генная инженерия (NLP)"
    },
    "gene_signal": {
        "name": "gene_engineering_clean_signal.parquet",
        "url": "https://drive.google.com/uc?id=1-VO0v49BFRIpvJl2ix0WzVnRjP1hxNRh",
        "display": "🧬 Генная инженерия (сигналы)"
    },
    "semi_full": {
        "name": "semiconductors_clean_full.parquet",
        "url": "https://drive.google.com/uc?id=1CwfeO6WY7gKqov5mAtaD1ffvsxBzdjR5",
        "display": "💻 Полупроводники (полные)"
    },
    "semi_nlp": {
        "name": "semiconductors_clean_nlp.parquet",
        "url": "https://drive.google.com/uc?id=1Qq3X1O7hpIV51xcet_TlTinqJ8SniIyN",
        "display": "💻 Полупроводники (NLP)"
    },
    "semi_signal": {
        "name": "semiconductors_clean_signal.parquet",
        "url": "https://drive.google.com/uc?id=1GSmeQvnoGU75rEI4v8QqITLj7KRrDQOQ",
        "display": "💻 Полупроводники (сигналы)"
    }
}

# Функция для проверки существования файла
def file_exists(file_key):
    filepath = DATA_DIR / FILES[file_key]["name"]
    return filepath.exists()

# Функция для получения размера файла
def get_file_size(file_key):
    filepath = DATA_DIR / FILES[file_key]["name"]
    if filepath.exists():
        return round(filepath.stat().st_size / (1024 * 1024), 1)
    return 0

# Функция для скачивания файла
def download_file(file_key):
    file_info = FILES[file_key]
    filepath = DATA_DIR / file_info["name"]
    
    # Прогресс бар
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    try:
        status_text.text(f"📥 Скачиваю {file_info['name']}...")
        gdown.download(file_info["url"], str(filepath), quiet=False)
        progress_bar.progress(100)
        status_text.text(f"✅ Скачано: {file_info['name']}")
        time.sleep(1)
        progress_bar.empty()
        status_text.empty()
        return True
    except Exception as e:
        st.error(f"Ошибка: {e}")
        progress_bar.empty()
        status_text.empty()
        return False

# Функция для загрузки данных
@st.cache_data(ttl=3600)
def load_data(file_key, n_rows=5000):
    filepath = DATA_DIR / FILES[file_key]["name"]
    
    if not filepath.exists():
        return None
    
    try:
        df = pd.read_parquet(filepath)
        if n_rows and len(df) > n_rows:
            df = df.head(n_rows)
        return df
    except Exception as e:
        st.error(f"Ошибка загрузки: {e}")
        return None

# Боковая панель
with st.sidebar:
    st.header("⚙️ Настройки")
    
    # Выбор домена
    domain = st.radio(
        "Выберите домен",
        ["Генная инженерия", "Полупроводники"]
    )
    
    # Размер выборки
    sample_size = st.slider(
        "Количество строк",
        min_value=1000,
        max_value=10000,
        value=5000,
        step=1000
    )
    
    st.markdown("---")
    
    # Статус файлов
    st.subheader("📁 Статус файлов")
    
    if domain == "Генная инженерия":
        keys = ["gene_full", "gene_nlp", "gene_signal"]
    else:
        keys = ["semi_full", "semi_nlp", "semi_signal"]
    
    for key in keys:
        exists = file_exists(key)
        size = get_file_size(key)
        if exists:
            st.success(f"✅ {FILES[key]['display']} ({size} MB)")
        else:
            st.warning(f"⏳ {FILES[key]['display']} (не скачан)")
    
    st.markdown("---")
    
    # Кнопка очистки кэша
    if st.button("🔄 Очистить кэш"):
        st.cache_data.clear()
        st.success("Кэш очищен!")
        st.rerun()

# Основная область
st.header(f"📈 Домен: {domain}")

# Кнопка загрузки
if st.button("🚀 Загрузить данные", type="primary", use_container_width=True):
    
    # Определяем какие файлы нужно скачать
    if domain == "Генная инженерия":
        files_to_download = ["gene_full", "gene_nlp", "gene_signal"]
    else:
        files_to_download = ["semi_full", "semi_nlp", "semi_signal"]
    
    # Скачиваем файлы, которых нет
    for key in files_to_download:
        if not file_exists(key):
            success = download_file(key)
            if not success:
                st.stop()
    
    st.success("✅ Все данные загружены!")
    
    # Загружаем данные
    data = {}
    for key in files_to_download:
        with st.spinner(f"Загрузка {FILES[key]['display']}..."):
            df = load_data(key, sample_size)
            if df is not None:
                data[FILES[key]['display']] = df
    
    # Сохраняем в session state
    st.session_state['data'] = data
    st.session_state['domain'] = domain
    st.rerun()

# Если данные уже загружены
if 'data' in st.session_state and st.session_state['domain'] == domain:
    data = st.session_state['data']
    
    # Создаем вкладки
    tab1, tab2, tab3 = st.tabs(["📊 Обзор", "📈 Графики", "🔍 Данные"])
    
    with tab1:
        st.subheader("Общая информация")
        
        # Метрики
        cols = st.columns(3)
        total_rows = sum(len(df) for df in data.values())
        
        cols[0].metric("Всего записей", f"{total_rows:,}")
        cols[1].metric("Типов данных", len(data))
        cols[2].metric("Домен", domain)
        
        # Информация по каждому датасету
        for name, df in data.items():
            with st.expander(f"📊 {name}"):
                st.write(f"**Строк:** {len(df):,}")
                st.write(f"**Колонок:** {len(df.columns)}")
                st.write("**Первые 5 строк:**")
                st.dataframe(df.head())
    
    with tab2:
        st.subheader("Визуализация")
        
        # Выбор датасета
        selected = st.selectbox("Выберите данные", list(data.keys()))
        df = data[selected]
        
        # Числовые колонки
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        
        if numeric_cols:
            # Выбор колонки
            col = st.selectbox("Выберите колонку", numeric_cols)
            
            # Гистограмма
            fig = px.histogram(
                df, 
                x=col,
                title=f"Распределение {col}",
                nbins=50
            )
            st.plotly_chart(fig, use_container_width=True)
            
            # Статистика
            st.write("**Статистика:**")
            st.write(df[col].describe())
        else:
            st.info("Нет числовых данных для визуализации")
    
    with tab3:
        st.subheader("Просмотр данных")
        
        # Выбор датасета
        selected = st.selectbox("Выберите данные", list(data.keys()), key="viewer")
        df = data[selected]
        
        # Показываем данные
        st.dataframe(df, use_container_width=True)
        
        # Информация о колонках
        with st.expander("📋 Информация о колонках"):
            col_info = pd.DataFrame({
                'Колонка': df.columns,
                'Тип': df.dtypes.values,
                'Непустых': df.count().values,
                'Уникальных': [df[col].nunique() for col in df.columns]
            })
            st.dataframe(col_info)

else:
    # Приветственный экран
    st.info("👆 Нажмите кнопку 'Загрузить данные' чтобы начать работу")
    
    with st.expander("ℹ️ О датасетах"):
        st.markdown("""
        ### Доступные данные:
        
        **🧬 Генная инженерия:**
        - Полные данные патентов
        - NLP-обработанные тексты
        - Сигнальные метрики
        
        **💻 Полупроводники:**
        - Полные данные патентов
        - NLP-обработанные тексты
        - Сигнальные метрики
        
        ### Как это работает:
        1. Выберите домен
        2. Настройте количество строк
        3. Нажмите кнопку загрузки
        4. Исследуйте данные!
        """)
