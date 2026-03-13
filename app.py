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

# Конфигурация страницы - ДОЛЖНА БЫТЬ ПЕРВОЙ командой Streamlit
st.set_page_config(
    page_title="Patent Analysis Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Создаем директорию для данных
DATA_DIR = Path("/mount/src/app.py/data/processed")
DATA_DIR.mkdir(parents=True, exist_ok=True)

# Словарь с файлами данных (ID файлов из Google Drive)
# Используем прямые ID вместо secrets для надежности
FILE_IDS = {
    "gene_full": "1mLx-mh1k4M4zNAOATLQiFXy06s0tfZHl",
    "gene_nlp": "1DiRskqNZ3ph04f3QsyI-RjMxuVPKehaq",
    "gene_signal": "1-VO0v49BFRIpvJl2ix0WzVnRjP1hxNRh",
    "semi_full": "1CwfeO6WY7gKqov5mAtaD1ffvsxBzdjR5",
    "semi_nlp": "1Qq3X1O7hpIV51xcet_TlTinqJ8SniIyN",
    "semi_signal": "1GSmeQvnoGU75rEI4v8QqITLj7KRrDQOQ",
    "patents": "1xI60lbOCbY7BQ_Wq9tX-cs6Zzvme8B9L",
    "cpc": "1L98w0Cx7Dh308W70W080dVabzN_34Kwk",
    "assignee": "1CBRr7564K7hGdd75ffE8IRIvjesolqkd",
    "title": "1BfEZRKC7qqWGna9uiqjdxzvhDRke8ZzN"
}

FILE_NAMES = {
    "gene_full": "gene_engineering_clean_full.parquet",
    "gene_nlp": "gene_engineering_clean_nlp.parquet",
    "gene_signal": "gene_engineering_clean_signal.parquet",
    "semi_full": "semiconductors_clean_full.parquet",
    "semi_nlp": "semiconductors_clean_nlp.parquet",
    "semi_signal": "semiconductors_clean_signal.parquet",
    "patents": "patents.parquet",
    "cpc": "cpc.parquet",
    "assignee": "assignee_harmonized.parquet",
    "title": "title_localized.parquet"
}

def get_file_url(file_key):
    """Формирует URL для скачивания с Google Drive"""
    file_id = FILE_IDS[file_key]
    return f"https://drive.google.com/uc?id={file_id}"

@st.cache_resource(ttl=3600, show_spinner=False)
def check_file_exists(file_key):
    """Проверяет существование файла (кэшируется)"""
    filepath = DATA_DIR / FILE_NAMES[file_key]
    return filepath.exists()

@st.cache_resource(ttl=3600, show_spinner=False)
def get_file_size(file_key):
    """Возвращает размер файла если он существует"""
    filepath = DATA_DIR / FILE_NAMES[file_key]
    if filepath.exists():
        return filepath.stat().st_size / (1024 * 1024)  # в MB
    return 0

def download_file(file_key):
    """
    Скачивает файл с прогресс-баром
    """
    filepath = DATA_DIR / FILE_NAMES[file_key]
    
    # Создаем прогресс-бар в streamlit
    progress_text = f"📥 Скачиваю {FILE_NAMES[file_key]}..."
    my_bar = st.progress(0, text=progress_text)
    
    try:
        # Функция для обновления прогресса
        def progress_callback(current, total):
            if total > 0:
                progress = int(current / total * 100)
                my_bar.progress(progress / 100, text=progress_text)
        
        # Скачиваем файл
        url = get_file_url(file_key)
        gdown.download(url, str(filepath), quiet=True, resume=True)
        
        my_bar.progress(1.0, text=f"✅ Скачан {FILE_NAMES[file_key]}")
        time.sleep(0.5)
        my_bar.empty()
        
        return True
    except Exception as e:
        my_bar.empty()
        st.error(f"Ошибка скачивания {FILE_NAMES[file_key]}: {e}")
        return False

@st.cache_data(ttl=1800, show_spinner="Загрузка данных...")
def load_data(file_key, nrows=None):
    """
    Загружает данные из parquet файла
    """
    filepath = DATA_DIR / FILE_NAMES[file_key]
    
    # Если файла нет - скачиваем
    if not filepath.exists():
        with st.spinner(f"Скачиваю {FILE_NAMES[file_key]}..."):
            success = download_file(file_key)
            if not success:
                return None
    
    try:
        # Загружаем данные
        if nrows:
            df = pd.read_parquet(filepath)
            df = df.head(nrows)
        else:
            df = pd.read_parquet(filepath)
        
        return df
    except Exception as e:
        st.error(f"Ошибка загрузки {file_key}: {e}")
        return None

def load_domain_data(domain, sample_size=5000):
    """
    Загружает данные для домена с ограничением размера
    """
    data = {}
    
    # Определяем ключи для домена
    if domain == "Генная инженерия":
        keys = ["gene_full", "gene_nlp", "gene_signal"]
        display_names = ["Полные данные", "NLP данные", "Сигнальные данные"]
    else:  # Полупроводники
        keys = ["semi_full", "semi_nlp", "semi_signal"]
        display_names = ["Полные данные", "NLP данные", "Сигнальные данные"]
    
    # Загружаем данные
    for key, display_name in zip(keys, display_names):
        with st.spinner(f"Загрузка {display_name}..."):
            df = load_data(key, nrows=sample_size)
            if df is not None:
                data[display_name] = df
                st.success(f"✅ {display_name} загружены ({len(df)} записей)")
    
    return data

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
            index=0,
            key="domain_selector"
        )
        
        st.markdown("---")
        
        # Размер выборки
        sample_size = st.slider(
            "Размер выборки (строк)",
            min_value=1000,
            max_value=20000,
            value=5000,
            step=1000,
            help="Меньший размер = быстрее загрузка"
        )
        
        st.markdown("---")
        
        # Информация о кэше
        if st.button("🔄 Очистить кэш"):
            st.cache_data.clear()
            st.cache_resource.clear()
            st.success("✅ Кэш очищен!")
            st.rerun()
        
        # Статус файлов
        with st.expander("📁 Статус файлов"):
            for key, name in FILE_NAMES.items():
                if key.startswith("gene_") and domain != "Генная инженерия":
                    continue
                if key.startswith("semi_") and domain != "Полупроводники":
                    continue
                
                exists = check_file_exists(key)
                size = get_file_size(key)
                
                if exists:
                    st.success(f"✅ {name} ({size:.1f} MB)")
                else:
                    st.warning(f"⏳ {name} (не скачан)")
    
    # Основной контент
    st.header(f"📈 Анализ домена: {domain}")
    
    # Кнопка загрузки данных
    if st.button("🚀 Загрузить данные", type="primary"):
        # Загружаем данные
        data = load_domain_data(domain, sample_size)
        
        if data:
            # Сохраняем в session state
            st.session_state['data'] = data
            st.session_state['domain'] = domain
            st.success("✅ Данные успешно загружены!")
    
    # Если данные уже загружены
    if 'data' in st.session_state and st.session_state['domain'] == domain:
        data = st.session_state['data']
        
        # Создаем вкладки
        tabs = st.tabs(["📊 Обзор", "📈 Визуализация", "🔍 Данные"])
        
        # Вкладка 1: Обзор
        with tabs[0]:
            st.subheader("Общая статистика")
            
            # Метрики
            col1, col2, col3 = st.columns(3)
            
            total_rows = sum(len(df) for df in data.values())
            
            with col1:
                st.metric("Всего записей", f"{total_rows:,}")
            
            with col2:
                st.metric("Типов данных", len(data))
            
            with col3:
                st.metric("Домен", domain)
            
            # Краткая информация о каждом датасете
            for name, df in data.items():
                with st.expander(f"📊 {name}"):
                    st.write(f"**Строк:** {len(df):,}")
                    st.write(f"**Колонок:** {len(df.columns)}")
                    st.write("**Типы данных:**")
                    st.write(df.dtypes.value_counts())
        
        # Вкладка 2: Визуализация
        with tabs[1]:
            st.subheader("Визуализация данных")
            
            # Выбор датасета
            selected_dataset = st.selectbox(
                "Выберите датасет",
                list(data.keys())
            )
            
            df = data[selected_dataset]
            
            # Выбор типа визуализации
            viz_type = st.radio(
                "Тип визуализации",
                ["Распределение", "Временной ряд", "Корреляция"],
                horizontal=True
            )
            
            if viz_type == "Распределение":
                # Выбор колонки
                numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
                if numeric_cols:
                    col = st.selectbox("Выберите колонку", numeric_cols)
                    fig = px.histogram(df, x=col, title=f"Распределение {col}")
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.warning("Нет числовых колонок для визуализации")
            
            elif viz_type == "Временной ряд":
                # Поиск колонок с датами
                date_cols = [col for col in df.columns if 'date' in col.lower() or 'year' in col.lower()]
                if date_cols:
                    col = st.selectbox("Выберите колонку с датой", date_cols)
                    if col in df.columns:
                        if 'date' in col.lower():
                            df[col] = pd.to_datetime(df[col], errors='coerce')
                            timeline = df[col].dt.year.value_counts().sort_index()
                        else:
                            timeline = df[col].value_counts().sort_index()
                        
                        fig = px.line(
                            x=timeline.index, 
                            y=timeline.values,
                            title=f"Тренд по {col}"
                        )
                        st.plotly_chart(fig, use_container_width=True)
                else:
                    st.warning("Не найдены колонки с датами")
            
            elif viz_type == "Корреляция":
                numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
                if len(numeric_cols) > 1:
                    corr = df[numeric_cols].corr()
                    fig = px.imshow(
                        corr,
                        title="Корреляционная матрица",
                        color_continuous_scale='RdBu_r'
                    )
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.warning("Нужно минимум 2 числовые колонки для корреляции")
        
        # Вкладка 3: Данные
        with tabs[2]:
            st.subheader("Просмотр данных")
            
            # Выбор датасета
            selected_dataset = st.selectbox(
                "Выберите датасет для просмотра",
                list(data.keys()),
                key="data_viewer"
            )
            
            df = data[selected_dataset]
            
            # Показываем данные
            st.dataframe(df.head(100))
            
            # Статистика
            with st.expander("Статистика"):
                st.write(df.describe())
    
    else:
        # Если данные не загружены
        st.info("👆 Нажмите кнопку 'Загрузить данные' для начала работы")
        
        # Превью доступных данных
        with st.expander("📋 Доступные данные"):
            st.markdown("""
            **Генная инженерия:**
            - Полные данные патентов
            - NLP-обработанные тексты
            - Сигнальные данные и метрики
            
            **Полупроводники:**
            - Полные данные патентов
            - NLP-обработанные тексты
            - Сигнальные данные и метрики
            """)

if __name__ == "__main__":
    main()
