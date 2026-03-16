import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import traceback
from pathlib import Path
import io
import base64

# Импортируем функции из data_loader
from data_loader import load_domain_data, get_data_source_info, DATA_SOURCES

# ДОЛЖНА быть первой командой Streamlit
st.set_page_config(
    page_title="Patent Analysis Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Функция для создания ссылки на скачивание CSV
def get_csv_download_link(df, filename):
    csv = df.to_csv(index=False, encoding='utf-8-sig')
    b64 = base64.b64encode(csv.encode()).decode()
    href = f'<a href="data:file/csv;base64,{b64}" download="{filename}" style="text-decoration: none; padding: 5px 10px; background-color: #4CAF50; color: white; border-radius: 5px;">📥 Скачать CSV</a>'
    return href

# Функция для создания ссылки на скачивание Excel
def get_excel_download_link(df, filename):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Data')
    excel_data = output.getvalue()
    b64 = base64.b64encode(excel_data).decode()
    href = f'<a href="data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64,{b64}" download="{filename}" style="text-decoration: none; padding: 5px 10px; background-color: #2196F3; color: white; border-radius: 5px;">📥 Скачать Excel</a>'
    return href

# Заголовок
st.title("📊 Patent Analysis Dashboard")
st.markdown("---")

# Инициализация session state
if 'data_loaded' not in st.session_state:
    st.session_state.data_loaded = False
if 'current_domain' not in st.session_state:
    st.session_state.current_domain = None

# Проверка наличия данных
DATA_DIR = Path(__file__).parent / "data" / "processed"
gene_file = DATA_DIR / "gene_engineering_clean.parquet"
semi_file = DATA_DIR / "semiconductors_clean.parquet"

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
    
    # Статус данных
    st.subheader("📁 Статус данных")
    
    if gene_file.exists():
        st.success("✅ Генная инженерия: данные есть")
        # Показываем размер файла
        size_mb = gene_file.stat().st_size / (1024 * 1024)
        st.caption(f"Размер: {size_mb:.1f} MB")
    else:
        st.warning("⚠️ Генная инженерия: данных нет")
    
    if semi_file.exists():
        st.success("✅ Полупроводники: данные есть")
        # Показываем размер файла
        size_mb = semi_file.stat().st_size / (1024 * 1024)
        st.caption(f"Размер: {size_mb:.1f} MB")
    else:
        st.warning("⚠️ Полупроводники: данных нет")
    
    st.markdown("---")
    
    # Кнопка загрузки данных
    if st.button("🚀 Загрузить данные", type="primary", use_container_width=True):
        with st.spinner("🔄 Загрузка данных... Это может занять несколько секунд..."):
            try:
                # Загружаем данные
                months, papers, patents, metrics = load_domain_data(domain)
                
                # Сохраняем в session state
                st.session_state.months = months
                st.session_state.papers = papers
                st.session_state.patents = patents
                st.session_state.metrics = metrics
                st.session_state.data_loaded = True
                st.session_state.current_domain = domain
                
                st.success("✅ Данные успешно загружены!")
                st.rerun()
            except Exception as e:
                st.error(f"❌ Ошибка при загрузке данных: {e}")
                st.exception(e)
    
    # Кнопка очистки кэша
    if st.button("🔄 Очистить кэш"):
        st.cache_data.clear()
        st.session_state.data_loaded = False
        st.success("✅ Кэш очищен!")
        st.rerun()
    
    st.markdown("---")
    
    # Информация об источниках данных
    with st.expander("📊 Об источниках данных"):
        source_info = get_data_source_info(domain)
        st.markdown(f"""
        **Текущий источник:** {source_info['status']}
        **Откуда:** {source_info['source']}
        **Дата обновления:** {source_info['date']}
        **Описание:** {source_info['description']}
        """)
        
        st.markdown("---")
        st.markdown("**BigQuery интеграция:**")
        st.markdown(f"{DATA_SOURCES['bigquery']['status']} - {DATA_SOURCES['bigquery']['description']}")
        
        if DATA_SOURCES['bigquery']['status'] == "⏳ В процессе подключения":
            st.info("🔜 Ожидается доступ от коллег к BigQuery")
    
    # Информация о данных
    with st.expander("ℹ️ О датасетах"):
        st.markdown("""
        **Доступные домены:**
        - 🧬 **Генная инженерия**
        - 💻 **Полупроводники**
        
        **Типы данных:**
        - Публикации (научные статьи)
        - Патенты
        - Компании и университеты
        - Технологические темы
        
        **Метрики:**
        - Trend Score (оценка тренда)
        - Time Lag (временной лаг)
        - AI-интеграция
        - География
        """)

# Основной контент
if st.session_state.data_loaded and st.session_state.current_domain == domain:
    # Получаем данные из session state
    months = st.session_state.months
    papers = st.session_state.papers
    patents = st.session_state.patents
    metrics = st.session_state.metrics
    
    # Заголовок с доменом
    st.header(f"📈 Анализ домена: {domain}")
    source_info = get_data_source_info(domain)
    st.caption(f"📊 Данные предоставлены: {source_info['source']} | Обновлено: {source_info['date']}")
    
    # Метрики в карточках
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "📄 Публикации",
            f"{metrics['papers_total']:,}",
            delta=f"{metrics['papers_growth']}% за 2 года"
        )
    
    with col2:
        st.metric(
            "📃 Патенты",
            f"{metrics['patents_total']:,}",
            delta=f"{metrics['patents_growth']}% за 2 года"
        )
    
    with col3:
        st.metric(
            "📊 Trend Score",
            f"{metrics['trend_score']}/100",
            delta=metrics['trend_status']
        )
    
    with col4:
        st.metric(
            "⏱️ Time Lag",
            f"{metrics['time_lag']} лет",
            delta=metrics['time_lag_change']
        )
    
    st.markdown("---")
    
    # Вкладки
    tab1, tab2, tab3, tab4 = st.tabs(["📈 Тренды", "🏢 Заявители", "🌍 География", "🤖 AI-анализ"])
    
    with tab1:
        st.subheader("Динамика публикаций и патентов")
        
        # График трендов
        fig = go.Figure()
        
        fig.add_trace(go.Scatter(
            x=months,
            y=papers,
            mode='lines+markers',
            name='Публикации',
            line=dict(color='#1f77b4', width=3),
            marker=dict(size=4)
        ))
        
        fig.add_trace(go.Scatter(
            x=months,
            y=patents,
            mode='lines+markers',
            name='Патенты',
            line=dict(color='#ff7f0e', width=3),
            marker=dict(size=4)
        ))
        
        fig.update_layout(
            title="Сравнение динамики публикаций и патентов",
            xaxis_title="Месяц",
            yaxis_title="Количество",
            hovermode='x unified',
            height=500
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Данные для скачивания
        trend_df = pd.DataFrame({
            'Месяц': months,
            'Публикации': papers,
            'Патенты': patents
        })
        
        col1, col2 = st.columns(2)
        with col1:
            st.markdown(get_csv_download_link(trend_df, f"{domain}_trends.csv"), unsafe_allow_html=True)
        with col2:
            st.markdown(get_excel_download_link(trend_df, f"{domain}_trends.xlsx"), unsafe_allow_html=True)
        
        # Статистика
        col1, col2 = st.columns(2)
        
        with col1:
            st.info(f"**Всего публикаций:** {metrics['papers_total']:,}")
            st.info(f"**Рост публикаций:** {metrics['papers_growth']}% за последние 2 года")
            st.info(f"**Средняя цитируемость:** {metrics['papers_cited_avg']}")
        
        with col2:
            st.info(f"**Всего патентов:** {metrics['patents_total']:,}")
            st.info(f"**Рост патентов:** {metrics['patents_growth']}% за последние 2 года")
    
    with tab2:
        st.subheader("Топ заявителей")
        
        if metrics['top_assignees'] and metrics['assignee_values'] and metrics['top_assignees'][0] != "Нет данных":
            # Горизонтальная бар-чарт
            fig = px.bar(
                x=metrics['assignee_values'],
                y=metrics['top_assignees'],
                orientation='h',
                title="Топ-5 заявителей по количеству патентов",
                labels={'x': 'Количество патентов', 'y': ''},
                color=metrics['assignee_values'],
                color_continuous_scale='viridis'
            )
            
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
            
            # Данные для скачивания
            assignee_df = pd.DataFrame({
                'Заявитель': metrics['top_assignees'],
                'Количество патентов': metrics['assignee_values']
            })
            
            col1, col2 = st.columns(2)
            with col1:
                st.markdown(get_csv_download_link(assignee_df, f"{domain}_assignees.csv"), unsafe_allow_html=True)
            with col2:
                st.markdown(get_excel_download_link(assignee_df, f"{domain}_assignees.xlsx"), unsafe_allow_html=True)
        else:
            st.info("Нет данных о заявителях")
    
    with tab3:
        st.subheader("Географическое распределение")
        
        if metrics['countries'] and metrics['country_values'] and metrics['countries'][0] != "Нет данных":
            # Круговая диаграмма
            fig = px.pie(
                values=metrics['country_values'],
                names=metrics['countries'],
                title="Распределение патентов по странам",
                hole=0.3
            )
            
            fig.update_traces(textposition='inside', textinfo='percent+label')
            fig.update_layout(height=400)
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Таблица с данными
            geo_df = pd.DataFrame({
                'Страна': metrics['countries'],
                'Доля (%)': metrics['country_values']
            })
            st.dataframe(geo_df, use_container_width=True)
            
            # Данные для скачивания
            col1, col2 = st.columns(2)
            with col1:
                st.markdown(get_csv_download_link(geo_df, f"{domain}_geography.csv"), unsafe_allow_html=True)
            with col2:
                st.markdown(get_excel_download_link(geo_df, f"{domain}_geography.xlsx"), unsafe_allow_html=True)
        else:
            st.info("Нет данных о географическом распределении")
    
    with tab4:
        st.subheader("AI-интеграция")
        
        # Метрика AI доли
        col1, col2 = st.columns(2)
        
        with col1:
            # Круговая диаграмма для AI
            ai_data = pd.DataFrame({
                'Категория': ['AI-патенты', 'Другие'],
                'Доля': [metrics['ai_share'], 100 - metrics['ai_share']]
            })
            
            fig = px.pie(
                ai_data,
                values='Доля',
                names='Категория',
                title=f"Доля AI-патентов: {metrics['ai_share']}%",
                color_discrete_sequence=['#2ecc71', '#e74c3c']
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Данные для скачивания
            ai_df = pd.DataFrame({
                'Категория': ['AI-патенты', 'Другие'],
                'Доля (%)': [metrics['ai_share'], 100 - metrics['ai_share']]
            })
            
            col_a, col_b = st.columns(2)
            with col_a:
                st.markdown(get_csv_download_link(ai_df, f"{domain}_ai.csv"), unsafe_allow_html=True)
            with col_b:
                st.markdown(get_excel_download_link(ai_df, f"{domain}_ai.xlsx"), unsafe_allow_html=True)
        
        with col2:
            st.metric(
                "🤖 Доля AI-патентов",
                f"{metrics['ai_share']}%",
                delta=None
            )
            
            if domain == "Полупроводники":
                st.info("""
                **Технологии, связанные с AI:**
                - GAA транзисторы
                - Квантовые точки
                - 2D материалы
                - Нейроморфные вычисления
                """)
            else:
                st.info("""
                **Технологии, связанные с AI:**
                - CRISPR-Cas9
                - CRISPR-Cas12a
                - Базовое редактирование
                - Прайм-редактирование
                """)
    
    # Детальная статистика (исправлено для Arrow)
    with st.expander("📊 Детальная статистика"):
        # Создаем DataFrame с правильными типами данных
        stats_data = {
            'Метрика': [
                'Всего публикаций',
                'Всего патентов',
                'Средняя цитируемость',
                'Рост публикаций (2 года)',
                'Рост патентов (2 года)',
                'Time Lag',
                'Trend Score',
                'AI доля'
            ],
            'Значение': [
                str(metrics['papers_total']),
                str(metrics['patents_total']),
                str(metrics['papers_cited_avg']),
                f"{metrics['papers_growth']}%",
                f"{metrics['patents_growth']}%",
                f"{metrics['time_lag']} лет",
                f"{metrics['trend_score']}",
                f"{metrics['ai_share']}%"
            ],
            'Статус': [
                '-',
                '-',
                '-',
                'за 2 года',
                'за 2 года',
                metrics['time_lag_change'],
                metrics['trend_status'],
                '-'
            ]
        }
        
        stats_df = pd.DataFrame(stats_data)
        st.dataframe(stats_df, use_container_width=True, hide_index=True)
        
        # Скачать статистику
        col1, col2 = st.columns(2)
        with col1:
            st.markdown(get_csv_download_link(stats_df, f"{domain}_statistics.csv"), unsafe_allow_html=True)
        with col2:
            st.markdown(get_excel_download_link(stats_df, f"{domain}_statistics.xlsx"), unsafe_allow_html=True)

else:
    # Приветственный экран
    st.info("👆 Выберите домен в боковой панели и нажмите 'Загрузить данные' для начала работы")
    
    # Информация о статусе проекта
    st.markdown("---")
    st.subheader("📋 Статус подключения источников данных")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("### 🧬 Генная инженерия")
        if gene_file.exists():
            st.success("✅ Данные загружены")
            size_mb = gene_file.stat().st_size / (1024 * 1024)
            st.caption(f"Размер: {size_mb:.1f} MB")
        else:
            st.warning("⏳ Данные не найдены")
        st.caption("Источник: лаборатория генной инженерии")
    
    with col2:
        st.markdown("### 💻 Полупроводники")
        if semi_file.exists():
            st.success("✅ Данные загружены")
            size_mb = semi_file.stat().st_size / (1024 * 1024)
            st.caption(f"Размер: {size_mb:.1f} MB")
        else:
            st.warning("⏳ Данные не найдены")
        st.caption("Источник: лаборатория полупроводников")
    
    with col3:
        st.markdown("### ☁️ BigQuery")
        st.warning("⏳ В процессе подключения")
        st.caption("Ожидается доступ")
    
    st.markdown("---")
    
    # Превью данных
    if gene_file.exists() or semi_file.exists():
        st.subheader("📊 Доступные данные")
        
        if gene_file.exists():
            with st.expander("🧬 Генная инженерия -预览"):
                try:
                    df_preview = pd.read_parquet(gene_file).head(5)
                    st.dataframe(df_preview)
                except:
                    st.info("Не удалось загрузить превью")
        
        if semi_file.exists():
            with st.expander("💻 Полупроводники -预览"):
                try:
                    df_preview = pd.read_parquet(semi_file).head(5)
                    st.dataframe(df_preview)
                except:
                    st.info("Не удалось загрузить превью")
