import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import traceback

# Импортируем функции из data_loader
from data_loader import load_domain_data, _download_files

# ДОЛЖНА быть первой командой Streamlit
st.set_page_config(
    page_title="Patent Analysis Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Заголовок
st.title("📊 Patent Analysis Dashboard")
st.markdown("---")

# Инициализация session state
if 'data_loaded' not in st.session_state:
    st.session_state.data_loaded = False
if 'current_domain' not in st.session_state:
    st.session_state.current_domain = None

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
    
    # Кнопка загрузки данных
    if st.button("🚀 Загрузить данные", type="primary", use_container_width=True):
        with st.spinner("🔄 Загрузка данных... Это может занять несколько минут..."):
            try:
                # Сначала проверяем/скачиваем файлы
                _download_files()
                
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
    
    # Информация о данных
    with st.expander("ℹ️ О данных"):
        st.markdown("""
        **Доступные домены:**
        - 🧬 **Генная инженерия**
        - 💻 **Полупроводники**
        
        **Типы данных:**
        - Публикации (статьи)
        - Патенты
        - CPC классификация
        - Заявители
        
        **Метрики:**
        - Trend Score
        - Time Lag
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
    
    # Метрики в карточках
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "📄 Публикации",
            f"{metrics['papers_total']:,}",
            delta=f"{metrics['papers_growth']}%"
        )
    
    with col2:
        st.metric(
            "📃 Патенты",
            f"{metrics['patents_total']:,}",
            delta=f"{metrics['patents_growth']}%"
        )
    
    with col3:
        st.metric(
            "📊 Trend Score",
            f"{metrics['trend_score']}",
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
        
        # Статистика роста
        col1, col2 = st.columns(2)
        
        with col1:
            st.info(f"**Рост публикаций:** {metrics['papers_growth']}% за последние 2 года")
            st.info(f"**Средняя цитируемость:** {metrics['papers_cited_avg']}")
        
        with col2:
            st.info(f"**Рост патентов:** {metrics['patents_growth']}% за последние 2 года")
            st.info(f"**Всего патентов:** {metrics['patents_total']:,}")
    
    with tab2:
        st.subheader("Топ заявителей")
        
        if metrics['top_assignees'] and metrics['assignee_values']:
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
        else:
            st.info("Нет данных о заявителях")
    
    with tab3:
        st.subheader("Географическое распределение")
        
        if metrics['countries'] and metrics['country_values']:
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
        
        with col2:
            st.metric(
                "🤖 Доля AI-патентов",
                f"{metrics['ai_share']}%",
                delta=f"{metrics['ai_share'] - 15:.1f}% vs среднему"
            )
            
            st.info("""
            **Что такое AI-патенты?**
            
            Патенты с классификацией G06N (Computer systems based on specific computational models)
            
            *Нейронные сети
            *Машинное обучение
            *Искусственный интеллект
            *Эволюционные вычисления
            """)
    
    # Дополнительная информация
    with st.expander("📊 Детальная статистика"):
        stats_df = pd.DataFrame({
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
                f"{metrics['papers_total']:,}",
                f"{metrics['patents_total']:,}",
                metrics['papers_cited_avg'],
                f"{metrics['papers_growth']}%",
                f"{metrics['patents_growth']}%",
                f"{metrics['time_lag']} лет ({metrics['time_lag_change']})",
                f"{metrics['trend_score']} ({metrics['trend_status']})",
                f"{metrics['ai_share']}%"
            ]
        })
        
        st.dataframe(stats_df, use_container_width=True)

else:
    # Приветственный экран
    st.info("👆 Выберите домен в боковой панели и нажмите 'Загрузить данные' для начала работы")
    
    # Превью
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        ### 🧬 Генная инженерия
        - Анализ публикаций и патентов
        - Тренды развития технологий
        - Ключевые игроки рынка
        - География исследований
        """)
    
    with col2:
        st.markdown("""
        ### 💻 Полупроводники
        - Динамика патентования
        - Технологические тренды
        - Лидеры инноваций
        - AI-интеграция
        """)
    
    # Пример графика
    st.markdown("---")
    st.subheader("📈 Пример визуализации")
    
    # Демо-данные
    demo_dates = pd.date_range(start='2020-01-01', end='2025-12-01', freq='MS').strftime('%Y-%m')
    demo_papers = np.random.poisson(lam=50, size=len(demo_dates)).cumsum()
    demo_patents = np.random.poisson(lam=30, size=len(demo_dates)).cumsum()
    
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=demo_dates, y=demo_papers, name='Публикации (пример)', line=dict(color='#1f77b4')))
    fig.add_trace(go.Scatter(x=demo_dates, y=demo_patents, name='Патенты (пример)', line=dict(color='#ff7f0e')))
    fig.update_layout(title="Пример графика (демонстрационные данные)", height=400)
    
    st.plotly_chart(fig, use_container_width=True)
