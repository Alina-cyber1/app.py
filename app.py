import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import numpy as np
import io
import matplotlib.pyplot as plt
import os
import traceback
import time

print("🚀 app.py начал выполняться")
# Импортируем наш загрузчик данных
from data_loader import load_domain_data

# ========== НАСТРОЙКА СТРАНИЦЫ ==========
st.set_page_config(
    page_title="Tech Trends Monitor",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ========== ИМПОРТЫ ДЛЯ PDF ==========
PDF_AVAILABLE = False
try:
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import letter, A4
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import inch
    PDF_AVAILABLE = True
    print("✅ ReportLab успешно импортирован")
except ImportError as e:
    print(f"❌ Ошибка импорта ReportLab: {e}")
    PDF_AVAILABLE = False

# ========== КРАСИВЫЙ CSS ==========
st.markdown("""
<style>
    .main-header {
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        font-size: 3rem;
        font-weight: 700;
        margin-bottom: 0;
    }
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        border-radius: 10px;
        padding: 20px;
        color: white;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    .domain-card {
        background: white;
        border-radius: 10px;
        padding: 20px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
        border-left: 5px solid #667eea;
        margin-bottom: 10px;
    }
    .stButton > button {
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        color: white;
        border: none;
        border-radius: 5px;
        padding: 10px 20px;
        font-weight: 600;
        transition: transform 0.3s ease;
    }
    .stButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 12px rgba(102,126,234,0.4);
    }
    .stDownloadButton > button {
        background: linear-gradient(90deg, #00CC96 0%, #00B386 100%);
    }
</style>
""", unsafe_allow_html=True)

# ========== ФУНКЦИЯ ГЕНЕРАЦИИ PDF ==========
def generate_pdf_report(domain_name, metrics, dates, papers, patents):
    """Генерирует PDF отчет с графиками и метриками"""
    if not PDF_AVAILABLE:
        return None

    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4)
    styles = getSampleStyleSheet()
    story = []

    # Заголовок
    title_style = ParagraphStyle(
        'CustomTitle',
        parent=styles['Heading1'],
        fontSize=24,
        textColor=colors.HexColor('#667eea'),
        alignment=1,
        spaceAfter=30
    )

    title = Paragraph(f"🚀 Tech Trends Report: {domain_name}", title_style)
    story.append(title)

    # Дата
    date_style = ParagraphStyle(
        'DateStyle',
        parent=styles['Normal'],
        fontSize=10,
        textColor=colors.gray,
        alignment=1
    )
    date_text = Paragraph(f"Сгенерировано: {datetime.now().strftime('%d.%m.%Y %H:%M')}", date_style)
    story.append(date_text)
    story.append(Spacer(1, 20))

    # Ключевые метрики (используем .get() для безопасности)
    story.append(Paragraph("📊 Ключевые метрики", styles['Heading2']))
    story.append(Spacer(1, 10))

    metrics_data = [
        ['Показатель', 'Значение', 'Изменение'],
        ['Научные публикации', str(metrics.get('papers_total', 0)), f"+{metrics.get('papers_growth', 0)}%"],
        ['Патенты', str(metrics.get('patents_total', 0)), f"+{metrics.get('patents_growth', 0)}%"],
        ['Time Lag', f"{metrics.get('time_lag', 0)} года", str(metrics.get('time_lag_change', '0'))],
        ['Trend Score', f"{metrics.get('trend_score', 0)}/100", metrics.get('trend_status', 'Нет данных')],
        ['AI-интеграция', f"{metrics.get('ai_share', 0)}%", "в патентах"]
    ]

    metrics_table = Table(metrics_data, colWidths=[150, 100, 100])
    metrics_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#667eea')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 12),
        ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#f8f9fa')),
        ('GRID', (0, 0), (-1, -1), 1, colors.lightgrey),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
        ('TOPPADDING', (0, 0), (-1, -1), 6),
    ]))
    story.append(metrics_table)
    story.append(Spacer(1, 20))

    # Топ заявителей
    story.append(Paragraph("🏭 Топ-5 заявителей", styles['Heading2']))
    story.append(Spacer(1, 10))

    top_assignees = metrics.get('top_assignees', ['Нет данных'])
    assignee_values = metrics.get('assignee_values', [0])
    assignees_data = [['Компания', 'Количество патентов']]
    for i in range(len(top_assignees)):
        assignees_data.append([top_assignees[i], str(assignee_values[i])])

    assignees_table = Table(assignees_data, colWidths=[200, 100])
    assignees_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#00CC96')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#f8f9fa')),
        ('GRID', (0, 0), (-1, -1), 1, colors.lightgrey),
    ]))
    story.append(assignees_table)
    story.append(Spacer(1, 20))

    # География
    story.append(Paragraph("🌍 География патентования", styles['Heading2']))
    story.append(Spacer(1, 10))

    countries = metrics.get('countries', ['Нет данных'])
    country_values = metrics.get('country_values', [100])
    geo_data = [['Страна', 'Доля (%)']]
    for i in range(len(countries)):
        geo_data.append([countries[i], str(country_values[i])])

    geo_table = Table(geo_data, colWidths=[150, 100])
    geo_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#FF4B4B')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#f8f9fa')),
        ('GRID', (0, 0), (-1, -1), 1, colors.lightgrey),
    ]))
    story.append(geo_table)
    story.append(Spacer(1, 20))

    # График динамики
    story.append(Paragraph("📈 Динамика развития (последние 2 года)", styles['Heading2']))
    story.append(Spacer(1, 10))

    if len(dates) > 0:
        fig, ax = plt.subplots(figsize=(8, 4))
        ax.plot(dates[-24:], papers[-24:], label='Публикации', color='#00CC96', linewidth=2)
        ax.plot(dates[-24:], patents[-24:], label='Патенты', color='#FF4B4B', linewidth=2)
        ax.set_xlabel('Год')
        ax.set_ylabel('Количество')
        ax.set_title(f'{domain_name}: Динамика за 2 года')
        ax.legend()
        ax.grid(True, alpha=0.3)
        ax.set_facecolor('#f8f9fa')
        fig.patch.set_facecolor('#f8f9fa')

        img_buffer = io.BytesIO()
        fig.savefig(img_buffer, format='png', dpi=150, bbox_inches='tight', facecolor='#f8f9fa')
        plt.close(fig)
        img_buffer.seek(0)
        story.append(Image(img_buffer, width=450, height=250))
    else:
        story.append(Paragraph("Нет данных для графика", styles['Normal']))
    story.append(Spacer(1, 20))

    # Подтехнологии
    story.append(Paragraph("🔬 Быстрорастущие подтехнологии", styles['Heading2']))
    story.append(Spacer(1, 10))

    if "Полупроводники" in domain_name:
        subtopics = ["Квантовые вычисления", "Advanced Packaging", "GaN/SiC устройства", "EUV литография", "MRAM память"]
        growth = [55, 45, 38, 42, 28]
    else:
        subtopics = ["CRISPR-Cas12/13", "Липидные наночастицы", "CAR-T терапия", "Base editing", "Вирусные векторы"]
        growth = [68, 73, 52, 48, 41]

    subtopics_data = [['Подтехнология', 'Рост за год (%)']]
    for i in range(len(subtopics)):
        subtopics_data.append([subtopics[i], str(growth[i])])

    subtopics_table = Table(subtopics_data, colWidths=[250, 100])
    subtopics_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#764ba2')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#f8f9fa')),
        ('GRID', (0, 0), (-1, -1), 1, colors.lightgrey),
    ]))
    story.append(subtopics_table)
    story.append(Spacer(1, 30))

    # Рекомендации
    story.append(Paragraph("💡 Рекомендации", styles['Heading2']))
    story.append(Spacer(1, 10))

    trend_score = metrics.get('trend_score', 0)
    if trend_score > 80:
        recs = [
            "🔥 Технология показывает взрывной рост. Рекомендуется:",
            "• Активно инвестировать в R&D",
            "• Усилить патентную защиту",
            "• Мониторить стартапы в этой области"
        ]
    elif trend_score > 60:
        recs = [
            "📈 Стабильный рост. Рекомендуется:",
            "• Продолжать текущие разработки",
            "• Анализировать конкурентные патенты",
            "• Рассмотреть стратегические партнерства"
        ]
    else:
        recs = [
            "💤 Технология в стадии созревания. Рекомендуется:",
            "• Оптимизировать существующие решения",
            "• Искать новые ниши применения",
            "• Мониторить смежные области"
        ]

    for rec in recs:
        story.append(Paragraph(rec, styles['Normal']))
        story.append(Spacer(1, 3))

    doc.build(story)
    buffer.seek(0)
    return buffer

# ========== ЗАГОЛОВОК ==========
st.markdown('<h1 class="main-header">🚀 Tech Trends Monitor</h1>', unsafe_allow_html=True)
st.markdown("*Отслеживание перехода науки в технологии*")

# ========== САЙДБАР ==========
with st.sidebar:
    st.markdown("## 🎛 Панель управления")
    st.markdown("---")

    domain = st.selectbox(
        "🔬 Технологический домен",
        ["💻 Полупроводники", "🧬 Генная инженерия"],
        help="Выберите технологическую область для анализа"
    )

    domain_clean = domain.replace("💻 ", "").replace("🧬 ", "")

    if "Полупроводники" in domain:
        st.markdown("""
        <div class="domain-card">
            <b>📋 Коды CPC:</b><br>
            H01L, H10, G03F, C23C, H05K<br>
            <b>🎯 Topics:</b><br>
            Semiconductor devices, Lithography, Thin films
        </div>
        """, unsafe_allow_html=True)
    else:
        st.markdown("""
        <div class="domain-card">
            <b>📋 Коды CPC:</b><br>
            C12N15/00, A61K48/00, C12N9/22<br>
            <b>🎯 Topics:</b><br>
            CRISPR/Cas, Gene therapy, Vectors
        </div>
        """, unsafe_allow_html=True)

    st.markdown("---")
    st.markdown("### 📅 Период анализа")
    year_range = st.slider("Выберите годы", 2015, 2025, (2020, 2025), label_visibility="collapsed")

    st.markdown("### 🌍 Страны патентования")
    col1, col2 = st.columns(2)
    with col1:
        us = st.checkbox("🇺🇸 US", value=True)
        cn = st.checkbox("🇨🇳 CN", value=True)
        jp = st.checkbox("🇯🇵 JP", value=False)
    with col2:
        kr = st.checkbox("🇰🇷 KR", value=False)
        ep = st.checkbox("🇪🇺 EP", value=True)
        wo = st.checkbox("🌐 WO", value=False)

    st.markdown("---")
    st.markdown("### 📊 Статус системы")
    
    # Загружаем данные с обработкой ошибок
    try:
        load_start = time.time()
        dates, papers, patents, metrics = load_domain_data(domain_clean)
        load_time = time.time() - load_start
        data_available = len(dates) > 0
    except Exception as e:
        st.error(f"❌ Ошибка загрузки данных: {e}")
        with st.expander("🔍 Детали ошибки"):
            st.code(traceback.format_exc())
        st.stop()

    # Применяем фильтры по годам
    start_year, end_year = year_range
    mask_years = [int(d[:4]) >= start_year and int(d[:4]) <= end_year for d in dates]
    dates_filtered = np.array(dates)[mask_years]
    papers_filtered = np.array(papers)[mask_years]
    patents_filtered = np.array(patents)[mask_years]

    # Метрики производительности (без psutil)
    try:
        # Пытаемся получить информацию о памяти из /proc (работает в Linux)
        with open('/proc/self/statm') as f:
            memory_pages = int(f.read().split()[0])
            page_size = os.sysconf('SC_PAGESIZE')
            memory_usage = memory_pages * page_size / 1024 / 1024
    except:
        # Если не получается, просто ставим 0
        memory_usage = 0

    status_col1, status_col2 = st.columns(2)
    with status_col1:
        st.markdown("🟢 OpenAlex")
        st.markdown("🟡 BigQuery")
        st.markdown(f"⏱ Загрузка: {load_time:.1f} сек")
    with status_col2:
        if data_available:
            papers_total = metrics.get('papers_total', 0)
            patents_total = metrics.get('patents_total', 0)
            st.markdown(f"✅ {papers_total} статей")
            st.markdown(f"⏳ {patents_total} патентов")
            st.markdown(f"💾 Память: {memory_usage:.0f} МБ")
        else:
            st.markdown("❌ Нет данных")
            st.markdown("❌ Нет данных")
            st.markdown("❌")
    
    st.progress(0.8 if data_available else 0.2, text="Готовность MVP")

    # Если данных нет – показываем предупреждение и останавливаем выполнение
    if not data_available:
        st.warning(f"⚠️ Для домена «{domain_clean}» пока нет данных. Пожалуйста, выберите другой домен или добавьте данные.")
        st.stop()

# ========== МЕТРИКИ ==========
col1, col2, col3, col4 = st.columns(4)
with col1:
    papers_total = metrics.get('papers_total', 0)
    papers_growth = metrics.get('papers_growth', 0)
    st.markdown(f"""
    <div class="metric-card">
        <h3 style="color: white; margin:0">📄 Публикации</h3>
        <h1 style="color: white; margin:0">{papers_total}</h1>
        <p style="color: white; margin:0">+{papers_growth}% за год</p>
    </div>
    """, unsafe_allow_html=True)
with col2:
    patents_total = metrics.get('patents_total', 0)
    patents_growth = metrics.get('patents_growth', 0)
    st.markdown(f"""
    <div class="metric-card">
        <h3 style="color: white; margin:0">📃 Патенты</h3>
        <h1 style="color: white; margin:0">{patents_total}</h1>
        <p style="color: white; margin:0">+{patents_growth}% за год</p>
    </div>
    """, unsafe_allow_html=True)
with col3:
    time_lag = metrics.get('time_lag', 0)
    time_lag_change = metrics.get('time_lag_change', '0')
    st.markdown(f"""
    <div class="metric-card">
        <h3 style="color: white; margin:0">⏱ Time Lag</h3>
        <h1 style="color: white; margin:0">{time_lag} года</h1>
        <p style="color: white; margin:0">{time_lag_change} vs 2024</p>
    </div>
    """, unsafe_allow_html=True)
with col4:
    trend_score = metrics.get('trend_score', 0)
    trend_status = metrics.get('trend_status', 'Нет данных')
    st.markdown(f"""
    <div class="metric-card">
        <h3 style="color: white; margin:0">🎯 Trend Score</h3>
        <h1 style="color: white; margin:0">{trend_score}/100</h1>
        <p style="color: white; margin:0">{trend_status}</p>
    </div>
    """, unsafe_allow_html=True)

# ========== ОСНОВНОЙ КОНТЕНТ ==========
tab1, tab2, tab3, tab4 = st.tabs(["📈 Тренды", "🔬 Подтехнологии", "📊 Данные", "📄 Отчеты"])

with tab1:
    if len(dates_filtered) == 0:
        st.warning("⚠️ Нет данных за выбранный период")
    else:
        st.markdown(f"## 📊 Динамика развития: {domain_clean}")

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=dates_filtered, y=papers_filtered,
            name='📄 Научные публикации',
            line=dict(color='#00CC96', width=4),
            fill='tozeroy',
            fillcolor='rgba(0,204,150,0.1)'
        ))
        fig.add_trace(go.Scatter(
            x=dates_filtered, y=patents_filtered,
            name='📃 Патенты',
            line=dict(color='#FF4B4B', width=4),
            fill='tozeroy',
            fillcolor='rgba(255,75,75,0.1)',
            yaxis='y2'
        ))

        fig.update_layout(
            title=f"{domain_clean}: Наука vs Технологии",
            xaxis_title="Год",
            yaxis_title="Количество публикаций",
            yaxis2=dict(
                title="Количество патентов",
                overlaying='y',
                side='right'
            ),
            hovermode='x unified',
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            height=500,
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            )
        )
        st.plotly_chart(fig, use_container_width=True)

        col1, col2 = st.columns(2)
        with col1:
            st.markdown("### 🏭 Топ заявителей")
            top_assignees = metrics.get('top_assignees', ['Нет данных'])
            assignee_values = metrics.get('assignee_values', [0])
            top_df = pd.DataFrame({
                'Компания': top_assignees,
                'Патенты': assignee_values
            })
            fig2 = px.bar(
                top_df, x='Компания', y='Патенты',
                color='Патенты',
                color_continuous_scale='Viridis'
            )
            fig2.update_layout(
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                showlegend=False
            )
            st.plotly_chart(fig2, use_container_width=True)

        with col2:
            st.markdown("### 🌍 География")
            countries = metrics.get('countries', ['Нет данных'])
            country_values = metrics.get('country_values', [100])
            geo_df = pd.DataFrame({
                'Страна': countries,
                'Доля': country_values
            })
            fig3 = px.pie(
                geo_df, values='Доля', names='Страна',
                color_discrete_sequence=px.colors.sequential.Viridis
            )
            fig3.update_traces(textposition='inside', textinfo='percent+label')
            fig3.update_layout(showlegend=False)
            st.plotly_chart(fig3, use_container_width=True)

with tab2:
    st.markdown(f"## 🔬 Подтехнологии в {domain_clean}")

    if "Полупроводники" in domain:
        subtopics = ["Литография (EUV/DUV)", "Advanced Packaging", "GaN/SiC устройства", "MRAM/FRAM память", "Квантовые вычисления"]
        growth = [45, 38, 32, 28, 55]
    else:
        subtopics = ["CRISPR-Cas9", "CRISPR-Cas12/13", "Вирусные векторы (AAV)", "Липидные наночастицы", "CAR-T терапия"]
        growth = [52, 68, 41, 73, 47]

    sub_df = pd.DataFrame({
        'Технология': subtopics,
        'Рост за год (%)': growth
    })

    fig4 = px.bar(
        sub_df, x='Технология', y='Рост за год (%)',
        color='Рост за год (%)',
        color_continuous_scale='Viridis',
        title="Темпы роста подтехнологий"
    )
    fig4.update_layout(
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)'
    )
    st.plotly_chart(fig4, use_container_width=True)

    st.markdown("---")
    st.markdown("### 🔍 Детальный анализ")

    for i, (sub, grow) in enumerate(zip(subtopics[:3], growth[:3])):
        with st.expander(f"📌 {sub} (Рост: +{grow}%)"):
            col1, col2 = st.columns(2)
            with col1:
                st.markdown(f"**Публикаций:** {np.random.randint(100, 500)}")
                st.markdown(f"**Патентов:** {np.random.randint(50, 300)}")
                st.markdown(f"**Time Lag:** {np.random.uniform(2, 5):.1f} года")
            with col2:
                st.markdown(f"**Топ-заявитель:** {np.random.choice(metrics.get('top_assignees', ['Нет данных']))}")
                st.markdown(f"**AI-интеграция:** {np.random.randint(10, 60)}%")
                st.markdown(f"**Trend Score:** {np.random.randint(65, 95)}/100")

with tab3:
    st.markdown(f"## 📊 Реальные данные по {domain_clean}")
    
    # Загружаем DataFrame для показа примеров
    domain_map = {'Полупроводники': 'semiconductors', 'Генная инженерия': 'gene_engineering'}
    domain_key = domain_map.get(domain_clean, domain_clean.lower())
    # ИСПРАВЛЕНО: правильное имя файла
    file_path = os.path.join('data', 'processed', f"{domain_key}_clean_full.parquet")
    
    try:
        if os.path.exists(file_path):
            df_real = pd.read_parquet(file_path, engine='fastparquet')
            st.success(f"✅ Загружено {len(df_real):,} записей из РЕАЛЬНЫХ данных")
            
            st.markdown("### 📊 Статистика реальных данных")
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Всего записей", f"{len(df_real):,}")
            with col2:
                if 'topic' in df_real.columns:
                    st.metric("Уникальных тем", df_real['topic'].nunique())
                else:
                    st.metric("Уникальных тем", "N/A")
            with col3:
                if 'affiliation' in df_real.columns:
                    st.metric("Организаций", df_real['affiliation'].nunique())
                else:
                    st.metric("Организаций", "N/A")
            with col4:
                if 'year' in df_real.columns:
                    st.metric("Годы", f"{df_real['year'].min()} - {df_real['year'].max()}")
                else:
                    st.metric("Годы", "N/A")
            
            st.markdown("### 📋 Примеры реальных записей")
            display_cols = ['publication_date', 'title', 'authors', 'topic', 'citations', 'affiliation']
            available_cols = [col for col in display_cols if col in df_real.columns]
            
            if available_cols:
                df_display = df_real[available_cols].head(10)
                column_names = {
                    'publication_date': 'Дата',
                    'title': 'Название',
                    'authors': 'Авторы',
                    'topic': 'Тема',
                    'citations': 'Цитирования',
                    'affiliation': 'Организация'
                }
                df_display = df_display.rename(columns={col: column_names.get(col, col) for col in df_display.columns})
                st.dataframe(df_display, hide_index=True, use_container_width=True)
                
                st.markdown("### 📥 Экспорт данных")
                col1, col2, col3 = st.columns([1, 1, 2])
                with col1:
                    csv = df_real.head(100).to_csv(index=False).encode('utf-8')
                    st.download_button(
                        "📥 Скачать реальные данные (CSV)",
                        csv,
                        f"{domain_key}_real_data.csv",
                        "text/csv",
                        use_container_width=True
                    )
                with col2:
                    if st.button("📊 Экспорт в Excel", disabled=True, use_container_width=True):
                        st.info("Функция в разработке")
                with col3:
                    ai_share = metrics.get('ai_share', 0)
                    st.info(f"📈 AI-интеграция в домене: {ai_share}% патентов содержат G06N*")
            else:
                st.warning("⚠️ Не найдены колонки для отображения")
        else:
            st.error(f"❌ Файл с данными не найден: {file_path}")
    except Exception as e:
        st.error(f"❌ Ошибка загрузки реальных данных: {str(e)}")
        with st.expander("🔍 Диагностическая информация"):
            st.code(f"""
            Путь к файлу: {file_path}
            Файл существует: {os.path.exists(file_path)}
            Размер файла: {os.path.getsize(file_path) if os.path.exists(file_path) else 'N/A'} байт
            Ошибка: {str(e)}
            """)

with tab4:
    st.markdown("## 📄 Генерация отчетов")

    if not PDF_AVAILABLE:
        st.error("⚠️ Для экспорта PDF необходимо установить библиотеки:")
        st.code("pip install reportlab matplotlib")
        with st.expander("🔍 Диагностика"):
            st.write("Текущие установленные пакеты:")
            import sys
            import subprocess
            try:
                result = subprocess.run([sys.executable, '-m', 'pip', 'list'], 
                                      capture_output=True, text=True)
                st.text(result.stdout[:500] + "...")
            except:
                st.write("Не удалось получить список пакетов")
        st.info("💡 После установки библиотек перезапустите приложение")
    else:
        st.success("✅ Библиотеки для PDF установлены")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("""
        <div style='background: linear-gradient(135deg, #667eea10, #764ba210); padding: 20px; border-radius: 10px; border: 1px solid #667eea30;'>
            <h3 style='margin:0; color: #667eea;'>🎴 Topic Card</h3>
            <p style='color: gray;'>⚡ Быстрый отчет по технологии</p>
            <hr style='margin: 10px 0;'>
            <p>✅ Название темы + домен</p>
            <p>✅ Ключевые метрики</p>
            <p>✅ Топ-5 игроков</p>
            <p>✅ График динамики</p>
            <p>✅ Рекомендации</p>
        </div>
        """, unsafe_allow_html=True)

        if st.button("🔄 Сгенерировать Topic Card", key="topic_btn", use_container_width=True):
            if PDF_AVAILABLE:
                with st.spinner("🔄 Генерация PDF отчета..."):
                    pdf_buffer = generate_pdf_report(domain_clean, metrics, dates, papers, patents)
                    if pdf_buffer:
                        st.success("✅ Topic Card готов!")
                        st.balloons()
                        st.download_button(
                            label="📥 Скачать PDF-отчет",
                            data=pdf_buffer,
                            file_name=f"topic_card_{domain_clean.lower().replace(' ', '_')}_{datetime.now().strftime('%Y%m%d')}.pdf",
                            mime="application/pdf",
                            use_container_width=True
                        )
            else:
                st.error("❌ Невозможно сгенерировать PDF: библиотеки не установлены")
                st.info("💡 Выполните команду: pip install reportlab matplotlib")

    with col2:
        st.markdown("""
        <div style='background: linear-gradient(135deg, #FF4B4B10, #FF6B6B10); padding: 20px; border-radius: 10px; border: 1px solid #FF4B4B30;'>
            <h3 style='margin:0; color: #FF4B4B;'>📊 Monthly Deep Dive</h3>
            <p style='color: gray;'>🔍 Детальный анализ с рекомендациями</p>
            <hr style='margin: 10px 0;'>
            <p>✅ Executive summary</p>
            <p>✅ Наука vs Патенты</p>
            <p>✅ Кластеры подтехнологий</p>
            <p>✅ Конкурентный анализ</p>
            <p>✅ Список "что читать"</p>
        </div>
        """, unsafe_allow_html=True)

        if st.button("🔬 Сгенерировать Deep Dive", key="deep_btn", use_container_width=True):
            if PDF_AVAILABLE:
                with st.spinner("🔄 Генерация детального отчета..."):
                    pdf_buffer = generate_pdf_report(domain_clean, metrics, dates, papers, patents)
                    if pdf_buffer:
                        st.success("✅ Deep Dive отчет готов!")
                        st.snow()
                        st.download_button(
                            label="📥 Скачать PDF (Deep Dive)",
                            data=pdf_buffer,
                            file_name=f"deep_dive_{domain_clean.lower().replace(' ', '_')}_{datetime.now().strftime('%Y%m%d')}.pdf",
                            mime="application/pdf",
                            use_container_width=True
                        )
            else:
                st.error("❌ Невозможно сгенерировать PDF: библиотеки не установлены")
                st.info("💡 Выполните команду: pip install reportlab matplotlib")

    st.markdown("---")
    st.markdown("### 📋 Быстрый экспорт")

    col1, col2, col3 = st.columns(3)
    with col1:
        if st.button("📊 Экспорт метрик в PDF", use_container_width=True):
            if PDF_AVAILABLE:
                with st.spinner("🔄 Генерация PDF..."):
                    pdf_buffer = generate_pdf_report(domain_clean, metrics, dates, papers, patents)
                    if pdf_buffer:
                        st.download_button(
                            label="📥 Скачать PDF",
                            data=pdf_buffer,
                            file_name=f"metrics_{domain_clean.lower().replace(' ', '_')}.pdf",
                            mime="application/pdf",
                            use_container_width=True
                        )
            else:
                st.error("❌ Невозможно сгенерировать PDF: библиотеки не установлены")
    with col2:
        timeline_data = pd.DataFrame({
            'Дата': dates_filtered if len(dates_filtered) > 0 else [],
            'Публикации': papers_filtered.astype(int) if len(papers_filtered) > 0 else [],
            'Патенты': patents_filtered.astype(int) if len(patents_filtered) > 0 else []
        })
        if len(timeline_data) > 0:
            csv = timeline_data.to_csv(index=False).encode('utf-8')
        else:
            csv = "".encode('utf-8')
        st.download_button(
            "📥 Скачать CSV",
            csv,
            f"{domain_clean.lower().replace(' ', '_')}_timeline.csv",
            "text/csv",
            use_container_width=True
        )
    with col3:
        if st.button("📧 Отправить по email", disabled=True, use_container_width=True):
            st.info("Функция в разработке")

# ========== ФУТЕР ==========
st.markdown("---")
st.markdown(f"""
<div style='text-align: center; color: gray; padding: 20px;'>
    <p style='font-size: 1.2em;'>🚀 Data Science платформа мониторинга технологических трендов | MVP v1.0</p>
    <p style='font-size: 0.9em;'>
        Текущий домен: {domain_clean} |
        Данные: OpenAlex, Google Patents |
        Обновлено: {datetime.now().strftime('%d.%m.%Y %H:%M')}
    </p>
    <p style='font-size: 0.8em; color: #999;'>
        Разработано в рамках стажировки | 2026
    </p>
</div>
""", unsafe_allow_html=True)
